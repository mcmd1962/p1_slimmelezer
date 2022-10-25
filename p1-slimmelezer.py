#!/usr/bin/env python3
# vim: set autoindent filetype=python tabstop=4 shiftwidth=4 softtabstop=4 number textwidth=175 expandtab:
# vim: set fileencoding=utf-8

import argparse
import datetime
import jinja2
import json
import logging
import os.path
import socket
import re
import struct
import sys
import time
import signal
import yaml

__author__ = "Marcel Dorenbos"
__copyright__ = "Copyright 2022, Marcel Dorenbos"
__license__ = "MIT"
__email__ = "marcel@home"
__version__ = "2022-10.11"

def get_arguments():
    """
    get commandline arguments
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="P1 reader interface")

    parser.add_argument("--config-file", default=__file__.replace('.py', '.yml').replace('/bin/', '/etc/'),
             help="P1 config file, default %(default)s", metavar='FILE')

    choices=['debug', 'info', 'warning', 'error', 'critical']
    parser.add_argument("--log", help="Set log level (default info)", choices=choices, default="info")

    parser.add_argument("--debug", action='store_true', help="debug mode")
    parser.add_argument('--version', action='version', version=__version__)
    parser.add_argument("--timesync-period", help="How often, in seconds, to sync time (default '%(default)s')", type=int, default=86400)
    arguments = parser.parse_args()

    return arguments

P1_ONE_VALUE_RGX  = re.compile(r'(?P<key>\d+\-\d+:\d+\.\d+\.\d+)\((?P<value>[^(]*?)\)$')
P1_TWO_VALUES_RGX = re.compile(r'(?P<key>\d+\-\d+:\d+\.\d+\.\d+)\((?P<value1>[^(]*?)\)\((?P<value2>[^(]*?)\)$')
P1_MORE_THAN_TWO_VALUES_RGX = re.compile(r'(?P<key>\d+\-\d+:\d+\.\d+\.\d+)\((?P<value>[^(]*?)\)\((?P<value2>[^(]*?)\(.*\)$')


def do_exit(sig, stack):
    sys.exit('Exiting')

# ##############################################################################
#===  CLASS  ===================================================================
#         NAME:  SlimmeLezer
#      PURPOSE:  Interface to network port of 'Slimme Lezer'
#===============================================================================

CONFIG = None

class SlimmeLezer():

    """
      This class contains the logic to communicate with the slimme lezer, fetch data,
      store data and generate reports.
    """
    def __init__(self, host, port, timesync_period, socket_timeout):
        self.host = host
        self.port = port
        self.timesync_period = timesync_period
        self.socket_timeout = socket_timeout

        self.p1_connection = (self.host, self.port)
        self.sleep_open_p1_connection = 1
        self.open_p1_connection()

        self.data = dict()
        self.data['meta'] = {}
        self.data['telegram'] = {}
        self.first_line_read = False
        self.frame_start_time = 0
        self.datagram_timestamp_format = '%y%m%d%H%M%S%Z'
        self.first_datagram_time = self.first_datagram_timestamp = datetime.datetime.now() - datetime.timedelta(seconds=2 * self.timesync_period)
        self.delta_time = 0.0

        self.multicast_address = get_config_value(category='multicast', key='address', config_type=str)
        self.multicast_port = get_config_value(category='multicast', key='port', config_type=int)
        self.multicast_TTL = get_config_value(category='multicast', key='TTL', config_type=int)
        self.multicast_connection = (self.multicast_address, self.multicast_port)
        self.open_multicast_connection()

        self.slimmelezer_buffer = ''
        self.slimmelezer_remainder = ''
        self.slimmelezer_delimeter = '\r\n'
        self.previous_timesync_period_block = 0

    def open_p1_connection(self):
        logging.info("Waiting %s seconds to open P1 socket", self.sleep_open_p1_connection)
        time.sleep(self.sleep_open_p1_connection)
        self.sleep_open_p1_connection = 15

        #Open socket port
        try:
            self.p1_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.p1_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.p1_sock.settimeout(20)
            self.p1_sock.connect(self.p1_connection)
            self.p1_sock.settimeout(self.socket_timeout)
        except:
            logging.critical("Error when opening P1 socket (%s, %s), reason: %s. Aaaaarch.", self.host, self.port, sys.exc_info())
            self.close_p1_connection()

        logging.info("P1 socket connection (%s) opened", self.p1_connection)

    def open_multicast_connection(self):
        #Open socket port
        try:
            self.multicast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            ttl = struct.pack('b', self.multicast_TTL)
            self.multicast_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            self.multicast_sock.settimeout(1)
        except:
            sys.exit("Error when opening multicast socket (%s, %s). reason: %s. Aaaaarch." % (self.multicast_address, self.multicast_port, sys.exc_info()))

        logging.info("Multicast socket connection (%s) opened", self.multicast_connection)

    def close_p1_connection(self):
        self.p1_sock.close()

    def parse_p1_value(self, value):
        logging.debug('Original value: %s', value)
        if '*m3' in value:
            value = int(1000 * float(value.split('*')[0]))
        elif '*kW' in value:
            value = int(1000 * float(value.split('*')[0]))
        elif '*V' in value:
            value = float(value.split('*')[0])
        elif '*A' in value:
            value = int(value.split('*')[0])
        logging.debug('  converted to: %s', value)
        return value

    def read_slimmelezer_buffer(self):
        while self.slimmelezer_delimeter not in self.slimmelezer_buffer:
            try:
                # Read data from the socket
                result = self.p1_sock.recv(1024).decode('UTF-8')
                logging.debug("Stream: %s", ' | '.join(result.split(self.slimmelezer_delimeter)))
                self.slimmelezer_buffer += result
            except socket.timeout:
                logging.warning("Socket timeout occured (>%ss)", self.socket_timeout)
                self.close_p1_connection()
                self.open_p1_connection()
            except:
                logging.error("P1 connection (%s) cannot be read (%s). Aaaaaaaaarch.", self.p1_connection, sys.exc_info())
                self.close_p1_connection()
                self.open_p1_connection()
        logging.debug('BUFFER (size=%s):  %s', len(self.slimmelezer_buffer), self.slimmelezer_buffer.split(self.slimmelezer_delimeter))
    
    def read_line(self):
        if not self.slimmelezer_delimeter in self.slimmelezer_buffer:
            self.read_slimmelezer_buffer()

        element, self.slimmelezer_buffer = self.slimmelezer_buffer.split(self.slimmelezer_delimeter, 1)
        logging.debug("Element: %s   buffer: %s", element, ' | '.join(self.slimmelezer_buffer.split(self.slimmelezer_delimeter)))
        return element

    def skip_datagram(self, count):
        self.first_line_read = False
        self.data = dict()
        self.data['meta'] = {}
        self.data['telegram'] = {}

        counted_datagrams = 0
        while True:
            line = self.read_line()
            if not line.startswith('!') or len(line) != 5:
                continue
            counted_datagrams += 1
            logging.info('skipping datagram %s out of %s', counted_datagrams, count)
            if counted_datagrams >= count:
                return

    def process_datagram(self):
        if not '0-0:1.0.0' in self.data['telegram']:
            logging.warning('Date key "0-0:1.0.0" not in telegram, skipping datagram now')
            return

        datagram_timestamp_string = self.data['telegram']['0-0:1.0.0'].replace('S', 'CEST').replace('W', 'CET')
        datagram_timestamp = datetime.datetime.strptime(datagram_timestamp_string, self.datagram_timestamp_format)
        if int(time.time())  //  self.timesync_period  != self.previous_timesync_period_block:
            self.delta_time = (self.frame_start_time.timestamp() - self.first_datagram_time.timestamp()) - (
                             datagram_timestamp.timestamp() - self.first_datagram_timestamp.timestamp())
            logging.info('Clock time difference drift: %10.6fs', self.delta_time)
            self.first_datagram_time = datetime.datetime.now()
            self.first_datagram_timestamp = datagram_timestamp
            self.previous_timesync_period_block = int(time.time()) // self.timesync_period

        delta_time_in_seconds = (datagram_timestamp - self.first_datagram_timestamp).seconds
        telegram_date = self.first_datagram_time + datetime.timedelta(seconds=delta_time_in_seconds)
        self.data['meta']['datagram-timestamp'] = datagram_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        self.data['meta']['telegram-timestamp'] = round(telegram_date.timestamp())
        self.data['meta']['clock-drift'] = round(self.delta_time, 6)
        self.data['meta']['current-time'] = time.time()

        message = json.dumps(self.data, indent=0)
        logging.debug('Multicast message: %s', message)
        self.multicast_sock.sendto(message.encode(), self.multicast_connection)

    def read_datagram(self, first_line='/ISK5\\2M550T-1013'):
        datagram_counter = 0
        skip_count = get_config_value(category='slimmelezer', key='skip_count', config_type=int, default=10)
        self.skip_datagram(count=skip_count)

        while True:
            line = self.read_line()
            logging.debug('line: %s  %s', self.first_line_read, line)
            if len(line) == 0:
                time.sleep(0.1)
                logging.debug('empty line')
                continue
            elif not self.first_line_read and line.startswith('/'):
                datagram_counter += 1
                logging.debug('First line found')
                self.data['telegram']['header'] = line
                self.frame_start_time = datetime.datetime.now()
                self.data['meta']['frame-start-time'] = time.time()
                self.data['meta']['frame-number'] = datagram_counter
                self.first_line_read = True
                continue
            elif not self.first_line_read:
                logging.warning('Missed header line, input buffer: %s; remainder: %s', self.slimmelezer_buffer.__repr__(), self.slimmelezer_remainder.__repr__())
                self.skip_datagram(count=2)
                continue
            elif line.startswith('!') and len(line) == 5:
                logging.debug('End line found')
                self.data['telegram']['checksum'] = line[1:]
                self.data['meta']['frame-end-time'] = time.time()
                duration = int(1000 * (self.data['meta']['frame-end-time'] - self.data['meta']['frame-start-time']))
                self.data['meta']['frame-time-duration'] = duration

                if '0-0:1.0.0' in self.data['telegram']:
                    self.process_datagram()
                else:
                    logging.warning('Date key "0-0:1.0.0" not in telegram, skipping datagram now')

                self.first_line_read = False
                self.data = dict()
                self.data['meta'] = {}
                self.data['telegram'] = {}
                continue

            p1_more_than_two_values_match = P1_MORE_THAN_TWO_VALUES_RGX.search(line)
            if p1_more_than_two_values_match:
                key = p1_more_than_two_values_match.group('key')
                value = self.parse_p1_value(p1_more_than_two_values_match.group('value'))
                value2 = self.parse_p1_value(p1_more_than_two_values_match.group('value2'))
                self.data['telegram'][key] = value
                logging.debug('Found more than 2 values match: line=%s  key=%s  value1=%s   value2=%s', line, key, value, value2)
                continue

            p1_two_values_match = P1_TWO_VALUES_RGX.search(line)
            if p1_two_values_match:
                key = p1_two_values_match.group('key')
                value1 = self.parse_p1_value(p1_two_values_match.group('value1'))
                value2 = self.parse_p1_value(p1_two_values_match.group('value2'))
                self.data['telegram'][f'{key}.A'] = value1
                self.data['telegram'][f'{key}.B'] = value2
                logging.debug('Found 2 values match: key=%s  value1=%s   value2=%s', key, value1, value2)
                continue

            p1_one_value_match = P1_ONE_VALUE_RGX.search(line)
            if p1_one_value_match:
                key = p1_one_value_match.group('key')
                value = self.parse_p1_value(p1_one_value_match.group('value'))
                self.data['telegram'][key] = value
                logging.debug('[%s] Found 1 value match: key=%s  value=%s', self.data['telegram'].get('0-0:1.0.0', ''), key, value)
                continue

            logging.error('Line not parsed: %s', line.__repr__())


def get_config_value(category, key, config_type=float, default=None):
    global CONFIG
    logging.debug('config: %s', CONFIG)
    if category not in CONFIG:
        logging.warning('Category %s not in config file, returning default %s', category, default)
        return default

    if key not in CONFIG[category]:
        logging.warning('Key %s not in category %s in config file, returning default %s', key, category, default)
        return default

    logging.debug('Returning value %s/%s=%s', category, key, config_type(CONFIG[category][key]))
    return config_type(CONFIG[category][key])


# ##############################################################################

def main():

    global CONFIG

    # Some initialization
    arguments = get_arguments()

    # Configure the logging
    numeric_level = getattr(logging, arguments.log.upper(), None)

    # create formatter
    formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
    logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(funcName)s]   %(message)s',
                        datefmt="%Y%m%d-%H%M%S", level=numeric_level, stream=sys.stdout)

    logging.info("Running version: %s", __version__)
    logging.info("Arguments: %s", arguments)

    script_name = os.path.basename(__file__)

    if not os.path.exists(arguments.config_file):
        logging.error('ERROR: cannot find config file %s, quitting ....', arguments.config_file)
        sys.exit('ERROR: cannot find config file %s, quitting ....' % arguments.config_file)

    logging.info("loading config from '%s'", arguments.config_file)

    path_matcher = re.compile(r'.*\$\{([^}^{]+)\}')
    def path_constructor(loader, node):
        ''' Extract the matched value, expand env variable, and replace the match '''
        value = node.value
        matches = re.findall(r'\$\{\w+?\}', value)
        logging.debug('before value=%s', value)
        for match in matches:
            environment_variable = (match[2:])[:-1]
            environment_value = os.environ.get(environment_variable, environment_variable)
            value = value.replace(match, environment_value)
        logging.debug('after value=%s', value)
        return value

    yaml.add_implicit_resolver('!path', path_matcher)
    yaml.add_constructor('!path', path_constructor)

    with open(arguments.config_file) as inf:
        CONFIG = inf.read()
        CONFIG = jinja2.Template(CONFIG).render()
        CONFIG = yaml.load(CONFIG, Loader=yaml.FullLoader)

    logging.info('config:\n%s', CONFIG)

    slimmelezer_host = get_config_value(category='slimmelezer', key='host', config_type=str)
    slimmelezer_port = get_config_value(category='slimmelezer', key='port', config_type=int)
    timesync_period = get_config_value(category='slimmelezer', key='timesync_period', config_type=int, default=86400)
    socket_timeout = get_config_value(category='multicast', key='socket_timeout', config_type=int, default=3)
    power_meter = SlimmeLezer(host=slimmelezer_host, port=slimmelezer_port, timesync_period=timesync_period, socket_timeout=socket_timeout)

    signal.signal(signal.SIGINT,  do_exit)
    signal.signal(signal.SIGUSR1, do_exit)

    try:
        power_meter.read_datagram()

    except KeyboardInterrupt:
        print("Interrupted, quit")
        power_meter.close_p1_connection()
        sys.exit(1)

#---------------------------------------------------------------------------
#  Main part here
#---------------------------------------------------------------------------
if __name__ == "__main__":
    main()

else:
    # Test several functions
    pass
