# p1_slimmelezer

This software is used to process P1 dutch smart meter data as being read by the "slimmelezer" as sold by Marcel Zuidwijk (https://www.zuidwijk.com/).
This device is connecting to the P1 port of the smart meter and exposes a TCP connection to the world to publish the data.
The p1-slimmelezer.py script is connecting to this device and multicasting the result to the network.
