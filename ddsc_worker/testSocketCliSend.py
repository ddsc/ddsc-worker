'''
Created on 12 nov. 2012

@author: LuS

Simulate socket data sending from a sensor called SocketP5007Fugro
'''

# Echo client program
import socket
import time

host = '10.10.101.118' # server on VM Linux
port = 5008
size = 1024
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
timeout = 2
s.settimeout(timeout)
try:
    s.connect((host,port))
    print 'connected'
    for i in range(1,100):
        rowstr = '2000-01-02T00:00:00Z,SocketP5007Fugro,1' + str(i)
        s.send(rowstr)
        time.sleep(1)  # TO VALIDATE TS OBJECT write our own functions here
    s.close()

except socket.error:
    print 'error: connection timeout'
    exit()
