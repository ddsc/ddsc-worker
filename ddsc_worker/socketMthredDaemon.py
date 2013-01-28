'''
Created on 14 nov. 2012

@author: LuS

Based on Python offical document 
20.17.4.3. Asynchronous Mixins
'''
import threading
import SocketServer
import time

import logging

from daemon import runner

class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        print "connection established with:  %r" % (self.client_address[0])
        first_time = time.time()
        current_time = time.time()
        timeout = 20
        
        path = '/home/alex/testdata/socket/'   ## TO BE put in a django setting file
        fileName = self.client_address[0] + '_' + str(self.client_address[1]) + '_'
        
        i=1
        while 1:           
            f = open(path+fileName+str(i)+'.csv', 'wb')
            while timeout > (current_time - first_time):
                try :
                    self.request.send("ok")
                    data = self.request.recv(1024)
                    f.write(data+'\r\n')
                    print "%r:%r wrote a line in %r" % (self.client_address[0],
                                                        self.client_address[1],f)
                    current_time = time.time()
                except :
                    print "connection with %r:%r lost" % (self.client_address[0], 
                                                          str(self.client_address[1]))
                    exit()
            i+=1
            first_time = time.time()


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

class App():
   
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path =  '/var/run/ddsc/tcpMthredDaemon.pid'
        self.pidfile_timeout = 5
           
    def run(self):
        #Main code goes here ...
        #Note that logger level needs to be set to logging.DEBUG before this shows up in the logs
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warn("Warning message")
        logger.error("Error message")

        HOST, PORT = "10.10.101.118", 5008
        server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
        ip, port = server.server_address

        server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()
        print "Server loop running in thread:", server_thread.name
        while True:
            time.sleep(1000)

app = App()
logger = logging.getLogger("DaemonLog")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler = logging.FileHandler("/var/log/ddsc/testdaemon.log")  ## TO be put in a django setting file
handler.setFormatter(formatter)
logger.addHandler(handler)

daemon_runner = runner.DaemonRunner(app)
#This ensures that the logger file handle does not get closed during daemonization
daemon_runner.daemon_context.files_preserve=[handler.stream]
daemon_runner.do_action()
