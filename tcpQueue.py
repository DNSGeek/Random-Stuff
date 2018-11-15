# -*- coding: utf-8
"""
This class implements a bidirectional TCP message queue in pure Python.
This class has background threads to process the queues so all requests
are handled asynchronously. It is fully fault tolerent, but does not
guarantee message delivery. If there is a problem sending a message,
the class will attempt to resend the message 3 times, then if it is
still unsuccessful it will give up and drop the message on the floor.

This class uses pickling to allow sending and receiving of any supported
Python data type. Whatever you put into the queue is exactly what you will
pull out of the queue on the other end. Ensure that what you're trying to
send supports pickling. Custom classes, for example, will require additional
work to be pickled.

It uses fast zlib compression to utilize the network effectively. No need to
compress the data on your own before sending or receiving.

Classes:
myQueue(IP[, port]): Implements the TCP queue and initializes the necessary variables.
Uses port 49152 by default if not specified.
myQueue.startServer(): Starts up the server side queue
myQueue.startClient(): Starts up the client side queue
myQueue.close(): Shuts down the client side queue
myQueue.getConsumer(): Pull the Consumer queue from the server
myQueue.sendToConsumer(): Push data into the Consumer queue
myQueue.getProducer(): Pull the Producer queue from the server
myQueue.sendToProducer(): Push data into the Producer queue
myQueue.isPQEmpty(): Returns True if there is no data in the Producer queue
myQueue.isCQEmpty(): Returns True if there is not data in the Consumer queue
myQueue.PQSize(): Returns the number of data in the Producer queue
myQueue.CQSize(): Returns the number of data in the Consumer queue

How to use:

For the main server:
import tcpQueue
import threading
from time import sleep

def procQueue():
    prodQ = tcpQueue.myQueue(“127.0.0.1”)
    prodQ.startClient()
    while True:
        sleep(SOME_NUM_OF_SECS)
        alldata = []
        data = prodQ.getProducer()
        while data != [] and data is not None: # Pull all the data
            alldata.append(data)
            del(data)
            data = prodQ.getProducer()
        if alldata != []:
            processData(alldata) # User defined function
        del(alldata)
        del(data)

if __name__ == ‘__main__’:
    myQ = tcpQueue.myQueue(“127.0.0.1”)
    myQ.startServer()
    myQ.startClient()

    # Run as a background thread to process the results asynchronously.
    worker = threading.Thread(target = procQueue)
    worker.daemon = True
    worker.start()

    while True:
        data = getDataToProcess() # User defined function
        for datum in data:
            myQ.sendToConsumer(datum) # Feed the queue
        del(data)
        if ‘datum’ in locals(): del(datum)
        while myQ.CQSize > SOME_THRESHOLD: # Sleep until the queue has been processed to a low water mark.
            sleep(SOME_NUM_OF_SECS)

For the client:
import tcpQueue
from time import sleep
from sys import argv

def getDatum(myQ):
    datum = myQ.getConsumer()
    while datum == [] or datum is None: # Wait for something to be pushed into the queue
        del(datum)
        sleep(SOME_NUM_OF_SECS)
        datum = myQ.getConsumer()
    return(datum)

if __name__ == ‘__main__’:
    myQ = tcpQueue(argv[1]) # Pass in the IP of the server as the first argument
    myQ.startClient()
    while True:
        datum = getDatum(myQ)
        result = processDatum(datum) # User defined function
        myQ.sendToProducer(result)
        del(datum)
        del(result)
"""

import threading
import socket
import zlib

try:
    import cPickle as pickle
except ImportError:
    import pickle
from platform import node
from time import sleep
from random import random
from string import lower
from collections import deque
from sys import exit

try:
    from nmSys import sendAlert
except ImportError:
    import syslog

DEBUG = False
SHUTDOWN = False

consumerQueue = deque()
producerQueue = deque()
workerQueue = deque()

consumerLock = threading.Lock()
prodLock = threading.Lock()
workerLock = threading.Lock()
logLock = threading.Lock()


def logger(msg, SYSLOGID="pyTCPQueue"):
    """Sends alerts to nmSys or to syslog if nmSys not defined."""
    if len(str(msg)) < 1:
        return  # Bail if no message was sent.

    logLock.acquire()
    if (
        "DEBUG" in globals() and DEBUG is True
    ):  # If in DEBUG mode, just print and return.
        print("%s: %s" % (SYSLOGID, str(msg)))
    else:
        try:
            sendAlert(node(), str(msg), "warn", "rad-sre@%s" % SYSLOGID)
        except:
            syslog.openlog(SYSLOGID)
            syslog.syslog(str(msg))
            syslog.closelog()

    logLock.release()
    return


def manageWorkers():
    """Keeps the workers clean and the memory low"""
    while True:
        if SHUTDOWN is True:
            workerLock.acquire()
            while len(workerQueue) > 0:
                worker = workerQueue.pop()
                if not worker.is_alive():
                    worker.join(0.1)
                else:
                    workerQueue.appendleft(worker)
                del (worker)
            workerLock.release()
            return
        sleep(10)
        workerLock.acquire()
        workers = []
        for i in xrange(0, len(workerQueue)):
            workers.append(workerQueue.pop())
        workerQueue.clear()
        for worker in workers:
            if not worker.is_alive():
                worker.join(0.1)
            else:
                workerQueue.append(worker)
            if "worker" in locals():
                del (worker)
        workerLock.release()
        if "i" in locals():
            del (i)
        if "worker" in locals():
            del (worker)
        if "workers" in locals():
            del (workers)


def controllingThread(sock):
    """Starts up new client sockets as needed"""
    while True:
        try:
            # Wait until someone tries to connect to us.
            client_sock, sockname = sock.accept()
            # 13s of ping + up to 60s of HTTP(S) timeouts = 73 + 2 for transport overhead
            client_sock.settimeout(75.0)
            worker = threading.Thread(target=serverThread, args=(client_sock,))
            worker.daemon = True
            worker.start()
            workerLock.acquire()
            workerQueue.appendleft(worker)
            workerLock.release()
            del (worker)
            del (client_sock)
            del (sockname)
        except Exception as ex:
            logger("Unable to accept connection: %s" % str(ex))
            sleep(1)
            continue


def serverThread(client_sock):
    """Manages the server socket."""
    while True:
        if SHUTDOWN is True:
            try:
                # shutdown will fail if the socket was closed from the other side.
                client_sock.shutdown(socket.SHUT_RDWR)
            except:
                pass
            try:
                # This should never, ever fail....
                client_sock.close()
            except:
                pass
            if "cnt" in locals():
                del (cnt)
            if "total" in locals():
                del (total)
            if "sockData" in locals():
                del (sockData)
            if "inData" in locals():
                del (inData)
            if "client_sock" in locals():
                del (client_sock)
            exit(0)
        try:
            cnt = ""
            total = ""
            sockData = ""
            while cnt != ":" and len(total) < 10:
                cnt = client_sock.recv(1)
                if cnt == "":  # A '' means the socket was closed on us. Bail.
                    try:
                        client_sock.shutdown(socket.SHUT_RDWR)
                    except:
                        pass
                    try:
                        client_sock.close()
                    except:
                        pass
                    if "cnt" in locals():
                        del (cnt)
                    if "total" in locals():
                        del (total)
                    if "sockData" in locals():
                        del (sockData)
                    if "client_sock" in locals():
                        del (client_sock)
                    exit(0)
                total += cnt
            if len(total) > 9:
                logger(
                    "Invalid TCP message received. Closing connection: %s"
                    % str(total[:9])
                )
                try:
                    client_sock.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                try:
                    client_sock.close()
                except:
                    pass
                if "cnt" in locals():
                    del (cnt)
                if "total" in locals():
                    del (total)
                if "sockData" in locals():
                    del (sockData)
                if "client_sock" in locals():
                    del (client_sock)
                exit(0)
            total = int(total[:-1])
            while len(sockData) < total:
                inData = client_sock.recv(total - len(sockData))
                if inData == "":
                    try:
                        # shutdown will fail if the socket was closed from the other side.
                        client_sock.shutdown(socket.SHUT_RDWR)
                    except:
                        pass
                    try:
                        # This should never, ever fail....
                        client_sock.close()
                    except:
                        pass
                    if "cnt" in locals():
                        del (cnt)
                    if "total" in locals():
                        del (total)
                    if "sockData" in locals():
                        del (sockData)
                    if "inData" in locals():
                        del (inData)
                    if "client_sock" in locals():
                        del (client_sock)
                    exit(0)
                sockData += inData
            cmd = lower(str(sockData))[0]
            if cmd == "c":
                consumerLock.acquire()
                if len(consumerQueue) > 0:
                    data = consumerQueue.pop()
                else:
                    consumerQueue.clear()
                    data = zlib.compress(pickle.dumps([]), 1)
                consumerLock.release()
                client_sock.sendall("%d:%s" % (len(data), data))
            elif cmd == "p":
                prodLock.acquire()
                if len(producerQueue) > 0:
                    data = producerQueue.pop()
                else:
                    producerQueue.clear()
                    data = zlib.compress(pickle.dumps([]), 1)
                prodLock.release()
                client_sock.sendall("%d:%s" % (len(data), data))
            else:
                prodLock.acquire()
                producerQueue.appendleft(sockData)
                prodLock.release()
            del (cmd)
        except Exception as ex:
            logger("Unable to process socket connection: %s" % str(ex))
            try:
                client_sock.shutdown(socket.SHUT_RDWR)
            except:
                pass
            try:
                client_sock.close()
            except:
                pass
            if "cnt" in locals():
                del (cnt)
            if "total" in locals():
                del (total)
            if "sockData" in locals():
                del (sockData)
            if "inData" in locals():
                del (inData)
            if "data" in locals():
                del (data)
            if "client_sock" in locals():
                del (client_sock)
            exit(0)

        if "cnt" in locals():
            del (cnt)
        if "total" in locals():
            del (total)
        if "sockData" in locals():
            del (sockData)
        if "inData" in locals():
            del (inData)
        if "data" in locals():
            del (data)


class myQueue(object):
    """This is the main TCP Queue class."""

    def __str__(self):
        """This routine will be called with a str(Class) call and will return
        an str representation of all the variables defined within the class.
        The data is returned as a list of lists. The first element is the
        variable name, the second is the value. The variables are alphabetized.
        [['varname', 'value'], ['varname2', 'value2'] [...]]"""
        from inspect import getmembers, isroutine

        myDict = dict()
        for name, obj in getmembers(self):
            # Variables that start with __ are "private" so shouldn't be displayed.
            # We don't want to display the names of the functions in the output.
            if not isroutine(obj) and not name.startswith("__"):
                myDict[name] = obj
        retval = []
        # Why are we sorting? So the results are always returned in the same order.
        # Dicts can return data in any order, so we sort.
        for i in sorted(myDict.keys()):
            retval.append([i, myDict[i]])
        myDict.clear()
        del (myDict)
        if "name" in locals():
            del (name)
        if "obj" in locals():
            del (obj)
        if "i" in locals():
            del (i)
        return str(retval)

    def __init__(self, host="127.0.0.1", port=49152):
        """Initializes the queueing system but does not start the queues.
        Will use 127.0.0.1 if no host specified, port 49152 if no port specified."""
        if type(host) is not str:
            logger("Started with invalid host")
            return
        if type(port) is not int or (port < 1 or port > 65535):
            logger("Started with invalid port")
            return

        self.myAddr = (host, port)
        self.ssock = None
        self.csock = None
        self.workerThread = None
        self.socketThread = None
        return

    def startServer(self):
        """Starts up the server queue. Backgrounds the queue for
        asynchronous communication and returns immediately."""
        try:
            self.ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Allow multiple connections to the same port, just like a real server. :)
            self.ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Start sending immediately. Screw Nagle
            self.ssock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.ssock.bind(self.myAddr)
            self.ssock.listen(
                128
            )  # Allow a queue of connections waiting to be processed
            self.socketThread = threading.Thread(
                target=controllingThread, args=(self.ssock,)
            )
            self.socketThread.daemon = True
            self.socketThread.start()
            self.workerThread = threading.Thread(target=manageWorkers)
            self.workerThread.daemon = True
            self.workerThread.start()
        except Exception as ex:
            logger("Unable to start Message Queue Server: %s" % str(ex))
            self.ssock = None

        return

    def startClient(self):
        """Connects to the server queue in client mode."""
        try:
            self.csock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.csock.connect(self.myAddr)
        except Exception as ex:
            logger("Unable to connect to server: %s" % str(ex))
            self.close()
        return

    def close(self):
        """Cleanly shuts down and closes the client queue connection."""
        from inspect import getmembers

        if self.csock is not None:
            try:
                self.csock.shutdown(socket.SHUT_RDWR)
            except:
                pass
            try:
                self.csock.close()
            except Exception as ex:
                logger("Unable to stop Consumer connection: %s" % str(ex))
            del (self.csock)
            self.csock = None
        for name, obj in getmembers(self):
            if name.startswith("__"):
                continue  # Ignore private stuff
            if type(obj) is dict:
                try:
                    eval("self.%s.clear()" % name)
                except Exception as ex:
                    print("Unable to clear dictionary %s: %s" % (name, str(ex)))
            if type(obj) is list:
                try:
                    eval("self.%s[:] = []" % name)
                except Exception as ex:
                    print("Unable to clear list %s: %s" % (name, str(ex)))
        if "name" in locals():
            del (name)
        if "obj" in locals():
            del (obj)
        return

    def getConsumer(self):
        """Attempts to get the next entry from the Consumer queue on the server.
        Returns the entry if exists
        Returns [] if no data to pull
        Returns None if an error occurred."""
        if self.csock is None:
            self.startClient()
            if self.csock is None:
                return []
        try:
            self.csock.sendall("1:c")
            cnt = ""
            total = ""
            sockData = ""
            while cnt != ":" and len(total) < 10:
                cnt = self.csock.recv(1)
                if cnt == "":
                    self.close()
                    if "cnt" in locals():
                        del (cnt)
                    if "total" in locals():
                        del (total)
                    if "sockData" in locals():
                        del (sockData)
                    return []
                total += cnt
            if len(total) > 9:
                logger(
                    "Invalid TCP Consumer message received. Closing connection: %s"
                    % str(total[:9])
                )
                self.close()
                if "cnt" in locals():
                    del (cnt)
                if "total" in locals():
                    del (total)
                if "sockData" in locals():
                    del (sockData)
                return []
            total = int(total[:-1])
            while len(sockData) < total:
                inData = self.csock.recv(total - len(sockData))
                if inData == "":
                    self.close()
                    if "cnt" in locals():
                        del (cnt)
                    if "total" in locals():
                        del (total)
                    if "sockData" in locals():
                        del (sockData)
                    if "inData" in locals():
                        del (inData)
                    return []
                sockData += inData
            try:
                data = pickle.loads(zlib.decompress(sockData))
            except:
                data = zlib.decompress(sockData)
            del (cnt)
            del (total)
            del (sockData)
            del (inData)
            return data
        except Exception as ex:
            logger("Unable to receive data: %s" % str(ex))
        self.close()
        if "cnt" in locals():
            del (cnt)
        if "total" in locals():
            del (total)
        if "sockData" in locals():
            del (sockData)
        if "inData" in locals():
            del (inData)
        if "data" in locals():
            del (data)
        return []

    def sendToProducer(self, blob):
        """Sends data from the client to the Producer queue for processing by the server
        In event of message failure, will attempt to re-send 3 times.
        After 3 attempts if not successful, will drop the message and continue."""
        sndcnt = -1
        while sndcnt < 3:
            sndcnt += 1
            if self.csock is None:
                self.startClient()
                if self.csock is None:
                    return
            try:
                data = zlib.compress(pickle.dumps(blob), 1)
            except:
                data = zlib.compress(blob, 1)
            try:
                self.csock.sendall("%d:%s" % (len(data), data))
                sndcnt = 3
            except Exception as ex:
                self.close()
                logger("Unable to send data: %s" % str(ex))
                sleep(random() * 2)
        del (sndcnt)
        if "data" in locals():
            del (data)
        return

    def sendToConsumer(self, blob):
        """Pushes data into the Consumer queue from the server."""
        try:
            data = zlib.compress(pickle.dumps(blob), 9)
        except:
            data = zlib.compress(blob, 1)
        consumerLock.acquire()
        consumerQueue.appendleft(data)
        consumerLock.release()
        del (data)
        return

    def getProducer(self):
        """Gets data from the Producer queue on the server.
        Returns the data is exists.
        Returns [] if no data in the queue.
        Returns None if there was an error communicating."""
        if self.csock is None:
            self.startClient()
            if self.csock is None:
                logger("Unable to get Producer data")
                return []
        try:
            self.csock.sendall("1:p")
            cnt = ""
            total = ""
            sockData = ""
            while cnt != ":" and len(total) < 10:
                cnt = self.csock.recv(1)
                if cnt == "":
                    self.close()
                    if "cnt" in locals():
                        del (cnt)
                    if "total" in locals():
                        del (total)
                    if "sockData" in locals():
                        del (sockData)
                    return []
                total += cnt
            if len(total) > 9:
                logger(
                    "Invalid TCP Producer message received. Closing connection: %s"
                    % str(total[:9])
                )
                self.close()
                if "cnt" in locals():
                    del (cnt)
                if "total" in locals():
                    del (total)
                if "sockData" in locals():
                    del (sockData)
                return []
            total = int(total[:-1])
            while len(sockData) < total:
                inData = self.csock.recv(total - len(sockData))
                if inData == "":
                    self.close()
                    if "cnt" in locals():
                        del (cnt)
                    if "total" in locals():
                        del (total)
                    if "sockData" in locals():
                        del (sockData)
                    if "inData" in locals():
                        del (inData)
                    return []
                sockData += inData
        except Exception as ex:
            self.close()
            logger("Unable to receive data: %s" % str(ex))
            sleep(random() * 2)
            if "cnt" in locals():
                del (cnt)
            if "total" in locals():
                del (total)
            if "sockData" in locals():
                del (sockData)
            if "inData" in locals():
                del (inData)
            return []
        try:
            data = pickle.loads(zlib.decompress(sockData))
        except:
            data = zlib.decompress(sockData)

        if "cnt" in locals():
            del (cnt)
        if "total" in locals():
            del (total)
        if "sockData" in locals():
            del (sockData)
        if "inData" in locals():
            del (inData)
        return data

    def clearQueues(self):
        consumerLock.acquire()
        consumerQueue.clear()
        consumerLock.release()
        prodLock.acquire()
        producerQueue.clear()
        prodLock.release()
        return

    def isPQEmpty(self):
        """Returns True if there is no data in the Producer queue, False otherwise.
        This function will only work on the server, not on the clients."""
        if self.PQSize() > 0:
            return False
        else:
            return True

    def isCQEmpty(self):
        """Returns True if there is no data in the Consumer queue, False otherwise.
        This function will only work on the server, not on the clients."""
        if self.CQSize() > 0:
            return False
        else:
            return True

    def CQSize(self):
        """Returns the number of elements in the Consumer queue.
        This function will only work on the server, not on the clients."""
        return len(consumerQueue)

    def PQSize(self):
        """Returns the number of elements in the Producer queue.
        This function will only work on the server, not on the clients."""
        return len(producerQueue)
