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
myQueue(IP[, port]): Implements the TCP queue and initializes
the necessary variables. Uses port 49152 by default if not specified.
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
            data = prodQ.getProducer()
        if alldata != []:
            processData(alldata) # User defined function

if __name__ == ‘__main__’:
    myQ = tcpQueue.myQueue(“127.0.0.1”)
    myQ.startServer()
    myQ.startClient()

    # Run as a background thread to process the results asynchronously.
    worker = threading.Thread(target=procQueue, daemon=True)
    worker.start()

    while True:
        data = getDataToProcess() # User defined function
        for datum in data:
            myQ.sendToConsumer(datum) # Feed the queue
        while myQ.CQSize > SOME_THRESHOLD: # Sleep until the queue has been processed to a low water mark.
            sleep(SOME_NUM_OF_SECS)

For the client:
import tcpQueue
from time import sleep
from sys import argv

def getDatum(myQ):
    datum = myQ.getConsumer()
    while datum == [] or datum is None: # Wait for something to be pushed into the queue
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
"""

import logging
import pickle
import socket
import threading
import zlib
from queue import Queue
from random import random
from sys import (
    exit as sysexit,
)
from time import sleep
from typing import (
    Any,
    Dict,
    List,
    Tuple,
)

SHUTDOWN: bool = False

consumerQueue: Queue = Queue()
producerQueue: Queue = Queue()
workerQueue: Queue = Queue()

consumerLock: threading.Lock = threading.Lock()
prodLock: threading.Lock = threading.Lock()
workerLock: threading.Lock = threading.Lock()
logLock: threading.Lock = threading.Lock()


logging.basicConfig(
    format="%(asctime)s %(levelname)s:\t%(message)s",
    level=logging.DEBUG,
)


def manageWorkers() -> None:
    """Keeps the workers clean and the memory low"""
    global workerLock
    global workerQueue
    logging.info("Starting manageWorkers.")
    while True:
        if SHUTDOWN is True:
            workers: List = []
            workerLock.acquire()
            while workerQueue.qsize() > 0:
                workers.append(workerQueue.get())
            for worker in workers:
                if not worker.is_alive():
                    worker.join(0.1)
                else:
                    workerQueue.put(worker)
            workerLock.release()
            return
        sleep(10)
        workerLock.acquire()
        workers = []
        while workerQueue.qsize() > 0:
            workers.append(workerQueue.get())
        for worker in workers:
            if not worker.is_alive():
                worker.join(0.1)
            else:
                workerQueue.put(worker)
        workerLock.release()


def serverThread(
    client_sock: socket.socket,
) -> None:
    """Manages the server socket."""
    logging.info("Starting new serverThread")
    global consumerLock
    global consumerQueue
    global prodLock
    global producerQueue
    global SHUTDOWN
    while True:
        if SHUTDOWN is True:
            try:
                client_sock.shutdown(socket.SHUT_RDWR)
            except Exception as ex:
                logging.warning(f"Couldn't close client socket: {ex}")
            try:
                client_sock.close()
            except Exception as ex:
                logging.warning(f"Couldn't close client socket: {ex}")
            sysexit(0)
        try:
            cnt: bytes = b""
            total: bytes = b""
            sockData: bytes = b""
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
                    sysexit(0)
                total += cnt
            if len(total) > 9:
                logging.error(
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
                sysexit(0)
            itotal: int = int(total[:-1])
            while len(sockData) < itotal:
                inData: bytes = client_sock.recv(itotal - len(sockData))
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
                    sysexit(0)
                sockData += inData
            cmd = str(sockData).lower()[0]
            if cmd == "c":
                consumerLock.acquire()
                if consumerQueue.qsize() > 0:
                    data: bytes = consumerQueue.get()
                else:
                    data = zlib.compress(
                        pickle.dumps([]),
                        1,
                    )
                consumerLock.release()
                client_sock.sendall(
                    b"%d:%s"
                    % (
                        len(data),
                        data,
                    )
                )
            elif cmd == "p":
                prodLock.acquire()
                if producerQueue.qsize() > 0:
                    data = producerQueue.get()
                else:
                    data = zlib.compress(
                        pickle.dumps([]),
                        1,
                    )
                prodLock.release()
                client_sock.sendall(
                    b"%d:%s"
                    % (
                        len(data),
                        data,
                    )
                )
            else:
                prodLock.acquire()
                producerQueue.put(sockData)
                prodLock.release()
        except Exception as ex:
            logging.error("Unable to process socket connection: %s" % str(ex))
            try:
                client_sock.shutdown(socket.SHUT_RDWR)
            except Exception as ex:
                logging.warning(f"Couldn't close client socket: {ex}")
            try:
                client_sock.close()
            except Exception as ex:
                logging.warning(f"Couldn't close client socket: {ex}")
            sysexit(0)


def controllingThread(
    sock: socket.socket,
) -> None:
    """Starts up new client sockets as needed"""
    logging.info("Starting controllingThread")
    while True:
        try:
            # Wait until someone tries to connect to us.
            (
                client_sock,
                sockname,
            ) = sock.accept()
            # 13s of ping + up to 60s of HTTP(S) timeouts = 73 + 2 for transport overhead
            client_sock.settimeout(75.0)
            worker = threading.Thread(
                target=serverThread,
                args=(client_sock,),
            )
            worker.daemon = True
            worker.start()
            workerLock.acquire()
            workerQueue.put(worker)
            workerLock.release()
        except Exception as ex:
            logging.warning("Unable to accept connection: %s" % str(ex))
            sleep(1)
            continue


class myQueue(object):
    """This is the main TCP Queue class."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 49152,
    ) -> None:
        """Initializes the queueing system but does not start the queues.
        Will use 127.0.0.1 if no host specified, port 49152 if no port specified."""
        logging.info("Initializing new Queue")
        if type(host) is not str:
            logging.error("Started with invalid host")
            return
        if not isinstance(port, int) or port < 1 or port > 65535:
            logging.error("Started with invalid port")
            return

        self.myAddr: Tuple = (
            host,
            port,
        )
        self.ssock: socket.socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        )
        self.csock: socket.socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        )
        self.workerThread: threading.Thread = threading.Thread()
        self.socketThread: threading.Thread = threading.Thread()

    def startServer(
        self,
    ) -> None:
        """Starts up the server queue. Backgrounds the queue for
        asynchronous communication and returns immediately."""
        logging.info("Entering startServer")
        try:
            # Allow multiple connections to the same port
            self.ssock.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_REUSEADDR,
                1,
            )
            # Start sending immediately. Screw Nagle
            self.ssock.setsockopt(
                socket.IPPROTO_TCP,
                socket.TCP_NODELAY,
                1,
            )
            self.ssock.bind(self.myAddr)
            self.ssock.listen(
                128
            )  # Allow a queue of connections waiting to be processed
            self.socketThread = threading.Thread(
                target=controllingThread,
                args=(self.ssock,),
            )
            self.socketThread.daemon = True
            self.socketThread.start()
            self.workerThread = threading.Thread(target=manageWorkers)
            self.workerThread.daemon = True
            self.workerThread.start()
        except Exception as ex:
            logging.error("Unable to start Message Queue Server: %s" % str(ex))
            self.ssock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            )

    def startClient(
        self,
    ) -> None:
        """Connects to the server queue in client mode."""
        logging.info("Entering startClient")
        try:
            self.csock.connect(self.myAddr)
        except Exception as ex:
            logging.error("Unable to connect to server: %s" % str(ex))
            self.close()

    def close(self) -> None:
        """Cleanly shuts down and closes the client queue connection."""
        logging.info("Closing connection")

        try:
            self.csock.shutdown(socket.SHUT_RDWR)
        except Exception as ex:
            logging.warning("Unable to stop Consumer connection: %s" % str(ex))
        try:
            self.csock.close()
        except Exception as ex:
            logging.error("Unable to stop Consumer connection: %s" % str(ex))

    def getConsumer(
        self,
    ) -> Any:
        """Attempts to get the next entry from the Consumer queue on the server.
        Returns the entry if exists
        Returns [] if no data to pull
        Returns None if an error occurred."""
        logging.debug("Entering getConsumer")
        try:
            self.csock.sendall(b"1:c")
            cnt: bytes = b""
            total: bytes = b""
            sockData: bytes = b""
            while cnt != ":" and len(total) < 10:
                cnt = self.csock.recv(1)
                if cnt == "":
                    self.close()
                    return []
                total += cnt
            if len(total) > 9:
                logging.error(
                    "Invalid TCP Consumer message received. Closing connection: %s"
                    % str(total[:9])
                )
                self.close()
                return []
            itotal = int(total[:-1])
            while len(sockData) < itotal:
                inData: bytes = self.csock.recv(itotal - len(sockData))
                if inData == b"":
                    self.close()
                    return []
                sockData += inData
            try:
                data: Any = pickle.loads(zlib.decompress(sockData))
            except:
                data = zlib.decompress(sockData)
            return data
        except Exception as ex:
            logging.warning("Unable to receive data: %s" % str(ex))
        self.close()
        return []

    def sendToProducer(self, blob: Any) -> None:
        """Sends data from the client to the Producer queue for processing by the server
        In event of message failure, will attempt to re-send 3 times.
        After 3 attempts if not successful, will drop the message and continue."""
        logging.debug("Entering sendToProducer")
        sndcnt: int = -1
        while sndcnt < 3:
            sndcnt += 1
            try:
                data = zlib.compress(
                    pickle.dumps(blob),
                    1,
                )
            except:
                data = zlib.compress(blob, 1)
            try:
                self.csock.sendall(
                    b"%d:%s"
                    % (
                        len(data),
                        data,
                    )
                )
                sndcnt = 3
            except Exception as ex:
                self.close()
                logging.warning("Unable to send data: %s" % str(ex))
                sleep(random() * 2)

    def sendToConsumer(self, blob: Any) -> None:
        """Pushes data into the Consumer queue from the server."""
        global consumerLock
        global consumerQueue
        logging.debug("Entering sendToConsumer")
        try:
            data: Any = zlib.compress(
                pickle.dumps(blob),
                9,
            )
        except:
            data = zlib.compress(blob, 1)
        consumerLock.acquire()
        consumerQueue.put(data)
        consumerLock.release()

    def getProducer(
        self,
    ) -> Any:
        """Gets data from the Producer queue on the server.
        Returns the data is exists.
        Returns [] if no data in the queue.
        Returns None if there was an error communicating."""
        logging.debug("Entering getProducer")
        try:
            self.csock.sendall(b"1:p")
            cnt: bytes = b""
            total: bytes = b""
            sockData: bytes = b""
            while cnt != b":" and len(total) < 10:
                cnt = self.csock.recv(1)
                if cnt == "":
                    self.close()
                    return []
                total += cnt
            if len(total) > 9:
                logging.error(
                    "Invalid TCP Producer message received. Closing connection: %s"
                    % str(total[:9])
                )
                self.close()
                return []
            itotal: int = int(total[:-1])
            while len(sockData) < itotal:
                inData = self.csock.recv(itotal - len(sockData))
                if inData == "":
                    self.close()
                    return []
                sockData += inData
        except Exception as ex:
            self.close()
            logging.warning("Unable to receive data: %s" % str(ex))
            sleep(random() * 2.0)
            return []
        try:
            data: Any = pickle.loads(zlib.decompress(sockData))
        except:
            data = zlib.decompress(sockData)
        return data

    def clearQueues(
        self,
    ) -> None:
        logging.debug("Entering clearQueues")
        global consumerLock
        global consumerQueue
        global prodLock
        global producerQueue
        consumerLock.acquire()
        while consumerQueue.qsize() > 0:
            _ = consumerQueue.get()
        consumerLock.release()
        prodLock.acquire()
        while producerQueue.qsize() > 0:
            _ = producerQueue.get()
        prodLock.release()

    def isPQEmpty(self):
        """Returns True if there is no data in the Producer queue, False otherwise.
        This function will only work on the server, not on the clients."""
        logging.debug("Entering isPQEmpty")
        if self.PQSize() > 0:
            return False
        else:
            return True

    def isCQEmpty(self):
        """Returns True if there is no data in the Consumer queue, False otherwise.
        This function will only work on the server, not on the clients."""
        logging.debug("Entering is CQEmpty")
        if self.CQSize() > 0:
            return False
        else:
            return True

    def CQSize(self) -> int:
        """Returns the number of elements in the Consumer queue.
        This function will only work on the server, not on the clients."""
        logging.debug("Entering CQSize")
        global consumerLock
        global consumerQueue
        consumerLock.acquire()
        size: int = consumerQueue.qsize()
        consumerLock.release()
        return size

    def PQSize(self) -> int:
        """Returns the number of elements in the Producer queue.
        This function will only work on the server, not on the clients."""
        logging.debug("Entering PQSize")
        global prodLock
        global producerQueue
        prodLock.acquire()
        size: int = producerQueue.qsize()
        prodLock.release()
        return size

    def __str__(self) -> str:
        """This routine will be called with a str(Class) call and will return
        an str representation of all the variables defined within the class.
        The data is returned as a list of lists. The first element is the
        variable name, the second is the value. The variables are alphabetized.
        [['varname', 'value'], ['varname2', 'value2'] [...]]"""
        from inspect import (
            getmembers,
            isroutine,
        )

        myDict: Dict = {}
        for (
            name,
            obj,
        ) in getmembers(self):
            # Variables that start with __ are "private" so shouldn't be displayed.
            # We don't want to display the names of the functions in the output.
            if not isroutine(obj) and not name.startswith("__"):
                myDict[name] = obj
        retval: List = []
        # Why are we sorting? So the results are always returned in the same order.
        # Dicts can return data in any order, so we sort.
        for i in sorted(myDict.keys()):
            retval.append(
                [
                    i,
                    myDict[i],
                ]
            )
        return str(retval)
