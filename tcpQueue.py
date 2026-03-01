# -*- coding: utf-8 -*-
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
    prodQ = tcpQueue.myQueue("127.0.0.1")
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

if __name__ == '__main__':
    myQ = tcpQueue.myQueue("127.0.0.1")
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
        if 'datum' in locals(): del(datum)
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

if __name__ == '__main__':
    myQ = tcpQueue(argv[1]) # Pass in the IP of the server as the first argument
    myQ.startClient()
    while True:
        datum = getDatum(myQ)
        result = processDatum(datum) # User defined function
        myQ.sendToProducer(result)
        del(datum)
        del(result)
"""

import pickle
import socket
import threading
import zlib
from collections import deque
from platform import node
from random import random
from sys import exit
from time import sleep
from typing import Any, Optional

try:
    from nmSys import sendAlert
except ImportError:
    import syslog

DEBUG: bool = False
SHUTDOWN: bool = False

consumerQueue: deque[bytes] = deque()
producerQueue: deque[bytes] = deque()
workerQueue: deque[threading.Thread] = deque()

consumerLock: threading.Lock = threading.Lock()
prodLock: threading.Lock = threading.Lock()
workerLock: threading.Lock = threading.Lock()
logLock: threading.Lock = threading.Lock()


def logger(msg: Any, SYSLOGID: str = "pyTCPQueue") -> None:
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
        except Exception:
            syslog.openlog(SYSLOGID)
            syslog.syslog(str(msg))
            syslog.closelog()

    logLock.release()
    return


def manageWorkers() -> None:
    """Keeps the workers clean and the memory low."""
    while True:
        if SHUTDOWN is True:
            workerLock.acquire()
            while len(workerQueue) > 0:
                worker: threading.Thread = workerQueue.pop()
                if not worker.is_alive():
                    worker.join(0.1)
                else:
                    workerQueue.appendleft(worker)
                del worker
            workerLock.release()
            return
        sleep(10)
        workerLock.acquire()
        workers: list[threading.Thread] = []
        for i in range(0, len(workerQueue)):
            workers.append(workerQueue.pop())
        workerQueue.clear()
        for worker in workers:
            if not worker.is_alive():
                worker.join(0.1)
            else:
                workerQueue.append(worker)
        workerLock.release()


def _recv_exactly(sock: socket.socket, n: int) -> Optional[bytes]:
    """Receive exactly n bytes from a socket. Returns None if the connection closes early."""
    buf: bytes = b""
    while len(buf) < n:
        chunk: bytes = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def _read_framed_message(sock: socket.socket) -> Optional[bytes]:
    """Read a length-prefixed message in the format 'LEN:DATA'.
    Returns the raw bytes payload, None if the connection was closed,
    or raises ValueError for a malformed frame."""
    cnt: bytes = b""
    total: bytes = b""
    while cnt != b":" and len(total) < 10:
        cnt = sock.recv(1)
        if not cnt:
            return None
        total += cnt
    if len(total) > 9:
        raise ValueError(
            "Invalid TCP message frame header: %s" % str(total[:9])
        )
    length: int = int(total[:-1])  # strip the trailing ':'
    return _recv_exactly(sock, length)


def _close_socket(sock: socket.socket) -> None:
    """Best-effort shutdown and close of a socket."""
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except Exception:
        pass
    try:
        sock.close()
    except Exception:
        pass


def serverThread(client_sock: socket.socket) -> None:
    """Manages the server socket."""
    while True:
        if SHUTDOWN is True:
            _close_socket(client_sock)
            exit(0)
        try:
            sockData: Optional[bytes] = _read_framed_message(client_sock)
            if sockData is None:
                _close_socket(client_sock)
                exit(0)

            cmd: bytes = sockData[:1].lower()
            data: bytes
            if cmd == b"c":
                consumerLock.acquire()
                if len(consumerQueue) > 0:
                    data = consumerQueue.pop()
                else:
                    consumerQueue.clear()
                    data = zlib.compress(pickle.dumps([]), 1)
                consumerLock.release()
                frame: bytes = b"%d:" % len(data) + data
                client_sock.sendall(frame)
            elif cmd == b"p":
                prodLock.acquire()
                if len(producerQueue) > 0:
                    data = producerQueue.pop()
                else:
                    producerQueue.clear()
                    data = zlib.compress(pickle.dumps([]), 1)
                prodLock.release()
                frame = b"%d:" % len(data) + data
                client_sock.sendall(frame)
            else:
                prodLock.acquire()
                producerQueue.appendleft(sockData)
                prodLock.release()

        except ValueError as ex:
            logger("Malformed frame received: %s" % str(ex))
            _close_socket(client_sock)
            exit(0)
        except Exception as ex:
            logger("Unable to process socket connection: %s" % str(ex))
            _close_socket(client_sock)
            exit(0)


def controllingThread(sock: socket.socket) -> None:
    """Starts up new client sockets as needed."""
    while True:
        try:
            client_sock, sockname = sock.accept()
            # 13s of ping + up to 60s of HTTP(S) timeouts = 73 + 2 for transport overhead
            client_sock.settimeout(75.0)
            worker = threading.Thread(target=serverThread, args=(client_sock,))
            worker.daemon = True
            worker.start()
            workerLock.acquire()
            workerQueue.appendleft(worker)
            workerLock.release()
        except Exception as ex:
            logger("Unable to accept connection: %s" % str(ex))
            sleep(1)
            continue


class myQueue:
    """This is the main TCP Queue class."""

    def __init__(self, host: str = "127.0.0.1", port: int = 49152) -> None:
        """Initializes the queueing system but does not start the queues.
        Will use 127.0.0.1 if no host specified, port 49152 if no port specified.
        """
        if not isinstance(host, str):
            logger("Started with invalid host")
            return
        if not isinstance(port, int) or not (1 <= port <= 65535):
            logger("Started with invalid port")
            return

        self.myAddr: tuple[str, int] = (host, port)
        self.ssock: Optional[socket.socket] = None
        self.csock: Optional[socket.socket] = None
        self.workerThread: Optional[threading.Thread] = None
        self.socketThread: Optional[threading.Thread] = None
        return

    def startServer(self) -> None:
        """Starts up the server queue. Backgrounds the queue for
        asynchronous communication and returns immediately."""
        try:
            self.ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.ssock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.ssock.bind(self.myAddr)
            self.ssock.listen(128)
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

    def startClient(self) -> None:
        """Connects to the server queue in client mode."""
        try:
            self.csock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.csock.connect(self.myAddr)
        except Exception as ex:
            logger("Unable to connect to server: %s" % str(ex))
            self.close()
        return

    def close(self) -> None:
        """Cleanly shuts down and closes the client queue connection."""
        from inspect import getmembers

        if self.csock is not None:
            _close_socket(self.csock)
            del self.csock
            self.csock = None
        for name, obj in getmembers(self):
            if name.startswith("__"):
                continue
            if isinstance(obj, dict):
                try:
                    eval("self.%s.clear()" % name)
                except Exception as ex:
                    print(
                        "Unable to clear dictionary %s: %s" % (name, str(ex))
                    )
            if isinstance(obj, list):
                try:
                    eval("self.%s[:] = []" % name)
                except Exception as ex:
                    print("Unable to clear list %s: %s" % (name, str(ex)))
        return

    def getConsumer(self) -> Any:
        """Attempts to get the next entry from the Consumer queue on the server.
        Returns the entry if it exists.
        Returns [] if no data to pull.
        Returns None if an error occurred."""
        if self.csock is None:
            self.startClient()
            if self.csock is None:
                return []
        try:
            self.csock.sendall(b"1:c")
            sockData: Optional[bytes] = _read_framed_message(self.csock)
            if sockData is None:
                self.close()
                return []
            try:
                return pickle.loads(zlib.decompress(sockData))
            except Exception:
                return zlib.decompress(sockData)
        except Exception as ex:
            logger("Unable to receive data: %s" % str(ex))
        self.close()
        return []

    def sendToProducer(self, blob: Any) -> None:
        """Sends data from the client to the Producer queue for processing by the server.
        In event of message failure, will attempt to re-send 3 times.
        After 3 attempts if not successful, will drop the message and continue.
        """
        sndcnt: int = -1
        while sndcnt < 3:
            sndcnt += 1
            if self.csock is None:
                self.startClient()
                if self.csock is None:
                    return
            data: bytes
            try:
                data = zlib.compress(pickle.dumps(blob), 1)
            except Exception:
                data = zlib.compress(blob, 1)
            try:
                frame: bytes = b"%d:" % len(data) + data
                self.csock.sendall(frame)
                sndcnt = 3
            except Exception as ex:
                self.close()
                logger("Unable to send data: %s" % str(ex))
                sleep(random() * 2)
        return

    def sendToConsumer(self, blob: Any) -> None:
        """Pushes data into the Consumer queue from the server."""
        data: bytes
        try:
            data = zlib.compress(pickle.dumps(blob), 9)
        except Exception:
            data = zlib.compress(blob, 1)
        consumerLock.acquire()
        consumerQueue.appendleft(data)
        consumerLock.release()
        return

    def getProducer(self) -> Any:
        """Gets data from the Producer queue on the server.
        Returns the data if it exists.
        Returns [] if no data in the queue.
        Returns None if there was an error communicating."""
        if self.csock is None:
            self.startClient()
            if self.csock is None:
                logger("Unable to get Producer data")
                return []
        try:
            self.csock.sendall(b"1:p")
            sockData: Optional[bytes] = _read_framed_message(self.csock)
            if sockData is None:
                self.close()
                return []
        except Exception as ex:
            self.close()
            logger("Unable to receive data: %s" % str(ex))
            sleep(random() * 2)
            return []
        try:
            return pickle.loads(zlib.decompress(sockData))
        except Exception:
            return zlib.decompress(sockData)

    def clearQueues(self) -> None:
        consumerLock.acquire()
        consumerQueue.clear()
        consumerLock.release()
        prodLock.acquire()
        producerQueue.clear()
        prodLock.release()
        return

    def isPQEmpty(self) -> bool:
        """Returns True if there is no data in the Producer queue, False otherwise.
        This function will only work on the server, not on the clients."""
        return self.PQSize() == 0

    def isCQEmpty(self) -> bool:
        """Returns True if there is no data in the Consumer queue, False otherwise.
        This function will only work on the server, not on the clients."""
        return self.CQSize() == 0

    def CQSize(self) -> int:
        """Returns the number of elements in the Consumer queue.
        This function will only work on the server, not on the clients."""
        return len(consumerQueue)

    def PQSize(self) -> int:
        """Returns the number of elements in the Producer queue.
        This function will only work on the server, not on the clients."""
        return len(producerQueue)

    def __str__(self) -> str:
        """Returns a str representation of all non-private instance variables, alphabetized.
        The data is returned as a list of lists: [['varname', value], ...]"""
        from inspect import getmembers, isroutine

        myDict: dict[str, Any] = {
            name: obj
            for name, obj in getmembers(self)
            if not isroutine(obj) and not name.startswith("__")
        }
        retval: list[list[Any]] = [
            [k, myDict[k]] for k in sorted(myDict.keys())
        ]
        return str(retval)
