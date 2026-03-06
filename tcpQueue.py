# -*- coding: utf-8 -*-
"""
This class implements a bidirectional TCP message queue in pure Python.
This class has background threads to process the queues so all requests
are handled asynchronously. It is fully fault tolerant, but does not
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
myQueue.isCQEmpty(): Returns True if there is no data in the Consumer queue
myQueue.PQSize(): Returns the number of items in the Producer queue
myQueue.CQSize(): Returns the number of items in the Consumer queue

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
        while data != [] and data is not None:  # Pull all the data
            alldata.append(data)
            data = prodQ.getProducer()
        if alldata:
            processData(alldata)  # User defined function

if __name__ == '__main__':
    myQ = tcpQueue.myQueue("127.0.0.1")
    myQ.startServer()
    myQ.startClient()

    # Run as a background thread to process the results asynchronously.
    worker = threading.Thread(target=procQueue)
    worker.daemon = True
    worker.start()

    while True:
        data = getDataToProcess()  # User defined function
        for datum in data:
            myQ.sendToConsumer(datum)  # Feed the queue
        while myQ.CQSize() > SOME_THRESHOLD:  # Sleep until the queue drains to a low water mark.
            sleep(SOME_NUM_OF_SECS)

For the client:
import tcpQueue
from time import sleep
from sys import argv

def getDatum(myQ):
    datum = myQ.getConsumer()
    while datum == [] or datum is None:  # Wait for something to be pushed into the queue
        sleep(SOME_NUM_OF_SECS)
        datum = myQ.getConsumer()
    return datum

if __name__ == '__main__':
    myQ = tcpQueue.myQueue(argv[1])  # Pass in the IP of the server as the first argument
    myQ.startClient()
    while True:
        datum = getDatum(myQ)
        result = processDatum(datum)  # User defined function
        myQ.sendToProducer(result)
"""

import logging
import pickle
import socket
import threading
import zlib
from collections import deque
from random import random
from time import sleep
from typing import Any, Optional

DEBUG: bool = False
SHUTDOWN: bool = False

consumerQueue: deque[bytes] = deque()
producerQueue: deque[bytes] = deque()
workerQueue: deque[threading.Thread] = deque()

consumerLock: threading.Lock = threading.Lock()
prodLock: threading.Lock = threading.Lock()
workerLock: threading.Lock = threading.Lock()
logLock: threading.Lock = threading.Lock()

# Pre-encode the empty-queue sentinel once rather than re-compressing on every miss.
_EMPTY_PAYLOAD: bytes = zlib.compress(pickle.dumps([]), 1)


def _make_frame(data: bytes) -> bytes:
    """Wrap a payload in the length-prefix framing format: b'<len>:<data>'."""
    return str(len(data)).encode() + b":" + data


def logger(msg: Any, SYSLOGID: str = "pyTCPQueue") -> None:
    """Sends alerts to syslog."""
    msg_str = str(msg)
    if not msg_str:
        return

    # Capture the log string before acquiring the lock so the lock is held only
    # for the minimal critical section (the print/syslog call itself).
    with logLock:
        logging.debug("%s: %s" % (SYSLOGID, msg_str))


def manageWorkers() -> None:
    """Reaps finished worker threads to keep memory tidy."""
    while True:
        if SHUTDOWN:
            with workerLock:
                while workerQueue:
                    worker: threading.Thread = workerQueue.pop()
                    if not worker.is_alive():
                        worker.join(0.1)
                    else:
                        workerQueue.appendleft(worker)
                        sleep(
                            0.1
                        )  # Avoid busy-spinning while waiting for threads to finish.
            return

        sleep(10)
        with workerLock:
            # Drain the queue, reap finished threads, re-enqueue the rest.
            live: list[threading.Thread] = []
            while workerQueue:
                worker = workerQueue.pop()
                if worker.is_alive():
                    live.append(worker)
                else:
                    worker.join(0.1)
            for worker in live:
                workerQueue.appendleft(worker)


def _recv_exactly(sock: socket.socket, n: int) -> Optional[bytes]:
    """Receive exactly n bytes from a socket.
    Returns None if the connection closes before n bytes arrive."""
    chunks: list[bytes] = []
    received: int = 0
    while received < n:
        chunk: bytes = sock.recv(n - received)
        if not chunk:
            return None
        chunks.append(chunk)
        received += len(chunk)
    return b"".join(chunks)


def _read_framed_message(sock: socket.socket) -> Optional[bytes]:
    """Read a length-prefixed message in the format b'LEN:DATA'.
    Returns the raw bytes payload, or None if the connection was closed.
    Raises ValueError for a malformed frame."""
    header: bytes = b""
    while len(header) < 10:
        byte: bytes = sock.recv(1)
        if not byte:
            return None
        if byte == b":":
            break
        header += byte
    else:
        # Consumed 10 bytes without finding a colon — malformed frame.
        raise ValueError("Frame header too long or missing colon: %s" % header[:9])

    if not header:
        raise ValueError("Empty frame header")

    length: int = int(header)
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
    """Manages a single accepted client connection on the server side.
    Runs until the client disconnects, an unrecoverable error occurs, or
    SHUTDOWN is set. Exits by returning (not sys.exit) so the thread dies
    cleanly without taking down the whole process."""
    try:
        while True:
            if SHUTDOWN:
                return

            try:
                sockData: Optional[bytes] = _read_framed_message(client_sock)
            except ValueError as ex:
                logger("Malformed frame received, closing connection: %s" % str(ex))
                return

            if sockData is None:
                # Client closed the connection cleanly.
                return

            cmd: bytes = sockData[:1].lower()
            data: bytes

            if cmd == b"c":
                with consumerLock:
                    data = consumerQueue.pop() if consumerQueue else _EMPTY_PAYLOAD
                client_sock.sendall(_make_frame(data))

            elif cmd == b"p":
                with prodLock:
                    data = producerQueue.pop() if producerQueue else _EMPTY_PAYLOAD
                client_sock.sendall(_make_frame(data))

            else:
                with prodLock:
                    producerQueue.appendleft(sockData)

    except Exception as ex:
        logger("Unexpected error in server connection thread: %s" % str(ex))
    finally:
        _close_socket(client_sock)


def controllingThread(sock: socket.socket) -> None:
    """Accepts incoming connections and dispatches each to a serverThread."""
    while True:
        try:
            client_sock, _sockname = sock.accept()
            # 13s of ping + up to 60s of HTTP(S) timeouts = 73 + 2 for transport overhead
            client_sock.settimeout(75.0)
            worker = threading.Thread(target=serverThread, args=(client_sock,))
            worker.daemon = True
            worker.start()
            with workerLock:
                workerQueue.appendleft(worker)
        except Exception as ex:
            logger("Unable to accept connection: %s" % str(ex))
            sleep(1)


class myQueue:
    """Bidirectional TCP message queue."""

    def __init__(self, host: str = "127.0.0.1", port: int = 49152) -> None:
        """Initializes the queueing system but does not start the queues.
        Uses 127.0.0.1 if no host specified, port 49152 if no port specified.
        Raises ValueError for invalid host or port."""
        if not isinstance(host, str):
            raise ValueError("host must be a string, got %s" % type(host).__name__)
        if not isinstance(port, int) or not (1 <= port <= 65535):
            raise ValueError(
                "port must be an integer between 1 and 65535, got %r" % port
            )

        self.myAddr: tuple[str, int] = (host, port)
        self.ssock: Optional[socket.socket] = None
        self.csock: Optional[socket.socket] = None
        self.workerThread: Optional[threading.Thread] = None
        self.socketThread: Optional[threading.Thread] = None

    def startServer(self) -> None:
        """Starts the server queue. Backgrounds the listener for
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

    def startClient(self) -> None:
        """Connects to the server queue in client mode."""
        try:
            self.csock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.csock.connect(self.myAddr)
        except Exception as ex:
            logger("Unable to connect to server: %s" % str(ex))
            self.close()

    def close(self) -> None:
        """Cleanly shuts down and closes the client connection."""
        if self.csock is not None:
            _close_socket(self.csock)
            self.csock = None

        # Clear any instance-level dicts and lists.
        for name, obj in vars(self).items():
            if isinstance(obj, dict):
                try:
                    obj.clear()
                except Exception as ex:
                    logging.error(f"Unable to clear dictionary {name}: {ex}")
            elif isinstance(obj, list):
                try:
                    obj.clear()
                except Exception as ex:
                    logging.error(f"Unable to clear list {name}: {ex}")

    def getConsumer(self) -> Any:
        """Attempts to get the next entry from the Consumer queue.
        Returns the entry if it exists.
        Returns [] if the queue is empty.
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
            logger("Unable to receive Consumer data: %s" % str(ex))
            self.close()
            return []

    def sendToProducer(self, blob: Any) -> None:
        """Sends data to the Producer queue for processing by the server.
        On failure, retries up to 3 times with a random back-off, then drops
        the message and continues."""
        data: bytes
        try:
            data = zlib.compress(pickle.dumps(blob), 1)
        except Exception:
            data = zlib.compress(blob, 1)

        for attempt in range(3):
            if self.csock is None:
                self.startClient()
                if self.csock is None:
                    return
            try:
                self.csock.sendall(_make_frame(data))
                return  # Success — done.
            except Exception as ex:
                self.close()
                logger(
                    "Unable to send data (attempt %d/3): %s" % (attempt + 1, str(ex))
                )
                sleep(random() * 2)

    def sendToConsumer(self, blob: Any) -> None:
        """Pushes data into the Consumer queue (server side)."""
        try:
            data: bytes = zlib.compress(pickle.dumps(blob), 1)
        except Exception:
            data = zlib.compress(blob, 1)
        with consumerLock:
            consumerQueue.appendleft(data)

    def getProducer(self) -> Any:
        """Gets the next item from the Producer queue.
        Returns the data if it exists.
        Returns [] if the queue is empty.
        Returns None if there was a communication error."""
        if self.csock is None:
            self.startClient()
            if self.csock is None:
                logger("Unable to get Producer data: could not connect")
                return []
        try:
            self.csock.sendall(b"1:p")
            sockData: Optional[bytes] = _read_framed_message(self.csock)
            if sockData is None:
                self.close()
                return []
        except Exception as ex:
            self.close()
            logger("Unable to receive Producer data: %s" % str(ex))
            sleep(random() * 2)
            return []
        try:
            return pickle.loads(zlib.decompress(sockData))
        except Exception:
            return zlib.decompress(sockData)

    def clearQueues(self) -> None:
        """Clears both the Consumer and Producer queues."""
        with consumerLock:
            consumerQueue.clear()
        with prodLock:
            producerQueue.clear()

    def isPQEmpty(self) -> bool:
        """Returns True if the Producer queue is empty.
        Only meaningful on the server side."""
        return self.PQSize() == 0

    def isCQEmpty(self) -> bool:
        """Returns True if the Consumer queue is empty.
        Only meaningful on the server side."""
        return self.CQSize() == 0

    def CQSize(self) -> int:
        """Returns the number of items in the Consumer queue.
        Only meaningful on the server side."""
        return len(consumerQueue)

    def PQSize(self) -> int:
        """Returns the number of items in the Producer queue.
        Only meaningful on the server side."""
        return len(producerQueue)

    def __str__(self) -> str:
        """Returns a string representation of all non-private instance
        variables, as a sorted list of [name, value] pairs."""
        from inspect import getmembers, isroutine

        myDict: dict[str, Any] = {
            name: obj
            for name, obj in getmembers(self)
            if not isroutine(obj) and not name.startswith("__")
        }
        return str([[k, myDict[k]] for k in sorted(myDict.keys())])
