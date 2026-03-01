#!/usr/bin/env python3

import gc
import os
import queue
import signal
import socket
import threading
import time
from random import random
from sys import argv, exit
from types import FrameType
from typing import Optional

myState: str = "D"
workerQueue: queue.Queue[threading.Thread] = queue.Queue()
workerLock: threading.Lock = threading.Lock()
TDELAY: float = 5.0
DEBUG: bool = True


def sigint_handler(sig: int, frame: Optional[FrameType]) -> None:
    # Stub routine in case cleanup is needed.
    print("\n\n*** Signal caught, exiting. ***\n\n")
    exit(0)


def checkForServer(processname: str = "nsn_server.py") -> None:
    global myState
    if DEBUG:
        print("Checking for %s process" % processname)
    try:
        nsnserver = os.popen(
            "/bin/ps -C %s --no-heading 2>/dev/null" % processname
        )
        srvcnt: str = nsnserver.readline()
        nsnserver.close()
    except Exception as ex:
        if DEBUG:
            print("Unable to check for NSN Server process: %s" % str(ex))
        myState = "S"
        return
    if len(srvcnt) == 0:  # If no process running, demote to Secondary.
        myState = "S"
    return


def countdown() -> None:
    global myState
    if DEBUG:
        print("Countdown reached. Setting myself as primary")
    myState = "P"
    return


def manageWorkers() -> None:
    while True:
        time.sleep(10)
        workers: list[threading.Thread] = []
        workerLock.acquire()
        while not workerQueue.empty():
            worker: threading.Thread = workerQueue.get()
            if not worker.is_alive():
                worker.join(0.1)
                if DEBUG:
                    print("*** Removed worker *** ")
            else:
                workers.append(worker)
        for worker in workers:
            workerQueue.put(worker)
        workerLock.release()
        gc.collect()


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


def clientThread(client_sock: socket.socket) -> None:
    global myState
    while True:
        try:
            cnt: bytes = client_sock.recv(1)
        except Exception:
            _close_socket(client_sock)
            exit(0)
        if not cnt:
            _close_socket(client_sock)
            exit(0)
        if DEBUG:
            print("Thread received %s" % str(cnt))
        if cnt == b"g":
            try:
                client_sock.sendall(myState.encode())
                if DEBUG:
                    print("Sent %s" % myState)
                continue
            except Exception as ex:
                if DEBUG:
                    print("Error sending data from thread: %s" % str(ex))
                _close_socket(client_sock)
                exit(0)
        if cnt == b"r":
            try:
                client_sock.sendall(bytes([int(random() * 10)]))
                continue
            except Exception as ex:
                if DEBUG:
                    print(
                        "Error sending re-election data from thread: %s"
                        % str(ex)
                    )
                _close_socket(client_sock)
                exit(0)
        if cnt == b"P" or cnt == b"S":
            myState = cnt.decode()
            continue
        # Someone sent something weird.
        if DEBUG:
            print(
                "Unknown message %s received by thread. Closing thread."
                % str(cnt)
            )
        _close_socket(client_sock)
        exit(0)


def serverThread(ssock: socket.socket) -> None:
    while True:
        client_sock, sockname = ssock.accept()
        client_sock.settimeout(5.0)
        client_sock.setblocking(True)
        worker = threading.Thread(target=clientThread, args=(client_sock,))
        worker.daemon = True
        worker.start()
        workerLock.acquire()
        workerQueue.put(worker)
        workerLock.release()
    return


if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGHUP, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)
    try:
        if len(argv) < 3:
            print("USAGE: %s Server_IP Remote_Server_IP" % str(argv[0]))
            exit(0)
        else:
            remoteip: str = str(argv[2])
            myip: str = str(argv[1])
    except Exception as ex:
        print("Invalid command line for heartbeat service: %s" % str(ex))
        exit(-1)

    HBaddr: tuple[str, int] = (myip, 53281)

    mwthread: threading.Thread = threading.Thread(target=manageWorkers)
    mwthread.daemon = True
    mwthread.start()

    try:
        ssock: socket.socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM
        )
        ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ssock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        ssock.bind(HBaddr)
        ssock.listen(128)
        socketThread: threading.Thread = threading.Thread(
            target=serverThread, args=(ssock,)
        )
        socketThread.daemon = True
        socketThread.start()
    except Exception as ex:
        print("Unable to start heartbeat server: %s" % str(ex))
        exit(-2)

    myState = "U"
    csock: Optional[socket.socket] = None

    checkForServer()

    t: Optional[threading.Timer] = None
    while True:
        if (time.gmtime().tm_sec % 10) == 0:
            checkForServer()
        time.sleep(1.0)
        while csock is None:
            try:
                csock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                csock.connect((remoteip, 53281))
            except Exception as ex:
                if DEBUG:
                    print("Unable to connect to remote server: %s" % str(ex))
                time.sleep(1.0)
                del csock
                csock = None
                continue
        if t is not None:
            t.cancel()
            t = None
        try:
            csock.sendall(b"g")
        except Exception as ex:
            if DEBUG:
                print("Failed to send client data: %s" % str(ex))
            _close_socket(csock)
            csock = None
            continue
        cnt: bytes = b""
        if t is None:
            t = threading.Timer(TDELAY, countdown)
            t.start()
        try:
            cnt = csock.recv(1)
        except Exception as ex:
            if DEBUG:
                print("Failed to get client data: %s" % str(ex))
            _close_socket(csock)
            csock = None
            continue
        if not cnt:
            if DEBUG:
                print("Client connection was closed.")
            _close_socket(csock)
            csock = None
            continue
        t.cancel()
        t = None
        if DEBUG:
            print("Received %s" % str(cnt))
        rem_state: str = cnt.decode()
        if (rem_state == "P" and myState == "P") or (
            rem_state != "P" and myState != "P"
        ):
            csock.sendall(b"r")
            if DEBUG:
                print("Initiating re-election")
            remrand: int = ord(csock.recv(1))
            myrand: int = int(random() * 10)
            if DEBUG:
                print("MyRand = %d, Received %d" % (myrand, remrand))
            if myrand >= remrand:
                if DEBUG:
                    print("Elected Primary")
                myState = "P"
                csock.sendall(b"S")
            else:
                if DEBUG:
                    print("Elected Secondary")
                myState = "S"
                csock.sendall(b"P")
