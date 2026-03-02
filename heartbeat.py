#!/usr/bin/env python3
"""
heartbeat.py — Primary/Secondary election daemon via TCP heartbeat.

Two nodes run this service and connect to each other. Each node maintains a
state (P = Primary, S = Secondary, U = Unknown, D = Disconnected) and runs a
re-election when both nodes are in the same state (both P or both non-P).

Usage:
    heartbeat.py <my_ip> <remote_ip>

Protocol (single-byte commands):
    b"g"  → peer queries our state; we reply with our state byte
    b"r"  → re-election request; we reply with a random byte 0–9
    b"P"  → peer informs us they are Primary; we set our state to P
    b"S"  → peer informs us they are Secondary; we set our state to S
"""

import argparse
import logging
import queue
import signal
import socket
import subprocess
import threading
import time
from random import random
from sys import argv, exit
from types import FrameType
from typing import Optional

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

myState: str = "D"
workerQueue: queue.Queue[threading.Thread] = queue.Queue()
TDELAY: float = 5.0

# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------


def sigint_handler(sig: int, frame: Optional[FrameType]) -> None:
    """Stub handler — add any cleanup here before exit."""
    logging.info("*** Signal caught, exiting. ***")
    exit(0)


# ---------------------------------------------------------------------------
# Process check
# ---------------------------------------------------------------------------


def checkForServer(processname: str = "my_server.py") -> None:
    """Demotes to Secondary if the managed server process is not running.

    NOTE: This function only demotes — it never promotes. Re-election via
    the heartbeat protocol is responsible for promotion back to Primary.
    """
    global myState
    logging.debug(f"Checking for {processname} process")
    try:
        result = subprocess.run(
            ["/bin/ps", "-C", processname, "--no-heading"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        running = bool(result.stdout.strip())
    except Exception as ex:
        logging.debug(f"Unable to check for server process: {ex}")
        myState = "S"
        return
    if not running:
        logging.debug(f"{processname} not found — demoting to Secondary")
        myState = "S"


# ---------------------------------------------------------------------------
# Countdown timer callback
# ---------------------------------------------------------------------------


def countdown() -> None:
    """Fired when TDELAY seconds pass with no heartbeat response.
    Assumes the remote is gone and claims Primary."""
    global myState
    logging.info("Countdown reached — no heartbeat response, claiming Primary")
    myState = "P"


# ---------------------------------------------------------------------------
# Worker thread reaper
# ---------------------------------------------------------------------------


def manageWorkers() -> None:
    """Periodically reaps finished client handler threads."""
    while True:
        time.sleep(10)
        # queue.Queue is already thread-safe; no external lock needed.
        live: list[threading.Thread] = []
        while not workerQueue.empty():
            try:
                worker: threading.Thread = workerQueue.get_nowait()
            except queue.Empty:
                break
            if worker.is_alive():
                live.append(worker)
            else:
                worker.join(0.1)
                logging.debug("*** Removed worker ***")
        for worker in live:
            workerQueue.put(worker)


# ---------------------------------------------------------------------------
# Socket helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Server-side threads
# ---------------------------------------------------------------------------


def clientThread(client_sock: socket.socket) -> None:
    """Handles a single accepted client connection.

    Runs until the client disconnects or sends an unknown command.
    Returns normally (never calls exit) so the thread dies cleanly.
    """
    global myState
    try:
        while True:
            try:
                cnt: bytes = client_sock.recv(1)
            except Exception as ex:
                logging.debug(f"Error receiving in clientThread: {ex}")
                return

            if not cnt:
                logging.debug("Client disconnected cleanly")
                return

            logging.debug(f"Thread received {cnt!r}")

            if cnt == b"g":
                try:
                    client_sock.sendall(myState.encode())
                    logging.debug(f"Sent state: {myState}")
                except Exception as ex:
                    logging.debug(f"Error sending state: {ex}")
                    return

            elif cnt == b"r":
                try:
                    client_sock.sendall(bytes([int(random() * 10)]))
                except Exception as ex:
                    logging.debug(f"Error sending re-election byte: {ex}")
                    return

            elif cnt in (b"P", b"S"):
                myState = cnt.decode()

            else:
                logging.debug(f"Unknown command {cnt!r} — closing connection")
                return
    finally:
        _close_socket(client_sock)


def serverThread(ssock: socket.socket) -> None:
    """Accepts incoming connections and dispatches each to a clientThread."""
    while True:
        try:
            client_sock, _sockname = ssock.accept()
        except Exception as ex:
            logging.debug(f"Accept failed: {ex}")
            return
        client_sock.settimeout(5.0)
        client_sock.setblocking(True)
        worker = threading.Thread(target=clientThread, args=(client_sock,))
        worker.daemon = True
        worker.start()
        workerQueue.put(worker)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGHUP, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)

    parser = argparse.ArgumentParser(prog="heartbeat")
    parser.add_argument(
        "-m",
        "--my-ip",
        help="The IP to use for the heartbeat service on this system",
        required=True,
    )
    parser.add_argument(
        "-r",
        "--remote-ip",
        help="The IP of the heartbeat service on the remote system",
        required=True,
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="Enable debug mode [Default=no]",
        required=False,
        action="store_true",
        default=False,
    )
    args = parser.parse_args()
    myip: str = args["my-ip"]
    remoteip: str = args["remote-ip"]
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:\t%(message)s",
        level=logging.INFO if not args["debug"] else logging.DEBUG,
    )

    HBaddr: tuple[str, int] = (myip, 53281)

    # Start worker reaper.
    mwthread = threading.Thread(target=manageWorkers, daemon=True)
    mwthread.start()

    # Start the local heartbeat listener.
    try:
        ssock: socket.socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM
        )
        ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ssock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        ssock.bind(HBaddr)
        ssock.listen(128)
        socketThread = threading.Thread(
            target=serverThread, args=(ssock,), daemon=True
        )
        socketThread.start()
    except Exception as ex:
        logging.critical(f"Unable to start heartbeat server: {ex}")
        exit(-2)

    myState = "U"
    csock: Optional[socket.socket] = None

    checkForServer()

    t: Optional[threading.Timer] = None

    while True:
        # Periodically re-check that our managed process is still alive.
        if (time.gmtime().tm_sec % 10) == 0:
            checkForServer()
        time.sleep(1.0)

        # Ensure we have a live connection to the remote heartbeat peer.
        while csock is None:
            try:
                csock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                csock.connect((remoteip, 53281))
            except Exception as ex:
                logging.error(f"Unable to connect to remote: {ex}")
                _close_socket(csock)  # Close the fd before discarding.
                csock = None
                time.sleep(1.0)

        # Cancel any outstanding countdown — we're about to send a fresh ping.
        if t is not None:
            t.cancel()
            t = None

        # Send heartbeat query.
        try:
            csock.sendall(b"g")
        except Exception as ex:
            logging.info(f"Failed to send heartbeat: {ex}")
            _close_socket(csock)
            csock = None
            continue

        # Start the failover timer — if no reply arrives within TDELAY seconds,
        # countdown() will claim Primary.
        t = threading.Timer(TDELAY, countdown)
        t.start()

        # Wait for the peer's state reply.
        try:
            cnt = csock.recv(1)
        except Exception as ex:
            logging.info(f"Failed to receive heartbeat reply: {ex}")
            _close_socket(csock)
            csock = None
            continue

        if not cnt:
            logging.info("Heartbeat connection closed by remote")
            _close_socket(csock)
            csock = None
            continue

        # Got a reply — cancel the failover timer.
        t.cancel()
        t = None

        rem_state: str = cnt.decode()
        logging.debug("Remote state: {rem_state}, My state: {myState}")

        # Re-election needed when both nodes agree on no clear Primary, or
        # when both claim Primary (split-brain).
        both_primary = rem_state == "P" and myState == "P"
        neither_primary = rem_state != "P" and myState != "P"
        if both_primary or neither_primary:
            logging.info(
                f"Re-election triggered (both={both_primary}, neither={neither_primary}"
            )
            try:
                csock.sendall(b"r")
                remrand: int = ord(csock.recv(1))
                myrand: int = int(random() * 10)
                logging.debug(f"MyRand={myrand}, RemoteRand={remrand}")
                if myrand >= remrand:
                    logging.info("Elected Primary")
                    myState = "P"
                    csock.sendall(b"S")
                else:
                    logging.info("Elected Secondary")
                    myState = "S"
                    csock.sendall(b"P")
            except Exception as ex:
                logging.error(f"Re-election failed: {ex}")
                _close_socket(csock)
                csock = None
