#!/usr/bin/python3 -uO

import argparse
import configparser
import logging
import os
import queue
import resource
import signal
import socket
import stat
import threading
import unittest
from os.path import exists
from platform import node
from random import randint
from sys import exit as sysexit
from sys import stderr
from time import gmtime, sleep
from typing import Tuple

myState: str = "U"  # U = unknown
base_dir: str = "/tmp"  # Change this to be where you want the files
configfile: str = f"{base_dir}/heartbeat.conf"
pidfile: str = f"{base_dir}/heartbeat.pid"
statusfile: str = f"{base_dir}/heartbeat.status"
statefile: str = f"{base_dir}/heartbeat.state"
PORT: int = 49152
workerQueue: queue.Queue = queue.Queue()
workerLock: threading.Lock = threading.Lock()
logLock: threading.Lock = threading.Lock()
stateLock: threading.Lock = threading.Lock()
palive: bool = True
count: int = 0


def Daemonize() -> int:
    """Daemonize a process in Python 2.4+"""

    pid = os.fork()
    if pid == 0:  # Are we the child?
        os.setsid()  # Create a new session
        signal.signal(signal.SIGHUP, signal.SIG_IGN)  # Ignore SIGHUP
        pid = os.fork()  # Fork again
        if pid == 0:  # Are we the new child?
            os.umask(0)  # Clear any UMASK flags that were set.
        else:  # Exit the first child.
            os._exit(0)
    else:  # Exit the parent.
        os._exit(0)

    # Ask the OS how many open FDs there can be.
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if maxfd == resource.RLIM_INFINITY:
        maxfd = 1024  # But use a sane value if the OS is confused.

    # Close all open file descriptors.
    for fd in range(0, maxfd):
        try:
            os.close(fd)
        except OSError:  # Any unopened fd's will error.
            pass

    os.open(os.devnull, os.O_RDWR)  # Redirect stdin to /dev/null
    os.dup2(0, 1)  # Redirect stdout to /dev/null
    os.dup2(0, 2)  # Redirect stderr to /dev/null
    return os.getpid()


def sigint_handler(a, b):
    logging.info("\n\n*** Signal caught, exiting. ***\n\n")
    if exists(statusfile):
        try:
            os.remove(statusfile)
        except OSError as ex:
            logging.error(f"Couldn't remove status file: {ex}")
    if exists(pidfile):
        try:
            os.remove(pidfile)
        except OSError as ex:
            logging.error(f"Couldn't remove pid file: {ex}")
    sysexit(0)


def checkForServer(processname: str, caddr: Tuple) -> bool:
    logging.debug("Entering checkForServer")
    assert isinstance(processname, str)
    assert len(processname) > 0
    assert isinstance(caddr, tuple)
    assert len(caddr) == 2
    global myState
    global palive
    global stateLock
    logging.debug(f"Checking for running {processname} process")
    try:
        myserver = os.popen("/bin/ps -A -o command 2>/dev/null")
        procs = myserver.readlines()
        myserver.close()
        srvcnt: int = 0
        for line in procs:
            line = line.strip().split()[0]
            logging.debug(f"Checking if process {line} matches {processname}")
            if line == processname:
                srvcnt += 1
        logging.info(f"There are {srvcnt} running {processname} processes.")
        del myserver
        del procs
        if "line" in locals():
            del line
    except Exception as ex:
        logging.error(
            f"Unable to check for running process {processname}: {ex}"
        )
        stateLock.acquire()
        myState = "S"
        palive = False
        stateLock.release()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(b"P", caddr)
            sock.close()
            del sock
        except Exception as ex:
            logging.error(f"Error sending forcedPrimary1: {ex}")
            sleep(1.0)
            return False
        return False

    if srvcnt == 0:  # If no process running, demote to Secondary.
        logging.info(
            f"No process {processname} running, making myself secondary."
        )
        stateLock.acquire()
        myState = "S"
        palive = False
        stateLock.release()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(b"P", caddr)
            sock.close()
            del sock
        except Exception as ex:
            logging.error(f"Error sending forcedPrimary2: {ex}")
            return False
    else:
        palive = True
    del srvcnt
    return True


class HB_Tests(unittest.TestCase):
    def setUp(self):
        global myState
        global palive
        myState = "P"
        palive = True
        self.ssock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ssock.settimeout(
            6.0
        )  # Abort recvfrom after 6 seconds and start again.
        self.ssock.bind(("127.0.0.1", 53281))
        return

    def tearDown(self):
        self.ssock.close()
        return

    def test_process(self):
        self.assertTrue(checkForServer("heartbeat.py", ("127.0.0.1", 53281)))
        return


def pingPong(processname: str, caddr: Tuple):
    logging.debug("Entering pingPong")
    assert isinstance(processname, str)
    assert len(processname) > 0
    assert isinstance(caddr, tuple)
    assert len(caddr) == 2
    global count
    global myState
    global stateLock

    # Generate a random value between 1 and 59 seconds
    offset = 0
    while offset % 5 == 0:
        offset = randint(1, 59)
    logging.debug(f"Will request state at {offset} seconds")

    randsleep = float(randint(1, 99)) / 100.0

    while True:
        while gmtime().tm_sec % 5 != 0:
            sleep(0.7)
            if (
                gmtime().tm_sec == offset
            ):  # Once a minute, try to get the state of the other server
                logging.info("Requesting state of other node")
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.sendto(b"G", caddr)
                    sock.close()
                    del sock
                    sleep(1)
                except Exception as ex:
                    logging.error(f"Error requesting state: {ex}")
        if gmtime().tm_sec % 10 == 0:
            checkForServer(processname, caddr)
        try:
            sleep(
                randsleep
            )  # So both HB nodes don't ping pong at the exact same moment
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(b"A", caddr)
            sock.close()
            del sock
            stateLock.acquire()
            count += 1
            stateLock.release()
        except Exception as ex:
            logging.error(f"Error sending ping: {ex}")

        if count >= 6:
            # We haven't heard from the other server in at least 30 seconds.
            stateLock.acquire()
            count = 0
            stateLock.release()
            if myState != "N":
                stateLock.acquire()
                myState = "N"
                stateLock.release()
                logging.error(
                    "No communication for 30 seconds. Forcing myself primary"
                )
        sleep(1)


def updateFile():
    logging.debug("Entering updateFile")
    fstate = ""
    global stateLock
    global myState

    while True:
        while gmtime().tm_sec != 58:
            sleep(0.9)
        stateLock.acquire()
        if fstate != myState:
            try:
                sfile = open(statefile, "wt")
                sfile.write(myState)
                sfile.flush()
                sfile.close()
                os.chmod(
                    statefile,
                    stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH,
                )
                fstate = myState
            except Exception as ex:
                logging.error(f"Coudn't write Heartbeat state: {ex}")
        stateLock.release()
        sleep(2)


def heartbeat():
    logging.debug("Entering heartbeat")
    global myState
    global count
    global stateLock

    logging.info("Starting background status change notifier.")
    fthread = threading.Thread(target=updateFile)
    fthread.daemon = True
    fthread.start()

    stateLock.acquire()
    myState = "U"
    try:
        sfile = open(statefile, "wt")
        sfile.write("U")
        sfile.flush()
        sfile.close()
        os.chmod(
            statefile,
            stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH,
        )
    except Exception as ex:
        logging.error(f"Coudn't write Initial Heartbeat state: {ex}")
    stateLock.release()

    try:
        logging.info("Creating server socket for listening.")
        ssock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ssock.settimeout(
            6.0
        )  # Abort recvfrom after 6 seconds and start again.
        ssock.bind(saddr)
    except Exception as ex:
        logging.error(
            f"Unable to open server socket for listening on {saddr}: {ex}"
        )
        sysexit(-1)

    ppthread = threading.Thread(
        target=pingPong, args=(args.process_name, caddr)
    )
    ppthread.daemon = True
    ppthread.start()

    # Figure out if we should be primary or secondary
    logging.info("Checking state of other node.")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(b"G", caddr)
        sock.close()
        del sock
    except Exception as ex:
        logging.error(
            f"Unable to check other node. Forcing myself Primary: {ex}"
        )
        stateLock.acquire()
        myState = "N"
        stateLock.release()

    while True:
        # Check for force primary
        if os.path.isfile(f"{base_dir}/primary"):
            try:
                os.remove(f"{base_dir}/primary")
                stateLock.acquire()
                myState = "P"
                stateLock.release()
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(b"S", caddr)
                sock.close()
                del sock
            except Exception as ex:
                logging.error(f"Unable to force primary: {ex}")
                sleep(1.0)
            continue

        # Check for force secondary
        if os.path.isfile(f"{base_dir}/secondary"):
            try:
                os.remove(f"{base_dir}/secondary")
                stateLock.acquire()
                myState = "S"
                stateLock.release()
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(b"P", caddr)
                sock.close()
                del sock
            except Exception as ex:
                logging.error(f"Unable to force secondary: {ex}")
                sleep(1.0)
            continue

        try:
            data, addr = ssock.recvfrom(1)
            if "addr" in locals():
                del addr
            data = data.decode("utf-8")
        except Exception as ex:
            logging.error(f"Failed to receive heartbeat data: {ex}")
            continue

        logging.debug(f"Received heartbeat {data}")

        if data == "B":  # We received the pong.
            stateLock.acquire()
            count = 0
            stateLock.release()
            continue

        stateLock.acquire()
        now = myState
        stateLock.release()

        # Send a pong if we received a ping
        if data == "A":
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(b"B", caddr)
                sock.close()
                del sock
                continue
            except Exception as ex:
                logging.error(f"Failed to send HB pong: {ex}")
                continue

        if data == "P":
            if now != "P":
                stateLock.acquire()
                myState = "P"
                stateLock.release()

        elif data == "S":
            if now != "S":
                stateLock.acquire()
                myState = "S"
                stateLock.release()

        elif data == "N":
            if now != "S":
                stateLock.acquire()
                myState = "S"
                stateLock.release()
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(b"P", caddr)
            sock.close()
            del sock

        elif data == "G":
            # If we received a G, we are no longer standalone
            if now == "N":
                stateLock.acquire()
                myState = "P"
                stateLock.release()

            # They want to know our state:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            if now == "P":
                sock.sendto(b"S", caddr)
            else:
                sock.sendto(b"P", caddr)
            sock.close()
            del sock

        if now != myState:
            logging.error(f"My status changed to {myState}")
        del now
        del data

        logging.info(f"My status = {myState}")
        sleep(1.0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGHUP, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)

    myConfig = configparser.ConfigParser()
    myConfig.read(configfile)
    myname = node().split(".")[0]
    if myConfig.has_option("hosts", myname):
        IP = myConfig.get("hosts", myname)
    else:
        IP = "127.0.0.1"
    if myConfig.has_option(myname, "proc"):
        proc = myConfig.get(myname, "proc")
    else:
        proc = "service.py"
    if myConfig.has_option(myname, "listen"):
        listen = myConfig.get(myname, "listen")
    else:
        listen = "0.0.0.0"
    if myConfig.has_option(myname, "port"):
        PORT = int(myConfig.get(myname, "port"))
    if myConfig.has_option(myname, "remoteip"):
        IP = myConfig.get(myname, "remoteip")

    parser = argparse.ArgumentParser(
        description="This application provides heartbeat functionality.",
        prog="heartbeat.py",
    )
    parser.add_argument(
        "-R",
        "--remote_ip",
        help=f"IP Address of remote heartbeat server [Default={IP}]",
        required=False,
        type=str,
        default=IP,
    )
    parser.add_argument(
        "-m",
        "--my_ip",
        help=f"IP Address to listen on [Default={listen}]",
        required=False,
        type=str,
        default=listen,
    )
    parser.add_argument(
        "-p",
        "--port",
        help=f"Port to communicate on [Default={PORT}]",
        required=False,
        type=int,
        default=PORT,
    )
    parser.add_argument(
        "-s",
        "--status",
        help="Return the status of this node",
        required=False,
        action="store_true",
    )
    parser.add_argument(
        "-P",
        "--force_primary",
        help="Force this node to be primary",
        required=False,
        action="store_true",
    )
    parser.add_argument(
        "-S",
        "--force_secondary",
        help="Force this node to be secondary",
        required=False,
        action="store_true",
    )
    parser.add_argument(
        "-n",
        "--process_name",
        help=f"Process name to key on for Primary [Default={proc}]",
        required=False,
        type=str,
        default=proc,
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="Enable debug mode (implies -f)",
        required=False,
        action="store_true",
    )
    parser.add_argument(
        "-f",
        "--foreground",
        help="Do not fork into the background [Default=background]",
        required=False,
        action="store_true",
    )
    parser.add_argument(
        "-k",
        "--kill",
        help="Kill running heartbeat daemon",
        required=False,
        action="store_true",
    )
    parser.add_argument(
        "-r",
        "--restart",
        help="Restart the running process",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-t",
        "--test",
        help="Run unit tests",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-w",
        "--write",
        help=f"Write config file {configfile}",
        required=False,
        default=False,
        action="store_true",
    )
    args = parser.parse_args()

    if args.test:
        unittest.main(verbosity=9, argv=["heartbeat.py"])

    logging.basicConfig(
        format="%(asctime)s %(levelname)s:\t%(message)s",
        level=logging.DEBUG if args.debug else logging.INFO,
    )
    if args.debug:
        logging.info("Activating DEBUG mode.")
        args.foreground = True

    if args.kill or args.restart:
        stderr.write("Killing backgrounded heartbeat daemon.\n")
        try:
            if os.path.isfile(f"{base_dir}/heartbeat.pid") is False:
                logging.error("No backgrounded heartbeat running.")
                if args.restart is False:
                    exit(0)
            else:
                with open(pidfile) as pf:
                    fpid = pf.readline()
                ppid = int(fpid.strip())
                os.kill(ppid, signal.SIGTERM)
                os.remove(pidfile)
                sleep(0.5)
                if args.kill is True:
                    exit(0)
        except Exception as ex:
            stderr.write(f"Unable to kill backgrounded heartbeat: {ex}\n")
            sysexit(-1)

    saddr = (args.my_ip, args.port)
    caddr = (args.remote_ip, args.port)
    if args.status:
        try:
            with open(statefile, "rt") as sfile:
                cnt = sfile.readline()
            print(cnt)
        except Exception as ex:
            logging.error(f"Error reading heartbeat: {ex}")
            print("U")
        exit(0)

    if args.write:
        cp = configparser.ConfigParser()
        cp[myname] = {}
        cp[myname]["remoteip"] = IP
        cp[myname]["listen"] = listen
        cp[myname]["port"] = str(PORT)
        cp[myname]["proc"] = args.process_name
        with open(configfile, "w") as cf:
            cp.write(cf)

    if args.force_primary is True and args.force_secondary is True:
        stderr.write(
            "You cannot force this node to be both primary and secondary.\n"
        )
        exit(0)

    if args.force_primary:
        logging.info("Will force this node to Primary status.")
        if gmtime().tm_sec % 10 == 0:
            sleep(1.0)
        try:
            if os.path.isfile(f"{base_dir}/secondary"):
                os.remove(f"{base_dir}/secondary")
            fp = open(f"{base_dir}/primary", "wt")
            fp.close()
            exit(0)
        except Exception as ex:
            logging.error(f"Unable to set node to Primary status: {ex}")
            exit(-1)

    if args.force_secondary:
        logging.info("Will force this node to Secondary status.")
        if gmtime().tm_sec % 10 == 0:
            sleep(1.0)
        try:
            if os.path.isfile(f"{base_dir}/primary"):
                os.remove(f"{base_dir}/primary")
            fs = open(f"{base_dir}/secondary", "wt")
            fs.close()
            exit(0)
        except Exception as ex:
            logging.error(f"Unable to set node to Secondary status: {ex}")
            exit(-1)

    if os.path.isfile(pidfile):
        stderr.write(
            "There is already a heartbeat running on this node. Exiting.\n"
        )
        exit(0)

    stderr.write("Starting new heartbeat daemon.\n")

    logging.info("Checking for running process")
    checkForServer(args.process_name, caddr)

    logging.info("Starting heartbeat loop.")

    if args.foreground:
        logging.info("Will not background.")
        heartbeat()
    else:
        pid = Daemonize()
        try:
            with open(pidfile, "wt") as p:
                p.write(str(pid))
        except Exception as ex:
            logging.info(f"Unable to create pidfile: {ex}")
            sysexit(-1)
        heartbeat()
