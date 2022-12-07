#!/usr/local/bin/pypy -O

# Change the interpreter above to your Python interpreter.

import socket
import syslog
import time

# from multiprocessing import Process as Worker
from threading import Thread as Worker

import daemon  # http://pypi.python.org/pypi/python-daemon/
import pygeoip  # https://github.com/appliedsec/pygeoip

WORKER_MAX = 256  # How many threads/processes to have.
DEAD_TIME = 1  # How long in seconds to wait for dead thread removal.
MAX_BUF = 1024  # How large a buffer for each thread's receive stream.

HOST = ""  # Empty is all interfaces, otherwise specify an IP to listen on
PORT = 12345  # What TCP port to listen to for new connections.

# The MaxMind GeoLiteCity database full path and filename
# http://geolite.maxmind.com/download/geoip/database/GeoLiteCity.dat.gz
# Updated the first Tuesday of each month.
GEOIPDB = "/root/geolocate/GeoLiteCity.dat"

# The identifier that will be used for this process in syslog.
SYSLOGID = "pyquova"

# This IP resolves to very close to the center of the 48 contiguous states.
DEFAULT_IP = "129.130.8.50"

############################################################################
# You shouldn't need to change anything below this line.                   #
############################################################################

# A counter to keep track of the number of lookups performed.
# The pointy-hairs like metrics. ^_^
statcounter = 0


def uni_to_ba(ba, data):
    try:
        newba = ba
        data = str(data)
        for i in data:
            newba.append(ord(i))
        return newba
    except Exception as ex:
        syslog.syslog("ERROR appending unicode to bytearray: %s" % str(ex))
        return ba


def decode_quova(data):
    try:
        IP = data[-4:]
        octets = "%d.%d.%d.%d" % (IP[0], IP[1], IP[2], IP[3])
        return octets
    except Exception as ex:
        syslog.syslog("Unable to decode passed in IP %s: %s" % (str(data), str(ex)))
        return DEFAULT_IP


def encode_quova(ipinfo, IP):
    # Create a spoofed Quova packet.
    # Header:
    #    16 x \x0
    #    Number of records returned in the response (always \x01 here)
    #    7 x \x0
    #    Number of fields in the record, always \x0c (12) here.
    #    7 x \x0
    #    Start of returned data.
    # Returned data:
    #    \x## The code for the data returned in this field.
    #    3 x \x0
    #    \x## The number of bytes in the field response
    #    The data for the response.
    #    Either 4 x \xff OR
    #     3 x \x00 followed by \x##.
    global statcounter
    try:
        # A static header to speed up response generation.
        resba = bytearray(
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x06Mapped\xff\xff\xff\xff\x00\x00\x00\x0e\x00\x00\x00"
        )
        sep = bytearray(b"\xff\xff\xff\xff\x00\x00\x00")
        resba.append(len(str(ipinfo["country_name"])))
        resba = uni_to_ba(resba, str(ipinfo["country_name"]).lower())
        resba += sep + b"\x06\x00\x00\x00"
        resba.append(len(str(ipinfo["region_name"])))
        resba += str(ipinfo["region_name"]).lower()
        resba += sep + b"\x07\x00\x00\x00"
        resba.append(len(str(ipinfo["city"])))
        resba = uni_to_ba(resba, str(ipinfo["city"]).lower())
        resba += sep + b"\x03\x00\x00\x00"
        resba.append(len(str(ipinfo["country_code"])))
        resba = uni_to_ba(resba, str(ipinfo["country_code"]).lower())
        resba += sep + b"\x0a\x00\x00\x00"
        resba.append(len(str(ipinfo["dma_code"])))
        resba += str(ipinfo["dma_code"]).lower()
        resba += sep + b"\x1b\x00\x00\x00"
        resba.append(len(str(ipinfo["area_code"])))
        resba += str(ipinfo["area_code"]).lower()
        resba += sep + b"\x0f\x00\x00\x00"
        resba.append(len(str(ipinfo["postal_code"])))
        resba += str(ipinfo["postal_code"]).lower()
        resba += sep + b"\x04\x00\x00\x00"
        resba.append(len(str(ipinfo["metro_code"])))
        resba += str(ipinfo["metro_code"]).lower()
        resba += sep + b"\x08\x00\x00\x00"
        resba.append(len(str(ipinfo["latitude"])))
        resba += str(ipinfo["latitude"]).lower()
        resba += sep + b"\x09\x00\x00\x00"
        resba.append(len(str(ipinfo["longitude"])))
        resba += str(ipinfo["longitude"]).lower()
        # Finalize the response.
        resba += sep + b"\x02\x00\x00\x00\x01\x30\xff\xff\xff\xff"
    except Exception as ex:
        syslog.syslog(
            "Error generating Quova packet for IP %s: %s. GeoIP info was %s"
            % (str(IP), str(ex), str(ipinfo))
        )
        # This should mean 0 records of 0 fields each.
        resba = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"

    statcounter += 1
    return resba


def process_connection(client_sock):
    global statcounter
    try:
        GEOIP = pygeoip.GeoIP(GEOIPDB)
    except Exception as ex:
        syslog.syslog("Unable to open the GeoIP database: %s" % str(ex))
        try:
            client_sock.shutdown(socket.SHUT_RDWR)
            client_sock.close()
        except Exception as ex:
            syslog.syslog("Unable to close socket 1: %s" % str(ex))
        finally:
            return
    while True:  # The thread will go away when the connection closes.
        try:
            data = bytearray()
            chunk = client_sock.recv(MAX_BUF)
            data.extend(chunk)
        except Exception as ex:
            try:
                client_sock.shutdown(socket.SHUT_RDWR)
                client_sock.close()
            except Exception as ex:
                syslog.syslog("Unable to close socket 2: %s" % str(ex))
            finally:
                syslog.syslog("Unable to read client socket: %s" % str(ex))
                return
        try:
            if data is None:
                syslog.syslog("No data received. Closing socket.")
                try:
                    client_sock.shutdown(socket.SHUT_RDWR)
                    client_sock.close()
                except Exception as ex:
                    syslog.syslog("Unable to close socket 3: %s" % str(ex))
                return
        except Exception as ex:
            syslog.syslog("Unable to check for None buffer: %s" % str(ex))

        try:
            if len(str(chunk)) > 4 and str(chunk[0:4]) == "stats":
                client_sock.sendall("%d\n" % statcounter)
                syslog.syslog("Sent statistics. %u queries." % statcounter)
                client_sock.shutdown(socket.SHUT_RDWR)
                client_sock.close()
                return
        except Exception as ex:
            syslog.syslog("Unable to check for stats command: %s" % str(ex))
        IP = decode_quova(data)
        try:
            ipinfo = GEOIP.record_by_addr(IP)
        except Exception as ex:
            ipinfo = GEOIP.record_by_addr(DEFAULT_IP)
            syslog.syslog("Error looking up IP %s: %s" % (str(IP), str(ex)))
        if ipinfo is None:
            # The GEOGRAPHIC CENTER of the UNITED STATES
            # LAT. 39°50' LONG. -98°35'
            ipinfo = GEOIP.record_by_addr(DEFAULT_IP)
        try:
            client_sock.sendall(encode_quova(ipinfo, IP))
        except Exception as ex:
            try:
                client_sock.shutdown(socket.SHUT_RDWR)
                client_sock.close()
            except Exception as ex:
                syslog.syslog("Unable to close socket 4: %s" % str(ex))
            finally:
                syslog.syslog(
                    "Unable to send GeoIP response for IP %s: %s" % (str(IP), str(ex))
                )
                return
    return


def server_loop(listen_sock):
    client_sock, sockname = listen_sock.accept()
    process_connection(client_sock)
    return


def start_worker(Worker, listen_sock):
    worker = Worker(target=server_loop, args=(listen_sock,))
    worker.daemon = True
    worker.start()
    return worker


def sock_setup():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((HOST, PORT))
    sock.listen(512)  # Number of connections to pend. Shouldn't be important.
    return sock


# Detach from the PTY and run as a daemon.
# Comment the daemon line and uncomment the while line to not daemonize.
with daemon.DaemonContext():
    # while True:
    # Open a syslog connection to set the app ID.
    syslog.openlog(SYSLOGID)
    listen_sock = sock_setup()

    # Spin up all the threads/processes.
    workers = []
    for i in range(WORKER_MAX):
        workers.append(start_worker(Worker, listen_sock))
    syslog.syslog("Started the Python Quova emulator.")

    # Check every DEAD_TIME seconds for dead workers and replace them.
    while True:
        time.sleep(DEAD_TIME)
        for worker in workers:
            if not worker.is_alive():
                workers.remove(worker)
                # syslog.syslog("Started new worker.")
                workers.append(start_worker(Worker, listen_sock))
