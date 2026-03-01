#!/usr/bin/env python3
"""
quova.py — A Python 3 emulator for the Quova GeoIP protocol.

Listens on a TCP port, decodes Quova-format IP lookup requests,
and responds with GeoIP data using the MaxMind GeoLiteCity database.
"""

import logging
import socket
import sys
import time
from threading import Thread
from typing import Final, TypedDict

import daemon  # https://pypi.org/project/python-daemon/
import pygeoip  # https://github.com/appliedsec/pygeoip

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WORKER_MAX: Final[int] = 256  # Max concurrent threads
DEAD_TIME: Final[float] = 1.0  # Seconds between dead-thread reaping
MAX_BUF: Final[int] = 1024  # Per-thread receive buffer size
HOST: Final[str] = ""  # Empty = all interfaces; set to a specific IP if needed
PORT: Final[int] = 12345  # TCP port to listen on

# MaxMind GeoLiteCity database path — updated the first Tuesday of each month.
# Download: https://www.miyuru.lk/geoiplegacy
GEOIPDB: Final[str] = "/root/geolocate/GeoLiteCity.dat"

SYSLOGID: Final[str] = "pyquova"

# This IP resolves to roughly the geographic centre of the contiguous US
# (Lat 39°50' N, Long 98°35' W) and is used as a safe fallback.
DEFAULT_IP: Final[str] = "129.130.8.50"

# ---------------------------------------------------------------------------
# GeoIP record type
# ---------------------------------------------------------------------------


class GeoIPRecord(TypedDict, total=False):
    """Shape of a pygeoip record dict, as returned by record_by_addr()."""

    country_name: str
    region_name: str
    city: str
    country_code: str
    dma_code: str | int
    area_code: str | int
    postal_code: str
    metro_code: str | int
    latitude: str | float
    longitude: str | float


# ---------------------------------------------------------------------------
# Logging setup  (replaces direct syslog calls for testability)
# ---------------------------------------------------------------------------

log: logging.Logger = logging.getLogger(SYSLOGID)


def _configure_logging(use_syslog: bool = True) -> None:
    """Attach a SysLogHandler (production) or StreamHandler (dev/tests)."""
    log.setLevel(logging.DEBUG)
    handler: logging.Handler
    if use_syslog:
        from logging.handlers import SysLogHandler

        handler = SysLogHandler(address="/dev/log")
    else:
        handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(name)s: %(levelname)s %(message)s")
    )
    log.addHandler(handler)


# ---------------------------------------------------------------------------
# Global metrics counter
# ---------------------------------------------------------------------------

statcounter: int = 0

# ---------------------------------------------------------------------------
# Packet constants
# ---------------------------------------------------------------------------

_HEADER: Final[bytes] = (
    b"\x00" * 16
    + b"\x01"  # 1 record returned
    + b"\x00" * 7
    + b"\x0c"  # 12 fields per record
    + b"\x00" * 7
    + b"\x06Mapped"  # field 0x06 = "Mapped"
    + b"\xff\xff\xff\xff"
    + b"\x00\x00\x00\x0e\x00\x00\x00"
)

_SEP: Final[bytes] = b"\xff\xff\xff\xff\x00\x00\x00"

_ERROR_RESPONSE: Final[bytes] = b"\x00" * 32  # 0 records, 0 fields

# Each entry is (quova_field_id_byte, GeoIPRecord key).
_FIELD_MAP: Final[list[tuple[bytes, str]]] = [
    (b"\x06", "country_name"),
    (b"\x07", "region_name"),
    (b"\x08", "city"),
    (b"\x03", "country_code"),
    (b"\x0a", "dma_code"),
    (b"\x1b", "area_code"),
    (b"\x0f", "postal_code"),
    (b"\x04", "metro_code"),
    (b"\x08", "latitude"),
    (b"\x09", "longitude"),
]

# ---------------------------------------------------------------------------
# Packet helpers
# ---------------------------------------------------------------------------


def _append_field(buf: bytearray, field_id: bytes, value: str) -> bytearray:
    """Encode a single Quova response field and append it to *buf*."""
    encoded: bytes = value.lower().encode("utf-8", errors="replace")
    buf += _SEP + field_id + b"\x00\x00\x00"
    buf.append(len(encoded))
    buf += encoded
    return buf


def decode_quova(data: bytes) -> str:
    """
    Extract the IPv4 address from the last 4 bytes of a Quova request packet.

    Returns a dotted-decimal string, or DEFAULT_IP on any error.
    """
    if len(data) < 4:
        log.warning(
            "Packet too short to contain an IP address (%d bytes)", len(data)
        )
        return DEFAULT_IP
    try:
        return "%d.%d.%d.%d" % (data[-4], data[-3], data[-2], data[-1])
    except Exception:
        log.exception("Unable to decode IP from packet: %r", data)
        return DEFAULT_IP


def encode_quova(ipinfo: GeoIPRecord, ip: str) -> bytes:
    """
    Build a Quova-format response packet from a pygeoip record dict.

    Falls back to _ERROR_RESPONSE if encoding fails.
    """
    global statcounter
    try:
        buf: bytearray = bytearray(_HEADER)

        for field_id, key in _FIELD_MAP:
            buf = _append_field(buf, field_id, str(ipinfo.get(key, "")))  # type: ignore[arg-type]

        # Terminator
        buf += _SEP + b"\x02\x00\x00\x00\x01\x30\xff\xff\xff\xff"

        statcounter += 1
        return bytes(buf)

    except Exception:
        log.exception(
            "Error generating Quova packet for IP %s; ipinfo=%s", ip, ipinfo
        )
        return _ERROR_RESPONSE


# ---------------------------------------------------------------------------
# Connection handler
# ---------------------------------------------------------------------------


def _fallback_record(geoip: pygeoip.GeoIP) -> GeoIPRecord:
    """Return the GeoIP record for DEFAULT_IP as a last-resort fallback."""
    record: GeoIPRecord | None = geoip.record_by_addr(DEFAULT_IP)
    return record if record is not None else GeoIPRecord()


def _close_socket(sock: socket.socket, label: str = "") -> None:
    """Attempt a graceful shutdown and close of *sock*, logging any errors."""
    try:
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()
    except OSError as exc:
        log.debug("Close socket%s: %s", f" ({label})" if label else "", exc)


def process_connection(client_sock: socket.socket) -> None:
    """Handle a single client connection in its own thread."""
    geoip: pygeoip.GeoIP
    try:
        geoip = pygeoip.GeoIP(GEOIPDB)
    except Exception:
        log.exception("Unable to open GeoIP database '%s'", GEOIPDB)
        _close_socket(client_sock, "db-open-fail")
        return

    try:
        while True:
            chunk: bytes
            try:
                chunk = client_sock.recv(MAX_BUF)
            except OSError:
                log.exception("Error reading from client socket")
                return

            if not chunk:
                log.debug("Client closed connection (empty read).")
                return

            # Stats command — send counter and close.
            if len(chunk) > 4 and chunk[:5] == b"stats":
                try:
                    client_sock.sendall(f"{statcounter}\n".encode())
                    log.info("Sent statistics: %d queries.", statcounter)
                except OSError:
                    log.exception("Error sending stats response")
                return

            ip: str = decode_quova(chunk)

            ipinfo: GeoIPRecord | None
            try:
                ipinfo = geoip.record_by_addr(ip)
            except Exception:
                log.exception("Error looking up IP %s", ip)
                ipinfo = None

            if ipinfo is None:
                log.debug("No GeoIP record for %s; using default.", ip)
                ipinfo = _fallback_record(geoip)

            try:
                client_sock.sendall(encode_quova(ipinfo, ip))
            except OSError:
                log.exception("Error sending GeoIP response for IP %s", ip)
                return
    finally:
        _close_socket(client_sock, "connection-done")


# ---------------------------------------------------------------------------
# Server / worker pool
# ---------------------------------------------------------------------------


def server_loop(listen_sock: socket.socket) -> None:
    """Accept one connection and hand it off to process_connection."""
    client_sock: socket.socket
    client_sock, _ = listen_sock.accept()
    process_connection(client_sock)


def start_worker(listen_sock: socket.socket) -> Thread:
    """Spawn and return a new daemon thread running server_loop."""
    worker: Thread = Thread(
        target=server_loop, args=(listen_sock,), daemon=True
    )
    worker.start()
    return worker


def sock_setup() -> socket.socket:
    """Create, bind, and return a listening TCP socket."""
    sock: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((HOST, PORT))
    sock.listen(512)
    return sock


def run(daemonize: bool = True) -> None:
    """Entry point — optionally daemonize, then run the worker pool."""
    _configure_logging(use_syslog=daemonize)

    def _main() -> None:
        listen_sock: socket.socket = sock_setup()
        workers: list[Thread] = [
            start_worker(listen_sock) for _ in range(WORKER_MAX)
        ]
        log.info("Started the Python Quova emulator on port %d.", PORT)

        while True:
            time.sleep(DEAD_TIME)
            dead: list[Thread] = [w for w in workers if not w.is_alive()]
            for w in dead:
                workers.remove(w)
                workers.append(start_worker(listen_sock))

    if daemonize:
        with daemon.DaemonContext():
            _main()
    else:
        _main()


if __name__ == "__main__":
    run(daemonize=True)
