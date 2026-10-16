"""
Bidirectional TCP message queue with on-disk durability.

Server-side queues are stored in an on-disk SQLite database (WAL mode).
Every enqueue and dequeue is a transaction, so queue contents survive
process crashes, hard kills and OOM kills. With the default
``synchronous="FULL"`` they also survive power loss and OS crashes; see
*Durability* below.

Wire format
-----------

Each frame on the wire is::

    <ascii_length>:<opcode_byte><payload_bytes>[<hmac_bytes>]

``<ascii_length>`` is the total byte length of everything after the colon,
encoded as ASCII digits. ``<opcode_byte>`` is one byte identifying the
frame type (see ``_OP_*`` below). ``<payload_bytes>`` is the (optional)
message payload. ``<hmac_bytes>`` is a 32-byte HMAC-SHA256 over
``<opcode_byte><payload_bytes>``, present only when the queue was
constructed with a ``secret_key``.

Opcodes are byte values >= 0x10, which guarantees they cannot collide with
the first byte of a zlib stream (always 0x78) or with any printable ASCII
character.

Every request frame gets exactly one response frame, so the protocol is
strictly request/response and never pipelined by the client (the server
does tolerate back-to-back frames):

* ``_OP_GET_*`` [wait]              -> ``_OP_ITEM`` payload, or ``_OP_EMPTY``
* ``_OP_LEASE_*`` [wait]            -> ``_OP_LEASED`` id+token+payload, or
                                       ``_OP_EMPTY``
* ``_OP_PUT_PRODUCER`` payload      -> ``_OP_ACK`` (sent only after the row
                                       is committed), or ``_OP_FULL``
* ``_OP_ACK_ITEM`` table+id+token   -> ``_OP_ACK`` 0x01 (deleted) or 0x00
                                       (lease no longer valid)
* ``_OP_RELEASE_ITEM`` table+id+token -> ``_OP_ACK`` 0x01 or 0x00
* any request                       -> ``_OP_ERROR`` utf-8 reason

``[wait]`` is an optional big-endian IEEE-754 double: the number of
seconds the server may hold the request open waiting for an item to
arrive (long polling). An empty payload means "answer immediately".

Delivery semantics
------------------

There are two ways to consume:

``get_consumer()`` / ``get_producer()``
    At-most-once. The server deletes the row *before* writing the
    response, so if the response is lost (client crash, network failure)
    the item is gone. Cheap and simple; use it when occasional loss is
    acceptable.

``lease_consumer()`` / ``lease_producer()`` with ``ack()`` / ``release()``
    At-least-once. The server marks the row as leased for
    ``lease_seconds`` and returns a :class:`Lease`. The row is invisible
    to other consumers until it is acked (deleted), released (made
    visible again), or the lease expires (made visible again
    automatically). A consumer that crashes mid-processing loses
    nothing; one that processes an item but fails to ack sees it
    redelivered. Make processing idempotent.

Producing with ``send_to_producer()`` is at-least-once: the client retries
a put whose ACK never arrives, so a message can be stored twice if the ACK
is lost after the commit.

Serialization
-------------

Messages are serialized with ``json`` (UTF-8) and zlib-compressed. JSON
was chosen over pickle to eliminate the remote-code-execution risk of
deserializing untrusted input. The trade-off is that not every Python
object survives a round-trip:

* ``tuple`` -> becomes ``list``
* ``bytes`` -> not supported; raises ``TypeError`` on send
* ``set`` / ``frozenset`` -> not supported
* integer dict keys -> become strings
* custom classes -> not supported (provide a ``json.JSONEncoder`` if needed)
* ``datetime`` and similar -> not supported (encode as ISO strings)
* strings containing lone surrogates -> raise ``ValueError`` on send

If you need to ship binary payloads, base64-encode them yourself before
sending and decode after receiving.

Received payloads are decompressed with a hard cap of
``_MAX_DECOMPRESSED_BYTES``; anything that would expand beyond it raises
:class:`PayloadError` without allocating the full output, which defuses
zlib "bombs". A payload that cannot be decoded is dropped (deleted on the
server for leases) and reported as :class:`PayloadError`, so a poison
message never blocks the queue.

Persistence
-----------

The server-side queues are SQLite tables. Each row holds:

* ``id`` - auto-increment primary key, used for FIFO ordering
* ``payload`` - the compressed JSON blob
* ``created_at`` - UTC seconds since the epoch (``time.time()``), float
* ``leased_until`` - UTC seconds until which the row is leased (0 = free)
* ``lease_token`` - random token identifying the current lease (0 = free)

A background maintenance thread runs every ``reaper_interval`` seconds
and:

1. Reaps finished worker threads from the in-memory tracking list.
2. If ``ttl_seconds`` is set, deletes rows whose ``created_at`` is older
   than ``time.time() - ttl_seconds`` (leased or not).
3. Compacts the DB: returns free pages to the OS via
   ``incremental_vacuum`` (up to ``_COMPACT_PAGE_BUDGET`` pages per pass)
   and truncates the WAL file via ``wal_checkpoint(TRUNCATE)``.

For compaction to work, fresh DBs are initialized with
``auto_vacuum=INCREMENTAL``. This setting can ONLY be enabled before any
table is created; pre-existing DBs without it get a logged warning
explaining the one-time manual migration::

    sqlite3 path/to/queue.db 'PRAGMA auto_vacuum=2; VACUUM;'

The schema version is tracked via ``PRAGMA user_version``. Schema v1
databases (no lease columns) are migrated in place on open with
``ALTER TABLE ... ADD COLUMN``. A DB stamped with a version newer than
this module understands is refused rather than risk corruption.

Durability
----------

Durability is controlled by ``synchronous``:

* ``"FULL"`` (default): every commit fsyncs the WAL. A committed row, and
  therefore every ACK the server sends, survives power loss.
* ``"NORMAL"``: the WAL is fsynced only at checkpoint. Rows survive
  process crashes but the most recent commits can be lost on power loss
  or OS crash. Roughly 2-10x higher write throughput on spinning disks.
* ``"EXTRA"``: like FULL, plus an extra fsync of the directory on
  checkpoint.

Timestamps are always UTC (``time.time()``), never local time.

Concurrency
-----------

WAL mode allows concurrent readers and a single writer. Each thread opens
its own SQLite connection lazily; connections are cached in
``threading.local`` storage. Read-only operations run without a
transaction; mutating operations run inside ``BEGIN IMMEDIATE``.

Multiple ``MyQueue`` server instances pointing at the same ``db_path``
compete for the writer lock, and long-poll wake-ups only propagate within
one process (other processes' inserts are noticed by a 1-second poll).
Workable but rarely what you want. Don't put the DB on a network
filesystem; SQLite is unhappy there, and WAL mode is refused at startup if
the filesystem cannot support it.

Backpressure
------------

Each table holds at most ``max_queue_size`` rows (leased rows included).
``full_policy`` decides what happens to an insert beyond that:

* ``"drop_oldest"`` (default): the oldest rows are deleted to make room
  (ring-buffer behaviour). The put is still ACKed; a warning is logged.
* ``"reject"``: the insert is refused. Over the wire the server answers
  ``_OP_FULL`` and the client raises ``queue.Full`` without retrying;
  ``send_to_consumer()`` raises ``queue.Full`` directly.

Security
--------

Always construct with ``secret_key=secrets.token_bytes(32)`` (or longer).
With a key set, every frame is HMAC-SHA256 signed on send and verified on
receive. Without a key, anyone who can connect can inject arbitrary
messages; a loud warning is logged.

HMAC provides integrity and authenticity only. It does NOT provide
confidentiality (payloads travel in plaintext) or replay protection (an
attacker on the network path can re-send a captured PUT to store a
duplicate). Run the queue on a trusted network, or tunnel it over SSH,
WireGuard or TLS (e.g. stunnel) when crossing untrusted networks.

Resource limits: the ASCII length prefix may not exceed
``_MAX_HEADER_BYTES``, a frame body may not exceed ``_MAX_FRAME_BYTES``,
a decompressed payload may not exceed ``_MAX_DECOMPRESSED_BYTES``, a peer
that stalls in the middle of a frame for ``timeout`` seconds is
disconnected, and ``max_connections`` caps concurrent connections (each
one costs a thread). TCP keepalive is enabled by default so that peers
that vanish without closing the connection are detected.

Signals
-------

Call ``install_signal_handlers()`` after ``start_server()`` to install
SIGTERM/SIGINT handlers that invoke ``stop_server()``. The handler runs
on the main thread and blocks it for the duration of the thread joins
(bounded to a few seconds). ``uninstall_signal_handlers()`` restores the
previous handlers. ``stop_server()`` does NOT need to flush data; every
operation is already on disk.

Public API
----------

``MyQueue(host, port, *, db_path=None, secret_key=None, ...)``
    Construct a queue. Server-side methods require ``db_path``. Usable
    as a context manager; leaving the block stops the server and closes
    the client connection.

``start_server()`` / ``stop_server()`` / ``install_signal_handlers()`` /
``uninstall_signal_handlers()`` / ``wait_for_shutdown(timeout=None)``
    Server lifecycle.

``start_client()`` / ``close()``
    Client lifecycle.

``get_consumer(wait=0)`` / ``get_producer(wait=0)``
    Pop the next item (at-most-once). Raises ``queue.Empty`` if empty
    after waiting up to ``wait`` seconds, ``ConnectionError`` on socket /
    protocol failure, ``RemoteError`` if the server reports an error,
    ``PayloadError`` if the item could not be decoded.

``lease_consumer(wait=0)`` / ``lease_producer(wait=0)`` / ``ack(lease)`` /
``release(lease)``
    At-least-once consumption. Same exceptions as ``get_*``.

``send_to_consumer(blob)`` / ``send_to_producer(blob)``
    Push an item. ``send_to_consumer`` is server-side and writes
    directly to the DB; ``send_to_producer`` is client-side, goes over
    the wire and waits for the server's ACK. Both raise ``queue.Full``
    under ``full_policy="reject"``.

``consumer_size()`` / ``producer_size()`` / ``consumer_inflight()`` /
``producer_inflight()`` / ``consumer_empty()`` / ``producer_empty()`` /
``peek_consumer()`` / ``peek_producer()`` / ``clear_queues()`` /
``compact()``
    Server-side introspection and maintenance.

Standard invocation
-------------------
q = MyQueue("0.0.0.0", 49152, db_path="/var/lib/queue.db", secret_key=KEY)
q.start_server()
q.install_signal_handlers()
q.wait_for_shutdown()  # blocks until SIGTERM/SIGINT
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import random
import secrets
import signal
import socket
import sqlite3
import struct
import threading
import time
import zlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Full
from typing import Any, Self

__all__ = [
    "Empty",
    "Full",
    "Lease",
    "MyQueue",
    "PayloadError",
    "ProtocolError",
    "RemoteError",
]

log = logging.getLogger(__name__)


# Wire-protocol opcodes. Each opcode is a single byte. Using bytes >= 0x10
# guarantees these will never collide with the first byte of a zlib stream
# (always 0x78) nor with any printable ASCII character.
_OP_GET_CONSUMER: bytes = b"\x10"  # client -> server: pop a consumer item
_OP_GET_PRODUCER: bytes = b"\x11"  # client -> server: pop a producer item
_OP_PUT_PRODUCER: bytes = b"\x12"  # client -> server: enqueue to producer queue
_OP_LEASE_CONSUMER: bytes = b"\x13"  # client -> server: lease a consumer item
_OP_LEASE_PRODUCER: bytes = b"\x14"  # client -> server: lease a producer item
_OP_ACK_ITEM: bytes = b"\x15"  # client -> server: delete a leased item
_OP_RELEASE_ITEM: bytes = b"\x16"  # client -> server: un-lease an item
_OP_ITEM: bytes = b"\x20"  # server -> client: here is an item (payload follows)
_OP_EMPTY: bytes = b"\x21"  # server -> client: queue was empty (no payload)
_OP_ACK: bytes = b"\x22"  # server -> client: request applied
_OP_LEASED: bytes = b"\x23"  # server -> client: id+token+payload of a leased item
_OP_ERROR: bytes = b"\x24"  # server -> client: request failed (utf-8 reason)
_OP_FULL: bytes = b"\x25"  # server -> client: queue full, put rejected

_GET_TABLE: dict[bytes, str] = {
    _OP_GET_CONSUMER: "consumer",
    _OP_GET_PRODUCER: "producer",
}
_LEASE_TABLE: dict[bytes, str] = {
    _OP_LEASE_CONSUMER: "consumer",
    _OP_LEASE_PRODUCER: "producer",
}

_WAIT_STRUCT = struct.Struct(">d")  # optional GET/LEASE payload: wait seconds
_LEASE_STRUCT = struct.Struct(">qq")  # LEASED prefix: row id, lease token
_REF_STRUCT = struct.Struct(">Bqq")  # ACK/RELEASE payload: table idx, id, token

_HMAC_SIZE: int = 32  # HMAC-SHA256 digest length, in bytes
_MAX_HEADER_BYTES: int = 16  # generous cap on the ASCII length prefix
_MAX_FRAME_BYTES: int = 64 * 1024 * 1024  # hard cap on a single frame body
# Largest message payload that can be carried in any frame type, leaving
# room for the opcode, the LEASED prefix and the HMAC.
_MAX_PAYLOAD_BYTES: int = _MAX_FRAME_BYTES - 1 - _LEASE_STRUCT.size - _HMAC_SIZE
_MAX_DECOMPRESSED_BYTES: int = 256 * 1024 * 1024  # cap on an inflated payload
_RECV_CHUNK: int = 65536  # max bytes requested per recv() call
_MAX_WAIT_SECONDS: float = 3600.0  # longest a GET/LEASE may long-poll
_WAIT_POLL_SECONDS: float = 1.0  # long-poll re-check interval
_MIN_SQLITE_VERSION: tuple[int, int, int] = (3, 35, 0)  # DELETE ... RETURNING
_SQLITE_BUSY_TIMEOUT: float = 5.0  # seconds to wait for the writer lock
_COMPACT_PAGE_BUDGET: int = 1000  # max pages reclaimed per compaction pass

# TCP keepalive tuning: probe after 60 idle seconds, every 10 seconds,
# give up after 5 unanswered probes (~110 s to detect a vanished peer).
_KEEPALIVE_IDLE: int = 60
_KEEPALIVE_INTERVAL: int = 10
_KEEPALIVE_COUNT: int = 5

# Whitelist of valid table names. We do interpolate table names into SQL
# strings (sqlite3 placeholders don't work for table names), so the names
# come exclusively from this tuple. The index of a table in this tuple is
# also its wire encoding in ACK/RELEASE frames.
_TABLES: tuple[str, ...] = ("consumer", "producer")

_SYNC_MODES: tuple[str, ...] = ("NORMAL", "FULL", "EXTRA")
_FULL_POLICIES: tuple[str, ...] = ("drop_oldest", "reject")

# Schema version stored in PRAGMA user_version. Bump when the schema changes
# and add a migration in _initialize_db. An old binary opening a DB whose
# schema is newer than it understands aborts rather than risk corruption.
#   v1: id, payload, created_at
#   v2: + leased_until, lease_token
_SCHEMA_VERSION = 2


# --------------------------------------------------------------------------- #
# Exceptions and public types                                                 #
# --------------------------------------------------------------------------- #


class ProtocolError(ValueError):
    """A frame was malformed, oversized, truncated, stalled, or failed HMAC
    verification. The connection it arrived on can no longer be trusted."""


class PayloadError(ValueError):
    """A received payload could not be decompressed or decoded. The
    connection itself is fine."""


class RemoteError(RuntimeError):
    """The server answered a request with an error frame. The connection
    itself is fine."""


@dataclass(frozen=True, slots=True)
class Lease:
    """An item handed out by ``lease_*()``. Pass it back to ``ack()`` once
    processed, or to ``release()`` to return it to the queue early."""

    item: Any
    queue: str
    id: int
    token: int


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _close_socket(sock: socket.socket | None) -> None:
    """Best-effort shutdown and close of a socket."""
    if sock is None:
        return
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except Exception:
        pass
    try:
        sock.close()
    except Exception:
        pass


def _enable_keepalive(sock: socket.socket) -> None:
    """Turn on TCP keepalive with aggressive-ish probing so that a peer
    that vanished without a FIN (power loss, NAT table expiry) is detected
    and ``recv`` raises instead of blocking forever. The per-option names
    differ by platform; missing ones are skipped."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    for name, value in (
        ("TCP_KEEPIDLE", _KEEPALIVE_IDLE),  # Linux, BSD
        ("TCP_KEEPALIVE", _KEEPALIVE_IDLE),  # macOS idle time
        ("TCP_KEEPINTVL", _KEEPALIVE_INTERVAL),
        ("TCP_KEEPCNT", _KEEPALIVE_COUNT),
    ):
        opt = getattr(socket, name, None)
        if opt is None:
            continue
        try:
            sock.setsockopt(socket.IPPROTO_TCP, opt, value)
        except OSError:
            pass


def _configure_socket(sock: socket.socket, timeout: float, keepalive: bool) -> None:
    """Apply the per-connection socket options used on both ends."""
    sock.settimeout(timeout)
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if keepalive:
            _enable_keepalive(sock)
    except OSError as ex:
        log.warning("Could not configure socket options: %s", ex)


def _serialize(blob: Any) -> bytes:
    """JSON-encode and zlib-compress an arbitrary JSON-serializable object.

    Raises ``TypeError`` if ``blob`` contains values JSON cannot represent
    (bytes, sets, custom objects, etc.) and ``ValueError`` if a string
    contains a lone surrogate. Tuples become lists and integer dict keys
    become strings on round-trip; see the module docstring."""
    encoded = json.dumps(blob, separators=(",", ":"), ensure_ascii=False)
    try:
        raw = encoded.encode("utf-8")
    except UnicodeEncodeError as ex:
        raise ValueError(
            "payload contains a lone surrogate that cannot be UTF-8 encoded: %s" % ex
        ) from ex
    return zlib.compress(raw, 1)


def _deserialize(data: bytes) -> Any:
    """Inverse of ``_serialize``. Raises :class:`PayloadError` on malformed
    input or on input that would inflate beyond ``_MAX_DECOMPRESSED_BYTES``
    (checked incrementally, so a zlib bomb never gets allocated)."""
    inflater = zlib.decompressobj()
    try:
        raw = inflater.decompress(data, _MAX_DECOMPRESSED_BYTES + 1)
    except zlib.error as ex:
        raise PayloadError("malformed payload: %s" % ex) from ex
    if len(raw) > _MAX_DECOMPRESSED_BYTES:
        raise PayloadError(
            "payload inflates beyond %d bytes; refusing to decompress"
            % _MAX_DECOMPRESSED_BYTES
        )
    if not inflater.eof:
        raise PayloadError("malformed payload: truncated zlib stream")
    try:
        return json.loads(raw.decode("utf-8"))
    except ValueError as ex:  # includes UnicodeDecodeError and JSONDecodeError
        raise PayloadError("malformed payload: %s" % ex) from ex


def _frame_size(payload_len: int, secret_key: bytes | None) -> int:
    """Size of the frame body for a payload of ``payload_len`` bytes."""
    return 1 + payload_len + (_HMAC_SIZE if secret_key else 0)


def _check_payload_size(payload: bytes) -> None:
    if len(payload) > _MAX_PAYLOAD_BYTES:
        raise ValueError(
            "serialized payload is %d bytes; the maximum is %d"
            % (len(payload), _MAX_PAYLOAD_BYTES)
        )


def _parse_wait(payload: bytes) -> float:
    """Decode the optional long-poll duration carried by GET/LEASE."""
    if not payload:
        return 0.0
    if len(payload) != _WAIT_STRUCT.size:
        raise ProtocolError("bad wait payload length %d" % len(payload))
    (wait,) = _WAIT_STRUCT.unpack(payload)
    if math.isnan(wait) or wait < 0 or wait > _MAX_WAIT_SECONDS:
        raise ProtocolError("wait out of range: %r" % wait)
    return wait


def _parse_ref(payload: bytes) -> tuple[str, int, int]:
    """Decode the (table, id, token) triple carried by ACK/RELEASE."""
    if len(payload) != _REF_STRUCT.size:
        raise ProtocolError("bad item reference length %d" % len(payload))
    table_idx, row_id, token = _REF_STRUCT.unpack(payload)
    if table_idx >= len(_TABLES):
        raise ProtocolError("bad table index %d" % table_idx)
    return _TABLES[table_idx], row_id, token


def _check_wait_arg(wait: float) -> float:
    if isinstance(wait, bool) or not isinstance(wait, (int, float)):
        raise TypeError("wait must be a number, got %s" % type(wait).__name__)
    if math.isnan(wait) or wait < 0 or wait > _MAX_WAIT_SECONDS:
        raise ValueError("wait must be between 0 and %g seconds" % _MAX_WAIT_SECONDS)
    return float(wait)


def _check_positive(name: str, value: Any, *, allow_none: bool = False) -> None:
    if value is None and allow_none:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(  # noqa: TRY004 - public API raises ValueError
            "%s must be a number, got %s" % (name, type(value).__name__)
        )
    if not value > 0:  # also rejects NaN
        raise ValueError("%s must be > 0" % name)


class _Frame:
    """Frame writer. Stateless; just a namespace."""

    @staticmethod
    def write(
        sock: socket.socket,
        opcode: bytes,
        payload: bytes,
        secret_key: bytes | None,
    ) -> None:
        size = _frame_size(len(payload), secret_key)
        if size > _MAX_FRAME_BYTES:
            raise ValueError(
                "frame too large: %d bytes (max %d)" % (size, _MAX_FRAME_BYTES)
            )
        parts = [str(size).encode("ascii"), b":", opcode, payload]
        if secret_key:
            mac = hmac.new(secret_key, digestmod=hashlib.sha256)
            mac.update(opcode)
            mac.update(payload)
            parts.append(mac.digest())
        # One join, one sendall: a single copy and (with TCP_NODELAY) a
        # single segment for small frames.
        sock.sendall(b"".join(parts))


class _FrameReader:
    """Per-connection buffered frame reader.

    Reads from the socket in large chunks and keeps any bytes belonging to
    a following frame in ``_buf``, so the header no longer has to be read
    one byte at a time. A socket timeout while the buffer is empty is an
    *idle* connection and surfaces as ``TimeoutError``; a timeout with a
    partial frame buffered means the peer stalled mid-frame and surfaces
    as :class:`ProtocolError`, because the stream can no longer be
    resynchronized.
    """

    __slots__ = ("_buf", "_key", "_sock")

    def __init__(self, sock: socket.socket, secret_key: bytes | None) -> None:
        self._sock = sock
        self._key = secret_key
        self._buf = bytearray()

    def read(self) -> tuple[bytes, bytes] | None:
        """Return ``(opcode, payload)``, or ``None`` if the peer closed the
        connection cleanly between frames. Raises :class:`ProtocolError`
        for malformed, oversized or unauthenticated frames and
        ``TimeoutError`` for an idle connection."""
        buf = self._buf
        sock = self._sock

        # --- header: "<digits>:" ---------------------------------------- #
        while True:
            idx = buf.find(b":", 0, _MAX_HEADER_BYTES + 1)
            if idx >= 0:
                break
            if len(buf) > _MAX_HEADER_BYTES:
                raise ProtocolError(
                    "header exceeds %d bytes; no colon found" % _MAX_HEADER_BYTES
                )
            try:
                chunk = sock.recv(_RECV_CHUNK)
            except TimeoutError:
                if buf:
                    raise ProtocolError("peer stalled mid-frame") from None
                raise  # idle connection
            if not chunk:
                if not buf:
                    return None  # clean close between frames
                raise ProtocolError("connection closed mid-header")
            buf += chunk

        length_str = bytes(buf[:idx])
        del buf[: idx + 1]
        if not length_str.isdigit():
            raise ProtocolError("non-numeric length: %r" % length_str)
        length = int(length_str)
        if length < 1:
            raise ProtocolError("frame too short: length=%d" % length)
        if length > _MAX_FRAME_BYTES:
            raise ProtocolError(
                "frame too large: %d bytes (max %d)" % (length, _MAX_FRAME_BYTES)
            )

        # --- body ---------------------------------------------------------- #
        if len(buf) >= length:
            # Whole frame already buffered (the common case for small frames).
            with memoryview(buf) as mv:
                body = bytearray(mv[:length])
            del buf[:length]
        else:
            # Pre-allocate once and recv straight into it; no re-growing.
            body = bytearray(length)
            have = len(buf)
            body[:have] = buf
            buf.clear()
            with memoryview(body) as mv:
                while have < length:
                    try:
                        n = sock.recv_into(mv[have:], min(length - have, _RECV_CHUNK))
                    except TimeoutError:
                        raise ProtocolError("peer stalled mid-frame") from None
                    if n == 0:
                        raise ProtocolError("connection closed mid-body")
                    have += n

        # --- authenticate and split ---------------------------------------- #
        end = len(body)
        with memoryview(body) as mv:
            if self._key:
                if end < _HMAC_SIZE + 1:
                    raise ProtocolError("frame too short for HMAC")
                end -= _HMAC_SIZE
                expected = hmac.new(self._key, mv[:end], hashlib.sha256).digest()
                if not hmac.compare_digest(bytes(mv[end:]), expected):
                    raise ProtocolError("HMAC verification failed")
            return bytes(mv[:1]), bytes(mv[1:end])


# --------------------------------------------------------------------------- #
# MyQueue                                                                     #
# --------------------------------------------------------------------------- #


class MyQueue:
    """Bidirectional TCP message queue with on-disk durability."""

    # ------------------------------------------------------------------ #
    # SQLite layer                                                       #
    # ------------------------------------------------------------------ #

    def _initialize_db(self) -> None:
        """Create or migrate the schema. Idempotent.

        On a fresh DB, sets ``auto_vacuum=INCREMENTAL`` so deletes can later
        be reclaimed back to the OS by ``_db_compact()``. ``auto_vacuum``
        can only be enabled before the first table is created; for
        pre-existing DBs without it, logs a one-time migration hint.

        Verifies the SQLite library is new enough, that WAL mode could be
        enabled, and that the on-disk schema version isn't newer than what
        this code understands. Migrates v1 databases to v2 by adding the
        lease columns, then stamps the version."""
        assert self._db_path is not None
        if sqlite3.sqlite_version_info < _MIN_SQLITE_VERSION:
            raise RuntimeError(
                "SQLite %s is too old; this module needs %s or newer for "
                "DELETE ... RETURNING"
                % (sqlite3.sqlite_version, ".".join(map(str, _MIN_SQLITE_VERSION)))
            )

        # Make sure the containing directory exists, otherwise sqlite3 fails
        # with an opaque "unable to open database file".
        Path(self._db_path).resolve().parent.mkdir(parents=True, exist_ok=True)

        # Open a dedicated connection for setup, separate from _open_db's
        # thread-local cache. We need direct pragma access outside a txn.
        conn = sqlite3.connect(
            self._db_path, isolation_level=None, timeout=_SQLITE_BUSY_TIMEOUT
        )
        try:
            (current_version,) = conn.execute("PRAGMA user_version").fetchone()
            if current_version > _SCHEMA_VERSION:
                raise RuntimeError(
                    f"Database at {self._db_path} has schema version "
                    f"{current_version}, newer than this code supports "
                    f"({_SCHEMA_VERSION}). Refusing to open to avoid "
                    "corruption."
                )

            placeholders = ",".join("?" * len(_TABLES))
            existing_tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master "
                    f"WHERE type='table' AND name IN ({placeholders})",
                    _TABLES,
                )
            }
            is_fresh = current_version == 0 and not existing_tables

            if is_fresh:
                # auto_vacuum=INCREMENTAL takes effect only on a virgin DB.
                # Once set, subsequent deletes free pages to a tracked list
                # which `PRAGMA incremental_vacuum` can later return to OS.
                # The VACUUM makes the new setting stick even if the file
                # already has a header page; it is a no-op cost on an empty
                # database. It must happen before journal_mode=WAL.
                conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
                conn.execute("VACUUM")
            else:
                (av_mode,) = conn.execute("PRAGMA auto_vacuum").fetchone()
                if av_mode != 2:  # 2 == INCREMENTAL
                    log.warning(
                        "Database at %s lacks auto_vacuum=INCREMENTAL. "
                        "The file will not shrink after deletes; "
                        "free pages will be reused but not returned to "
                        "the OS. To enable (this locks the DB; runtime "
                        "depends on size): "
                        "sqlite3 %s 'PRAGMA auto_vacuum=2; VACUUM;'",
                        self._db_path,
                        self._db_path,
                    )

            # journal_mode=WAL is persistent in the DB file; synchronous is
            # per-connection. Refuse to run if WAL could not be enabled
            # (typically a network filesystem): the concurrency and
            # durability story below depends on it.
            (mode,) = conn.execute("PRAGMA journal_mode=WAL").fetchone()
            if str(mode).lower() != "wal":
                raise RuntimeError(
                    f"Could not enable WAL mode on {self._db_path} (got "
                    f"journal_mode={mode!r}). Is the database on a network "
                    "filesystem?"
                )
            conn.execute(f"PRAGMA synchronous={self._synchronous}")

            # Create / migrate the schema in a transaction so a partial
            # failure leaves nothing half-built.
            conn.execute("BEGIN IMMEDIATE")
            try:
                for table in _TABLES:
                    conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table} (
                            id           INTEGER PRIMARY KEY AUTOINCREMENT,
                            payload      BLOB NOT NULL,
                            created_at   REAL NOT NULL,
                            leased_until REAL NOT NULL DEFAULT 0,
                            lease_token  INTEGER NOT NULL DEFAULT 0
                        )
                    """)
                    conn.execute(f"""
                        CREATE INDEX IF NOT EXISTS {table}_created_at_idx
                        ON {table}(created_at)
                    """)
                    # v1 -> v2 migration: add the lease columns to tables
                    # that predate them. ADD COLUMN with a constant default
                    # is O(1) in SQLite (no table rewrite).
                    columns = {
                        row[1] for row in conn.execute(f"PRAGMA table_info({table})")
                    }
                    if "leased_until" not in columns:
                        conn.execute(
                            f"ALTER TABLE {table} ADD COLUMN "
                            "leased_until REAL NOT NULL DEFAULT 0"
                        )
                    if "lease_token" not in columns:
                        conn.execute(
                            f"ALTER TABLE {table} ADD COLUMN "
                            "lease_token INTEGER NOT NULL DEFAULT 0"
                        )
                if not is_fresh and current_version < _SCHEMA_VERSION:
                    log.info(
                        "Migrated %s from schema v%d to v%d",
                        self._db_path,
                        current_version,
                        _SCHEMA_VERSION,
                    )
                # PRAGMA user_version doesn't accept a bound parameter;
                # literal interpolation of an int constant is safe.
                conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise
        finally:
            conn.close()

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 49152,
        *,
        db_path: str | Path | None = None,
        secret_key: bytes | None = None,
        max_queue_size: int = 10_000,
        full_policy: str = "drop_oldest",
        ttl_seconds: float | None = None,
        lease_seconds: float = 30.0,
        reaper_interval: float = 60.0,
        timeout: float = 75.0,
        connect_timeout: float = 10.0,
        synchronous: str = "FULL",
        max_connections: int | None = None,
        keepalive: bool = True,
    ) -> None:
        if not isinstance(host, str):
            raise ValueError(  # noqa: TRY004 - public API raises ValueError
                "host must be a string, got %s" % type(host).__name__
            )
        if (
            isinstance(port, bool)
            or not isinstance(port, int)
            or not (1 <= port <= 65535)
        ):
            raise ValueError(
                "port must be an integer between 1 and 65535, got %r" % port
            )
        if secret_key is not None:
            if not isinstance(secret_key, (bytes, bytearray)):
                raise ValueError(
                    "secret_key must be bytes, got %s" % type(secret_key).__name__
                )
            if len(secret_key) < 16:
                raise ValueError("secret_key must be at least 16 bytes")
        if isinstance(max_queue_size, bool) or not isinstance(max_queue_size, int):
            raise ValueError(  # noqa: TRY004 - public API raises ValueError
                "max_queue_size must be an integer, got %s"
                % type(max_queue_size).__name__
            )
        if max_queue_size < 1:
            raise ValueError("max_queue_size must be >= 1")
        if full_policy not in _FULL_POLICIES:
            raise ValueError(
                "full_policy must be one of %s, got %r" % (_FULL_POLICIES, full_policy)
            )
        _check_positive("ttl_seconds", ttl_seconds, allow_none=True)
        _check_positive("lease_seconds", lease_seconds)
        _check_positive("reaper_interval", reaper_interval)
        _check_positive("timeout", timeout)
        _check_positive("connect_timeout", connect_timeout)
        if not isinstance(synchronous, str) or synchronous.upper() not in _SYNC_MODES:
            raise ValueError(
                "synchronous must be one of %s, got %r" % (_SYNC_MODES, synchronous)
            )
        if max_connections is not None and (
            isinstance(max_connections, bool)
            or not isinstance(max_connections, int)
            or max_connections < 1
        ):
            raise ValueError(
                "max_connections must be a positive integer or None, got %r"
                % max_connections
            )
        if not isinstance(keepalive, bool):
            raise ValueError(  # noqa: TRY004 - public API raises ValueError
                "keepalive must be a bool, got %s" % type(keepalive).__name__
            )

        resolved_db: str | None = None
        if db_path is not None:
            raw_path = str(db_path)
            if raw_path in ("", ":memory:") or raw_path.startswith("file:"):
                raise ValueError(
                    "db_path %r is not supported: every thread opens its own "
                    "SQLite connection, so an in-memory or URI database would "
                    "give each thread a separate empty database. Use a file path."
                    % raw_path
                )
            # Expand "~" exactly once, here, so every later connect() sees
            # the same real path.
            resolved_db = str(Path(raw_path).expanduser())

        self._addr: tuple[str, int] = (host, port)
        self._db_path: str | None = resolved_db
        self._secret_key: bytes | None = (
            bytes(secret_key) if secret_key is not None else None
        )
        self._max_queue_size: int = max_queue_size
        self._full_policy: str = full_policy
        self._ttl_seconds: float | None = ttl_seconds
        self._lease_seconds: float = float(lease_seconds)
        self._reaper_interval: float = reaper_interval
        self._timeout: float = timeout
        self._connect_timeout: float = connect_timeout
        self._synchronous: str = synchronous.upper()
        self._max_connections: int | None = max_connections
        self._keepalive: bool = keepalive

        # Thread-local SQLite connections: one per thread, lazily opened.
        self._tls = threading.local()

        # Server-side runtime state.
        self._workers: list[socket.socket] = []
        self._workers_lock = threading.Lock()
        self._worker_threads: list[threading.Thread] = []
        self._worker_threads_lock = threading.Lock()
        self._shutdown = threading.Event()
        self._ssock: socket.socket | None = None
        self._listen_thread: threading.Thread | None = None
        self._maintenance_thread: threading.Thread | None = None
        # Long-poll wake-ups: one condition per table, notified on insert
        # and on release.
        self._item_conds: dict[str, threading.Condition] = {
            table: threading.Condition() for table in _TABLES
        }
        self._prev_signal_handlers: dict[int, Any] = {}

        # Client-side state.
        self._csock: socket.socket | None = None
        self._creader: _FrameReader | None = None
        self._csock_lock = threading.RLock()

        # Initialize the DB schema if a path was provided. Doing this here
        # (rather than lazily on first use) surfaces permission / disk
        # errors at construction time, where they belong.
        if self._db_path is not None:
            self._initialize_db()

        if self._secret_key is None:
            log.warning(
                "MyQueue(%s:%d) constructed without secret_key. "
                "Frames are NOT authenticated; anyone who can connect can "
                "inject arbitrary messages. "
                "Pass secret_key=secrets.token_bytes(32) for safe operation.",
                host,
                port,
            )

    def _open_db(self) -> sqlite3.Connection:
        """Return this thread's connection, opening it lazily.
        Reopens if ``db_path`` has changed since this thread last used it
        (which happens between test cases)."""
        if self._db_path is None:
            raise RuntimeError(
                "this MyQueue was constructed without db_path; "
                "DB-backed methods are unavailable"
            )

        cached = getattr(self._tls, "conn", None)
        cached_path = getattr(self._tls, "path", None)
        if cached is not None and cached_path == self._db_path:
            return cached
        if cached is not None:
            try:
                cached.close()
            except Exception:
                pass
        # Drop the stale entries *before* connecting: if connect() raises we
        # must not leave a closed connection cached.
        self._tls.conn = None
        self._tls.path = None

        conn = sqlite3.connect(
            self._db_path, isolation_level=None, timeout=_SQLITE_BUSY_TIMEOUT
        )
        # WAL mode is persistent in the DB file, but we re-set it because
        # this might be the first open. synchronous is per-connection.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA synchronous={self._synchronous}")
        self._tls.conn = conn
        self._tls.path = self._db_path
        return conn

    def close_db(self) -> None:
        """Close this thread's cached SQLite connection, if any."""
        conn = getattr(self._tls, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        self._tls.conn = None
        self._tls.path = None

    @contextmanager
    def _db_txn(self) -> Iterator[sqlite3.Connection]:
        """Context manager that opens a write transaction, commits on
        success, rolls back on *any* exception (including BaseException
        such as KeyboardInterrupt, which would otherwise leave the
        thread's connection stuck inside an open transaction)."""
        conn = self._open_db()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        else:
            try:
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    def _db_compact(self) -> None:
        """Reclaim free pages back to the OS and truncate the WAL file.

        Order matters: we checkpoint first to flush WAL frames into the
        main DB (so freed pages are visible to ``incremental_vacuum``),
        then vacuum, then checkpoint again to flush the vacuum's own
        writes and shrink the WAL file.

        ``incremental_vacuum`` does its work as its cursor is stepped, and
        how many pages one ``execute().fetchall()`` reclaims varies between
        SQLite builds (some yield a row per page, others free a single page
        per statement). Rather than trust that, we loop and watch
        ``freelist_count`` until it stops shrinking or the per-pass budget
        of ``_COMPACT_PAGE_BUDGET`` pages (~4MB at the default page size)
        is spent, which bounds how long any single compaction call can
        take."""
        if self._db_path is None:
            return
        try:
            conn = self._open_db()
            # 1. Flush WAL into main DB so vacuum sees the freed pages.
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
            # 2. Reclaim free pages, bounded by the page budget.
            reclaimed = 0
            budget = _COMPACT_PAGE_BUDGET
            while budget > 0:
                (before,) = conn.execute("PRAGMA freelist_count").fetchone()
                if before == 0:
                    break
                conn.execute("PRAGMA incremental_vacuum(%d)" % budget).fetchall()
                (after,) = conn.execute("PRAGMA freelist_count").fetchone()
                freed = before - after
                if freed <= 0:
                    break
                reclaimed += freed
                budget -= freed
            # 3. Truncate the WAL again to flush vacuum's own bookkeeping.
            ckpt = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if reclaimed:
                log.debug(
                    "DB compact: reclaimed %d pages, WAL log=%d ckpt=%d busy=%d",
                    reclaimed,
                    ckpt[1] if ckpt else -1,
                    ckpt[2] if ckpt else -1,
                    ckpt[0] if ckpt else -1,
                )
        except sqlite3.Error as ex:
            log.warning("DB compaction failed: %s", ex)

    def compact(self) -> None:
        """Reclaim free disk pages and truncate the WAL file.

        Called automatically by the maintenance loop on
        ``reaper_interval``, but useful to invoke manually after a large
        drain to immediately recover disk space."""
        if self._db_path is None:
            raise RuntimeError("compact requires db_path to be set")
        self._db_compact()

    def _notify(self, table: str) -> None:
        """Wake long-polling GET/LEASE handlers waiting on ``table``."""
        cond = self._item_conds[table]
        with cond:
            cond.notify_all()

    def _enqueue(self, table: str, payload: bytes) -> bool:
        """Insert one row. Returns ``False`` (and inserts nothing) if the
        table is full and ``full_policy`` is ``"reject"``; under
        ``"drop_oldest"`` the oldest rows are evicted to make room.

        ``SELECT COUNT(*)`` is deliberate: SQLite answers it by walking
        b-tree pages rather than rows, which benchmarks faster than any
        ``OFFSET``-based probe whenever the table is at or below
        ``max_queue_size``, i.e. always, since eviction keeps it there."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        if not payload:
            raise ValueError("refusing to enqueue an empty payload")
        now = time.time()
        inserted = False
        with self._db_txn() as conn:
            (count,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            # Evict as many as needed so that after the insert we are at
            # most at max_queue_size (a single delete is not enough if the
            # table somehow grew past the limit).
            overflow = count - self._max_queue_size + 1
            if overflow > 0 and self._full_policy == "reject":
                log.warning(
                    "%s queue at capacity (%d); rejecting put",
                    table,
                    self._max_queue_size,
                )
            else:
                if overflow > 0:
                    log.warning(
                        "%s queue at capacity (%d); dropping %d oldest",
                        table,
                        self._max_queue_size,
                        overflow,
                    )
                    conn.execute(
                        f"DELETE FROM {table} "
                        f"WHERE id IN (SELECT id FROM {table} ORDER BY id LIMIT ?)",
                        (overflow,),
                    )
                conn.execute(
                    f"INSERT INTO {table} (payload, created_at) VALUES (?, ?)",
                    (payload, now),
                )
                inserted = True
        if inserted:
            self._notify(table)
        return inserted

    def _dequeue(self, table: str) -> bytes | None:
        """Atomically pop the oldest *unleased* row's payload. Returns None
        if nothing is available. Uses ``DELETE ... RETURNING`` (SQLite >=
        3.35)."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        now = time.time()
        with self._db_txn() as conn:
            # SQLite applies all of a RETURNING statement's changes on the
            # first step, but the cursor still has to be drained so the
            # statement is reset before COMMIT runs on this connection.
            rows = conn.execute(
                f"DELETE FROM {table} WHERE id = ("
                f"  SELECT id FROM {table} WHERE leased_until < ? "
                "  ORDER BY id LIMIT 1"
                ") RETURNING payload",
                (now,),
            ).fetchall()
            return rows[0][0] if rows else None

    def _lease(self, table: str) -> tuple[int, int, bytes] | None:
        """Mark the oldest unleased row as leased for ``lease_seconds`` and
        return ``(id, token, payload)``, or None if nothing is available."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        now = time.time()
        token = 1 + secrets.randbits(62)  # never 0, always fits a signed int64
        with self._db_txn() as conn:
            rows = conn.execute(
                f"UPDATE {table} SET leased_until = ?, lease_token = ? WHERE id = ("
                f"  SELECT id FROM {table} WHERE leased_until < ? "
                "  ORDER BY id LIMIT 1"
                ") RETURNING id, payload",
                (now + self._lease_seconds, token, now),
            ).fetchall()
            if not rows:
                return None
            row_id, payload = rows[0]
            return row_id, token, payload

    def _ack_item(self, table: str, row_id: int, token: int) -> bool:
        """Delete a leased row. Returns False if the lease is no longer
        valid (expired and re-leased, already acked, evicted, reaped)."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        with self._db_txn() as conn:
            cur = conn.execute(
                f"DELETE FROM {table} "
                "WHERE id = ? AND lease_token = ? AND lease_token != 0",
                (row_id, token),
            )
            return cur.rowcount == 1

    def _release_item(self, table: str, row_id: int, token: int) -> bool:
        """Return a leased row to the queue immediately. Returns False if
        the lease is no longer valid."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        with self._db_txn() as conn:
            cur = conn.execute(
                f"UPDATE {table} SET leased_until = 0, lease_token = 0 "
                "WHERE id = ? AND lease_token = ? AND lease_token != 0",
                (row_id, token),
            )
            released = cur.rowcount == 1
        if released:
            self._notify(table)
        return released

    def _table_size(self, table: str) -> int:
        """Total rows, leased or not."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        conn = self._open_db()  # read-only: no writer lock needed
        (count,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return count

    def _table_inflight(self, table: str) -> int:
        """Rows currently under an unexpired lease."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        conn = self._open_db()
        (count,) = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE leased_until >= ?", (time.time(),)
        ).fetchone()
        return count

    def _table_empty(self, table: str) -> bool:
        """O(1) check for *no rows at all*, unlike COUNT(*)."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        conn = self._open_db()  # read-only: no writer lock needed
        row = conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
        return row is None

    def _table_peek(self, table: str) -> bytes | None:
        """Return the oldest unleased payload without removing it."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        conn = self._open_db()  # read-only: no writer lock needed
        row = conn.execute(
            f"SELECT payload FROM {table} WHERE leased_until < ? ORDER BY id LIMIT 1",
            (time.time(),),
        ).fetchone()
        return row[0] if row else None

    def _reap_expired(self) -> dict[str, int]:
        """Delete rows older than ``ttl_seconds``. Returns counts per table."""
        if self._ttl_seconds is None:
            return {}
        cutoff = time.time() - self._ttl_seconds
        deleted: dict[str, int] = {}
        with self._db_txn() as conn:
            for table in _TABLES:
                cur = conn.execute(
                    f"DELETE FROM {table} WHERE created_at < ?", (cutoff,)
                )
                if cur.rowcount > 0:
                    deleted[table] = cur.rowcount
        return deleted

    # ------------------------------------------------------------------ #
    # Server request handling                                            #
    # ------------------------------------------------------------------ #

    def _await_item(self, table: str, fetch: Callable[[], Any], wait: float) -> Any:
        """Run ``fetch`` until it returns something, re-trying for up to
        ``wait`` seconds. Wakes early when ``_notify(table)`` fires (an
        insert or release in this process) and re-checks at least every
        ``_WAIT_POLL_SECONDS`` to notice lease expiries, other processes'
        inserts, and shutdown."""
        if wait <= 0:
            return fetch()
        deadline = time.monotonic() + wait
        cond = self._item_conds[table]
        with cond:
            while True:
                result = fetch()
                if result is not None:
                    return result
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self._shutdown.is_set():
                    return None
                cond.wait(min(remaining, _WAIT_POLL_SECONDS))

    def _handle_request(self, opcode: bytes, payload: bytes) -> tuple[bytes, bytes]:
        """Execute one request and return ``(response_opcode, payload)``.
        Raises :class:`ProtocolError` for requests the server cannot make
        sense of; database errors propagate as ``sqlite3.Error``."""
        table = _GET_TABLE.get(opcode)
        if table is not None:
            wait = _parse_wait(payload)
            item = self._await_item(table, lambda: self._dequeue(table), wait)
            return (_OP_EMPTY, b"") if item is None else (_OP_ITEM, item)

        table = _LEASE_TABLE.get(opcode)
        if table is not None:
            wait = _parse_wait(payload)
            leased = self._await_item(table, lambda: self._lease(table), wait)
            if leased is None:
                return _OP_EMPTY, b""
            row_id, token, item = leased
            return _OP_LEASED, _LEASE_STRUCT.pack(row_id, token) + item

        if opcode == _OP_PUT_PRODUCER:
            if not payload:
                raise ProtocolError("empty PUT payload")
            if len(payload) > _MAX_PAYLOAD_BYTES:
                return _OP_ERROR, (
                    "payload too large: %d bytes (max %d)"
                    % (len(payload), _MAX_PAYLOAD_BYTES)
                ).encode("ascii")
            if self._enqueue("producer", payload):
                # ACK only after the row is committed, so the client knows
                # the message is durable.
                return _OP_ACK, b""
            return _OP_FULL, b""

        if opcode in (_OP_ACK_ITEM, _OP_RELEASE_ITEM):
            table, row_id, token = _parse_ref(payload)
            apply = self._ack_item if opcode == _OP_ACK_ITEM else self._release_item
            return _OP_ACK, (b"\x01" if apply(table, row_id, token) else b"\x00")

        raise ProtocolError("unknown opcode 0x%02x" % opcode[0])

    def _send_error(self, sock: socket.socket, message: str) -> None:
        """Best-effort error frame; failures are ignored because the
        connection is about to be closed anyway."""
        try:
            _Frame.write(
                sock,
                _OP_ERROR,
                message.encode("utf-8", "replace")[:1024],
                self._secret_key,
            )
        except (OSError, ValueError):
            pass

    def _serve_connection(self, client_sock: socket.socket, peer: str) -> None:
        """Handle a single accepted connection until close or shutdown."""
        reader = _FrameReader(client_sock, self._secret_key)
        try:
            while not self._shutdown.is_set():
                try:
                    frame = reader.read()
                except ProtocolError as ex:
                    log.warning("%s: closing connection: %s", peer, ex)
                    return
                except TimeoutError:
                    # Idle client with no partial frame buffered; keep the
                    # connection open unless we are shutting down.
                    continue
                if frame is None:
                    log.debug("%s: peer closed connection", peer)
                    return
                opcode, payload = frame

                try:
                    resp_opcode, resp_payload = self._handle_request(opcode, payload)
                except ProtocolError as ex:
                    log.warning(
                        "%s: protocol error on opcode 0x%02x: %s; closing connection",
                        peer,
                        opcode[0],
                        ex,
                    )
                    self._send_error(client_sock, str(ex))
                    return
                except sqlite3.Error as ex:
                    # Transient (lock timeout, disk full): tell the client
                    # and keep the connection; the next request may succeed.
                    log.error(
                        "%s: database error on opcode 0x%02x: %s", peer, opcode[0], ex
                    )
                    resp_opcode = _OP_ERROR
                    resp_payload = ("database error: %s" % ex).encode(
                        "utf-8", "replace"
                    )
                except Exception as ex:
                    log.exception(
                        "%s: unexpected error on opcode 0x%02x", peer, opcode[0]
                    )
                    self._send_error(client_sock, "internal server error: %s" % ex)
                    return
                _Frame.write(client_sock, resp_opcode, resp_payload, self._secret_key)
        except (ConnectionError, OSError) as ex:
            log.debug("%s: connection closed: %s", peer, ex)
        except Exception:
            log.exception("%s: unexpected error in connection thread", peer)
        finally:
            with self._workers_lock:
                try:
                    self._workers.remove(client_sock)
                except ValueError:
                    pass
            _close_socket(client_sock)
            # Each worker thread owns a SQLite connection; release it.
            self.close_db()

    def _controlling_loop(self) -> None:
        """Accept incoming connections and spawn a per-connection thread."""
        while not self._shutdown.is_set():
            ssock = self._ssock
            if ssock is None:
                return
            try:
                client_sock, addr = ssock.accept()
            except TimeoutError:
                continue
            except OSError:
                if self._shutdown.is_set():
                    return
                log.exception("accept() failed")
                time.sleep(0.5)
                continue
            except Exception:
                if self._shutdown.is_set():
                    return
                log.exception("Unexpected error in accept loop")
                time.sleep(0.5)
                continue

            if self._shutdown.is_set():
                _close_socket(client_sock)
                return

            peer = "%s:%s" % (addr[0], addr[1])

            with self._workers_lock:
                if (
                    self._max_connections is not None
                    and len(self._workers) >= self._max_connections
                ):
                    accepted = False
                else:
                    self._workers.append(client_sock)
                    accepted = True
            if not accepted:
                log.warning(
                    "%s: refusing connection; max_connections=%d reached",
                    peer,
                    self._max_connections,
                )
                _close_socket(client_sock)
                continue

            _configure_socket(client_sock, self._timeout, self._keepalive)

            t = threading.Thread(
                target=self._serve_connection,
                args=(client_sock, peer),
                name="tcpQueue-worker",
                daemon=True,
            )
            with self._worker_threads_lock:
                # Drop finished threads so the list can't grow without bound
                # between maintenance passes.
                self._worker_threads = [
                    th for th in self._worker_threads if th.is_alive()
                ]
                self._worker_threads.append(t)
            t.start()

    def _maintenance_loop(self) -> None:
        """Periodic background maintenance: thread reaping, TTL reaping,
        compaction."""
        try:
            while not self._shutdown.wait(self._reaper_interval):
                # 1. Drop references to finished worker threads.
                with self._worker_threads_lock:
                    self._worker_threads = [
                        t for t in self._worker_threads if t.is_alive()
                    ]

                # 2. Delete rows whose created_at is past the TTL cutoff.
                if self._ttl_seconds is not None:
                    try:
                        deleted = self._reap_expired()
                        for table, n in deleted.items():
                            log.info("Reaped %d expired entries from %s", n, table)
                    except Exception:
                        log.exception("TTL reaper iteration failed")

                # 3. Compact the DB: return free pages to the OS, truncate
                # the WAL.
                try:
                    self._db_compact()
                except Exception:
                    log.exception("DB compaction failed")
        finally:
            self.close_db()

    # ------------------------------------------------------------------ #
    # Server lifecycle                                                   #
    # ------------------------------------------------------------------ #

    def _resolve_listen_address(self) -> tuple[int, tuple[str, int]]:
        """Pick the address family for ``host``. IPv4 is preferred when a
        hostname resolves to both, preserving the historical default."""
        host, port = self._addr
        infos = socket.getaddrinfo(
            host or None,  # "" means "any interface", which getaddrinfo spells None
            port,
            socket.AF_UNSPEC,
            socket.SOCK_STREAM,
            0,
            socket.AI_PASSIVE,
        )
        infos.sort(key=lambda info: info[0] != socket.AF_INET)
        family, _, _, _, sockaddr = infos[0]
        return family, (str(sockaddr[0]), int(sockaddr[1]))

    def start_server(self) -> None:
        """Bind, listen, and start accepting connections in the background.
        Requires ``db_path`` to have been set at construction. IPv4 and
        IPv6 hosts are both accepted; ``"::"`` listens dual-stack where
        the platform supports it."""
        if self._db_path is None:
            raise RuntimeError("start_server requires db_path to be set")
        if self._ssock is not None:
            raise RuntimeError("server already started")

        family, sockaddr = self._resolve_listen_address()
        dualstack = (
            family == socket.AF_INET6
            and sockaddr[0] in ("::", "")
            and socket.has_dualstack_ipv6()
        )
        ssock = socket.create_server(
            sockaddr, family=family, backlog=128, dualstack_ipv6=dualstack
        )
        try:
            ssock.settimeout(1.0)  # let the accept loop poll the shutdown flag
        except Exception:
            _close_socket(ssock)
            raise

        # Clear the flag *before* the threads start, otherwise they may see a
        # stale "shutting down" state from a previous run and exit at once.
        self._shutdown.clear()
        self._ssock = ssock
        self._listen_thread = threading.Thread(
            target=self._controlling_loop,
            name="tcpQueue-listener",
            daemon=True,
        )
        self._listen_thread.start()
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            name="tcpQueue-maintenance",
            daemon=True,
        )
        self._maintenance_thread.start()

    def stop_server(self) -> None:
        """Stop accepting connections, close all server-side sockets, wake
        any long-polling handlers, and join background threads. Idempotent.
        No data flushing is needed; every operation is already on disk."""
        self._shutdown.set()

        ssock, self._ssock = self._ssock, None
        _close_socket(ssock)

        with self._workers_lock:
            workers = list(self._workers)
            self._workers.clear()
        for w in workers:
            _close_socket(w)

        for table in _TABLES:
            self._notify(table)

        current = threading.current_thread()
        for t in (self._listen_thread, self._maintenance_thread):
            if t is not None and t is not current and t.is_alive():
                t.join(timeout=5.0)
        self._listen_thread = None
        self._maintenance_thread = None

        with self._worker_threads_lock:
            threads = list(self._worker_threads)
            self._worker_threads.clear()
        for t in threads:
            if t is not current:
                t.join(timeout=1.0)

    def install_signal_handlers(
        self,
        signals: tuple[int, ...] | None = None,
    ) -> None:
        """Install handlers that call ``stop_server()`` on the given signals.

        Defaults to SIGTERM and SIGINT (whichever are available on this
        platform). Must be called from the main thread (Python only allows
        ``signal.signal`` from main). The previous handlers are remembered
        and can be put back with ``uninstall_signal_handlers()``.

        The handler runs on the main thread and blocks it while worker
        threads are joined (a few seconds at most). It is for clean
        teardown, NOT for data preservation: every operation is already
        durable on disk.
        """
        if signals is None:
            signals = tuple(
                sig
                for sig in (
                    getattr(signal, "SIGTERM", None),
                    getattr(signal, "SIGINT", None),
                )
                if sig is not None
            )

        def handler(signum: int, _frame: Any) -> None:
            log.info("Signal %d received; shutting down", signum)
            self.stop_server()

        for sig in signals:
            try:
                previous = signal.signal(sig, handler)
            except (ValueError, OSError) as ex:
                log.warning("Could not install handler for signal %s: %s", sig, ex)
                continue
            # Remember only the handler that was there before *our* first
            # install, so repeated installs don't overwrite it with ours.
            self._prev_signal_handlers.setdefault(int(sig), previous)

    def uninstall_signal_handlers(self) -> None:
        """Restore the handlers that were in place before
        ``install_signal_handlers()``. Must be called from the main thread."""
        for sig, previous in list(self._prev_signal_handlers.items()):
            try:
                signal.signal(sig, previous if previous is not None else signal.SIG_DFL)
            except (ValueError, OSError) as ex:
                log.warning("Could not restore handler for signal %s: %s", sig, ex)
            self._prev_signal_handlers.pop(sig, None)

    def wait_for_shutdown(self, timeout: float | None = None) -> bool:
        """Block until ``stop_server()`` is invoked (e.g. by a signal
        handler). Returns ``True`` if shutdown happened, ``False`` on
        timeout."""
        return self._shutdown.wait(timeout=timeout)

    # ------------------------------------------------------------------ #
    # Client side                                                        #
    # ------------------------------------------------------------------ #

    def _connect_locked(self) -> socket.socket | None:
        """Open a new client socket. Caller MUST hold self._csock_lock."""
        try:
            sock = socket.create_connection(self._addr, timeout=self._connect_timeout)
        except OSError as ex:
            log.error(
                "Unable to connect to %s:%d: %s",
                self._addr[0],
                self._addr[1],
                ex,
            )
            return None
        _configure_socket(sock, self._timeout, self._keepalive)
        self._creader = _FrameReader(sock, self._secret_key)
        return sock

    def _drop_connection_locked(self) -> None:
        _close_socket(self._csock)
        self._csock = None
        self._creader = None

    def start_client(self) -> bool:
        """(Re)connect to the server, dropping any existing connection.

        Returns ``True`` if a connection is established, ``False`` otherwise
        (later calls will transparently retry the connect). Calling it is
        optional: every client method connects on demand."""
        with self._csock_lock:
            self._drop_connection_locked()
            self._csock = self._connect_locked()
            return self._csock is not None

    def close(self) -> None:
        """Close the client connection."""
        with self._csock_lock:
            self._drop_connection_locked()

    def _ensure_connected_locked(self) -> socket.socket | None:
        if self._csock is None:
            self._csock = self._connect_locked()
        return self._csock

    def _drop_connection(self) -> None:
        with self._csock_lock:
            self._drop_connection_locked()

    def _request(
        self, opcode: bytes, payload: bytes, *, extra_wait: float = 0.0
    ) -> tuple[bytes, bytes]:
        """Send one request frame and return the response ``(opcode,
        payload)``. Connection-level failures drop the socket and raise
        ``ConnectionError``; an ``_OP_ERROR`` response raises
        :class:`RemoteError` and keeps the connection."""
        with self._csock_lock:
            sock = self._ensure_connected_locked()
            reader = self._creader
            if sock is None or reader is None:
                raise ConnectionError(
                    "not connected to %s:%d" % (self._addr[0], self._addr[1])
                )
            try:
                if extra_wait > 0:
                    # The server may legitimately hold a long-poll open for
                    # this long before answering.
                    sock.settimeout(self._timeout + extra_wait)
                _Frame.write(sock, opcode, payload, self._secret_key)
                frame = reader.read()
            except (OSError, ProtocolError) as ex:
                self._drop_connection_locked()
                raise ConnectionError(str(ex)) from ex
            finally:
                if extra_wait > 0 and self._csock is sock:
                    try:
                        sock.settimeout(self._timeout)
                    except OSError:
                        pass

            if frame is None:
                self._drop_connection_locked()
                raise ConnectionError("server closed connection")

            resp_opcode, resp_payload = frame
            if resp_opcode == _OP_ERROR:
                raise RemoteError(resp_payload.decode("utf-8", "replace"))
            return resp_opcode, resp_payload

    def _unexpected(self, resp_opcode: bytes) -> ConnectionError:
        """Drop the connection (we can't trust the stream any more) and
        build the exception for the caller to raise."""
        self._drop_connection()
        return ConnectionError("unexpected response opcode 0x%02x" % resp_opcode[0])

    def _get(self, opcode: bytes, wait: float) -> Any:
        wait = _check_wait_arg(wait)
        request_payload = _WAIT_STRUCT.pack(wait) if wait > 0 else b""
        resp_opcode, payload = self._request(opcode, request_payload, extra_wait=wait)
        if resp_opcode == _OP_EMPTY:
            raise Empty()
        if resp_opcode == _OP_ITEM:
            return _deserialize(payload)
        raise self._unexpected(resp_opcode)

    def get_consumer(self, wait: float = 0.0) -> Any:
        """Pop the next item from the consumer queue (at-most-once).

        ``wait`` > 0 lets the server hold the request open for up to that
        many seconds until an item arrives (long polling).

        Raises ``queue.Empty`` if the queue is (still) empty,
        ``ConnectionError`` on socket / protocol failure, ``RemoteError``
        if the server reported an error, ``PayloadError`` if the item
        could not be decoded (the item is dropped)."""
        return self._get(_OP_GET_CONSUMER, wait)

    def get_producer(self, wait: float = 0.0) -> Any:
        """Pop the next item from the producer queue (at-most-once). See
        ``get_consumer`` for semantics and exceptions."""
        return self._get(_OP_GET_PRODUCER, wait)

    def _lease_remote(self, opcode: bytes, table: str, wait: float) -> Lease:
        wait = _check_wait_arg(wait)
        request_payload = _WAIT_STRUCT.pack(wait) if wait > 0 else b""
        resp_opcode, payload = self._request(opcode, request_payload, extra_wait=wait)
        if resp_opcode == _OP_EMPTY:
            raise Empty()
        if resp_opcode != _OP_LEASED:
            raise self._unexpected(resp_opcode)
        if len(payload) < _LEASE_STRUCT.size:
            raise self._unexpected(resp_opcode)
        row_id, token = _LEASE_STRUCT.unpack_from(payload)
        try:
            item = _deserialize(payload[_LEASE_STRUCT.size :])
        except PayloadError:
            # A payload nobody can decode would otherwise be re-leased
            # forever once this lease expires. Drop it, like get_*() does.
            log.warning(
                "Dropping undecodable %s item id=%d (lease acked)", table, row_id
            )
            try:
                self.ack(Lease(item=None, queue=table, id=row_id, token=token))
            except Exception as ex:
                log.warning("Could not ack undecodable item id=%d: %s", row_id, ex)
            raise
        return Lease(item=item, queue=table, id=row_id, token=token)

    def lease_consumer(self, wait: float = 0.0) -> Lease:
        """Lease the next consumer item (at-least-once). The item stays
        invisible to other consumers for the server's ``lease_seconds``;
        call ``ack(lease)`` once processed or ``release(lease)`` to hand it
        back early. Same ``wait`` semantics and exceptions as
        ``get_consumer``."""
        return self._lease_remote(_OP_LEASE_CONSUMER, "consumer", wait)

    def lease_producer(self, wait: float = 0.0) -> Lease:
        """Lease the next producer item (at-least-once). See
        ``lease_consumer``."""
        return self._lease_remote(_OP_LEASE_PRODUCER, "producer", wait)

    def _item_ref_request(self, opcode: bytes, lease: Lease) -> bool:
        if not isinstance(lease, Lease):
            raise TypeError("expected a Lease, got %s" % type(lease).__name__)
        if lease.queue not in _TABLES:
            raise ValueError("unknown queue in lease: %r" % lease.queue)
        payload = _REF_STRUCT.pack(_TABLES.index(lease.queue), lease.id, lease.token)
        resp_opcode, resp_payload = self._request(opcode, payload)
        if resp_opcode != _OP_ACK or len(resp_payload) != 1:
            raise self._unexpected(resp_opcode)
        return resp_payload == b"\x01"

    def ack(self, lease: Lease) -> bool:
        """Delete a leased item; call this once it has been processed.
        Returns ``False`` if the lease was no longer valid (it expired and
        another consumer took the item, or the item was evicted or
        reaped), in which case the item may be processed again elsewhere."""
        return self._item_ref_request(_OP_ACK_ITEM, lease)

    def release(self, lease: Lease) -> bool:
        """Return a leased item to the queue immediately so another
        consumer can take it. Returns ``False`` if the lease was no longer
        valid."""
        return self._item_ref_request(_OP_RELEASE_ITEM, lease)

    def send_to_producer(self, blob: Any) -> bool:
        """Send ``blob`` to the producer queue and wait for the server's ACK.
        Retries up to 3 times with random back-off on connection or server
        errors, then drops the message. Returns ``True`` on success,
        ``False`` if the message was dropped.

        Delivery is at-least-once: if the ACK is lost after the server
        committed the row, the retry stores a second copy.

        Raises ``TypeError`` if ``blob`` is not JSON-serializable,
        ``ValueError`` if it serializes to more than the maximum payload
        size, and ``queue.Full`` (without retrying) if the server runs
        with ``full_policy="reject"`` and the queue is full."""
        payload = _serialize(blob)
        _check_payload_size(payload)

        attempts = 3
        for attempt in range(1, attempts + 1):
            try:
                resp_opcode, _ = self._request(_OP_PUT_PRODUCER, payload)
            except (ConnectionError, RemoteError) as ex:
                log.warning("Send failed (attempt %d/%d): %s", attempt, attempts, ex)
            else:
                if resp_opcode == _OP_ACK:
                    return True
                if resp_opcode == _OP_FULL:
                    raise Full("producer queue is full")
                log.warning(
                    "Send failed (attempt %d/%d): %s",
                    attempt,
                    attempts,
                    self._unexpected(resp_opcode),
                )
            if attempt < attempts:
                time.sleep(random.uniform(0, 2))

        log.error("Dropping message after %d failed send attempts", attempts)
        return False

    def send_to_consumer(self, blob: Any) -> None:
        """Push ``blob`` into the consumer queue (server side, direct DB
        write). Requires ``db_path`` to be set.

        Raises ``TypeError`` if ``blob`` is not JSON-serializable,
        ``ValueError`` if it is too large, and ``queue.Full`` if the queue
        is full under ``full_policy="reject"``."""
        if self._db_path is None:
            raise RuntimeError("send_to_consumer requires db_path to be set")
        payload = _serialize(blob)
        _check_payload_size(payload)
        if not self._enqueue("consumer", payload):
            raise Full("consumer queue is full")

    # ------------------------------------------------------------------ #
    # Introspection (server side)                                        #
    # ------------------------------------------------------------------ #

    def clear_queues(self) -> None:
        """Clear both queues, leased rows included. Server side only."""
        with self._db_txn() as conn:
            for table in _TABLES:
                conn.execute(f"DELETE FROM {table}")

    def consumer_size(self) -> int:
        """Rows in the consumer queue, including leased ones."""
        return self._table_size("consumer")

    def producer_size(self) -> int:
        """Rows in the producer queue, including leased ones."""
        return self._table_size("producer")

    def consumer_inflight(self) -> int:
        """Consumer rows currently leased to a client."""
        return self._table_inflight("consumer")

    def producer_inflight(self) -> int:
        """Producer rows currently leased to a client."""
        return self._table_inflight("producer")

    def consumer_empty(self) -> bool:
        """True if the consumer queue holds no rows at all."""
        return self._table_empty("consumer")

    def producer_empty(self) -> bool:
        """True if the producer queue holds no rows at all."""
        return self._table_empty("producer")

    def peek_consumer(self) -> Any:
        """Return the next available consumer item without removing it.
        Server side only. Raises ``queue.Empty`` if nothing is available."""
        payload = self._table_peek("consumer")
        if payload is None:
            raise Empty()
        return _deserialize(payload)

    def peek_producer(self) -> Any:
        """Return the next available producer item without removing it.
        Server side only. Raises ``queue.Empty`` if nothing is available."""
        payload = self._table_peek("producer")
        if payload is None:
            raise Empty()
        return _deserialize(payload)

    # ------------------------------------------------------------------ #
    # Context manager                                                    #
    # ------------------------------------------------------------------ #

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
        if self._ssock is not None or self._listen_thread is not None:
            self.stop_server()
        self.close_db()

    def __repr__(self) -> str:
        host, port = self._addr
        return "MyQueue(host=%r, port=%d, db_path=%r, secret_key=%s)" % (
            host,
            port,
            self._db_path,
            "<set>" if self._secret_key else "None",
        )
