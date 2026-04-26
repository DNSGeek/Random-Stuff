# -*- coding: utf-8 -*-
"""
Bidirectional TCP message queue with on-disk durability.

Server-side queues are stored in an on-disk SQLite database (WAL mode).
Every enqueue and dequeue is a transaction, so queue contents survive
process crashes, hard kills, OOM, and power loss — anything that would
let the disk's WAL log survive.

Wire format
-----------

Each frame on the wire is::

    <ascii_length>:<opcode_byte><payload_bytes>[<hmac_bytes>]

`<ascii_length>` is the total byte length of everything after the colon,
encoded as ASCII digits. `<opcode_byte>` is one byte identifying the frame
type (see ``_OP_*`` below). `<payload_bytes>` is the (optional) message
payload. `<hmac_bytes>` is a 32-byte HMAC-SHA256 over
`<opcode_byte><payload_bytes>`, present only when the queue was constructed
with a ``secret_key``.

Opcodes are byte values >= 0x10, which guarantees they cannot collide with
the first byte of a zlib stream (always 0x78) or with any printable ASCII
character.

Serialization
-------------

Messages are serialized with ``json`` (UTF-8) and zlib-compressed. JSON
was chosen over pickle to eliminate the remote-code-execution risk of
deserializing untrusted input. The trade-off is that not every Python
object survives a round-trip:

* ``tuple`` → becomes ``list``
* ``bytes`` → not supported; raises ``TypeError`` on send
* ``set`` / ``frozenset`` → not supported
* integer dict keys → become strings
* custom classes → not supported (provide a ``json.JSONEncoder`` if needed)
* ``datetime`` and similar → not supported (encode as ISO strings)

If you need to ship binary payloads, base64-encode them yourself before
sending and decode after receiving.

Persistence
-----------

The server-side queues are SQLite tables. Each row holds:

* ``id`` — auto-increment primary key, used for FIFO ordering
* ``payload`` — the compressed JSON blob
* ``created_at`` — UTC seconds since the epoch (``time.time()``), float

A background maintenance thread runs every ``reaper_interval`` seconds
and:

1. Reaps finished worker threads from the in-memory tracking list.
2. If ``ttl_seconds`` is set, deletes rows whose ``created_at`` is older
   than ``time.time() - ttl_seconds``.
3. Compacts the DB: returns free pages to the OS via
   ``incremental_vacuum`` (up to 100 pages per pass) and truncates the
   WAL file via ``wal_checkpoint(TRUNCATE)``. Without this step, the
   ``.db`` file would grow to the high-water mark of every burst it ever
   saw, even after rows are deleted; SQLite reuses freed pages but
   doesn't shrink the file on its own.

For compaction to work, fresh DBs are initialized with
``auto_vacuum=INCREMENTAL``. This setting can ONLY be enabled before any
table is created — pre-existing DBs from earlier versions of this module
will get a logged warning explaining the one-time manual migration::

    sqlite3 path/to/queue.db 'PRAGMA auto_vacuum=2; VACUUM;'

(That migration locks the DB for the duration of the VACUUM, which on a
multi-GB queue file can take seconds. Plan accordingly.)

The schema version is tracked via ``PRAGMA user_version``. If a future
version of this module bumps the schema, an old binary opening a newer
DB refuses to start rather than risk corruption.

Timestamps are always UTC (``time.time()``), never local time. This avoids
DST-related ambiguities in the reaper's cutoff comparison.

Concurrency
-----------

WAL mode allows concurrent readers and a single writer. Each thread opens
its own SQLite connection lazily; connections are cached in
``threading.local`` storage and reused across operations on the same
thread.

The producer queue is read by clients over TCP and written by clients
over TCP, so all access is serialized through the server's worker
threads. The consumer queue is also accessed only via the server. The
SQLite layer plus WAL plus per-row PK serialize correctly under load.

Multiple ``MyQueue`` server instances pointing at the same ``db_path``
will compete for the writer lock — workable but rarely what you want.
Don't put the DB on a network filesystem; SQLite is unhappy there.

Security
--------

Always construct with ``secret_key=secrets.token_bytes(32)`` (or longer).
With a key set, every frame is HMAC-SHA256 signed on send and verified on
receive. Without a key, anyone who can connect can inject arbitrary
messages; a loud warning is logged.

Signals
-------

Call ``install_signal_handlers()`` after ``start_server()`` to install
SIGTERM/SIGINT handlers that invoke ``stop_server()``. ``stop_server()``
does NOT need to flush data — every operation is already on disk.

The handlers are for shutting down sockets and joining threads cleanly.
They do nothing for SIGKILL, OOM kills, power loss, etc., which is fine
because data is already durable.

Public API
----------

``MyQueue(host, port, *, db_path=None, secret_key=None, ...)``
    Construct a queue. Server-side methods require ``db_path``.

``start_server()`` / ``stop_server()`` / ``install_signal_handlers()``
``wait_for_shutdown(timeout=None)``
    Server lifecycle.

``start_client()`` / ``close()``
    Client lifecycle.

``get_consumer()`` / ``get_producer()``
    Pop the next item. Raises ``queue.Empty`` if empty,
    ``ConnectionError`` on socket / protocol failure.

``send_to_consumer(blob)`` / ``send_to_producer(blob)``
    Push an item. ``send_to_consumer`` is server-side and writes
    directly to the DB; ``send_to_producer`` is client-side and goes
    over the wire.

``consumer_size()`` / ``producer_size()`` / ``consumer_empty()`` /
``producer_empty()`` / ``clear_queues()``
    Server-side introspection.

Migration notes (since the previous JSON+PEP 8 version)
-------------------------------------------------------

* Constructor takes new keyword args: ``db_path`` (path to SQLite file;
  required for any server-side use), ``ttl_seconds`` (None = no TTL),
  ``reaper_interval`` (default 60.0).
* The in-memory deques are gone. All server-side queue state lives in
  SQLite.
* New methods: ``install_signal_handlers``, ``wait_for_shutdown``.
* Server-side methods raise ``RuntimeError`` if ``db_path`` was not
  provided at construction time. Client-side methods do not require
  ``db_path``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import signal
import socket
import sqlite3
import threading
import time
import zlib
from contextlib import contextmanager
from pathlib import Path
from queue import Empty
from random import random
from time import sleep
from typing import Any, Iterator, Optional, Union

log = logging.getLogger("pyTCPQueue")


# Wire-protocol opcodes. Each opcode is a single byte. Using bytes >= 0x10
# guarantees these will never collide with the first byte of a zlib stream
# (always 0x78) nor with any printable ASCII character.
_OP_GET_CONSUMER: bytes = b"\x01"  # client -> server: give me a consumer item
_OP_GET_PRODUCER: bytes = b"\x02"  # client -> server: give me a producer item
_OP_PUT_PRODUCER: bytes = b"\x03"  # client -> server: enqueue payload to producer queue
_OP_ITEM: bytes = b"\x10"  # server -> client: here is an item (payload follows)
_OP_EMPTY: bytes = b"\x11"  # server -> client: queue was empty (no payload)

_HMAC_SIZE: int = 32  # HMAC-SHA256 digest length, in bytes
_MAX_HEADER_BYTES: int = 16  # generous cap on the ASCII length prefix

# Whitelist of valid table names. We do interpolate table names into SQL
# strings (sqlite3 placeholders don't work for table names), so the names
# come exclusively from this set.
_TABLES = ("consumer", "producer")

# Schema version stored in PRAGMA user_version. Bump when the schema changes
# and add a migration in _initialize_db. An old binary opening a DB whose
# schema is newer than it understands aborts rather than risk corruption.
_SCHEMA_VERSION = 1


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _close_socket(sock: Optional[socket.socket]) -> None:
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


def _serialize(blob: Any) -> bytes:
    """JSON-encode and zlib-compress an arbitrary JSON-serializable object.

    Raises ``TypeError`` if ``blob`` contains values JSON cannot represent
    (bytes, sets, custom objects, etc.). Tuples become lists and integer
    dict keys become strings on round-trip — see module docstring."""
    encoded = json.dumps(blob, separators=(",", ":"), ensure_ascii=False)
    return zlib.compress(encoded.encode("utf-8"), 1)


def _deserialize(data: bytes) -> Any:
    """Inverse of ``_serialize``. Raises ``ValueError`` on malformed input."""
    try:
        return json.loads(zlib.decompress(data).decode("utf-8"))
    except (zlib.error, UnicodeDecodeError, json.JSONDecodeError) as ex:
        raise ValueError("malformed payload: %s" % ex) from ex


class _Frame:
    """Frame I/O helpers. Stateless; just a namespace."""

    @staticmethod
    def write(
        sock: socket.socket,
        opcode: bytes,
        payload: bytes,
        secret_key: Optional[bytes],
    ) -> None:
        body = opcode + payload
        if secret_key:
            body += hmac.new(secret_key, body, hashlib.sha256).digest()
        sock.sendall(str(len(body)).encode() + b":" + body)

    @staticmethod
    def read(
        sock: socket.socket, secret_key: Optional[bytes]
    ) -> Optional[tuple[bytes, bytes]]:
        """Return ``(opcode, payload)`` or ``None`` if the peer closed the
        connection cleanly with no data buffered. Raises ``ValueError`` for
        any malformed or unauthenticated frame."""
        # Read the ASCII length header up to and including the colon. We
        # buffer rather than recv-ing one byte at a time.
        header = bytearray()
        while b":" not in header:
            remaining = _MAX_HEADER_BYTES - len(header)
            if remaining <= 0:
                raise ValueError(
                    "header exceeds %d bytes; no colon found" % _MAX_HEADER_BYTES
                )
            chunk = sock.recv(remaining)
            if not chunk:
                if not header:
                    return None  # clean close before any data arrived
                raise ValueError("connection closed mid-header")
            header.extend(chunk)

        colon_idx = header.index(b":")
        length_str = bytes(header[:colon_idx])
        body_so_far = bytearray(header[colon_idx + 1 :])

        try:
            length = int(length_str)
        except ValueError:
            raise ValueError("non-numeric length: %r" % length_str)
        if length < 1:
            raise ValueError("frame too short: length=%d" % length)

        while len(body_so_far) < length:
            chunk = sock.recv(length - len(body_so_far))
            if not chunk:
                raise ValueError("connection closed mid-body")
            body_so_far.extend(chunk)

        body = bytes(body_so_far)

        if secret_key:
            if len(body) < _HMAC_SIZE + 1:
                raise ValueError("frame too short for HMAC")
            msg, mac = body[:-_HMAC_SIZE], body[-_HMAC_SIZE:]
            expected = hmac.new(secret_key, msg, hashlib.sha256).digest()
            if not hmac.compare_digest(mac, expected):
                raise ValueError("HMAC verification failed")
            body = msg

        return body[:1], body[1:]


# --------------------------------------------------------------------------- #
# MyQueue                                                                     #
# --------------------------------------------------------------------------- #


class MyQueue:
    """Bidirectional TCP message queue with on-disk durability."""

    def _initialize_db(self) -> None:
        """Create tables and indexes if they don't exist. Idempotent.

        On a fresh DB, sets ``auto_vacuum=INCREMENTAL`` so deletes can later
        be reclaimed back to the OS by ``_db_compact()``. ``auto_vacuum``
        can only be enabled before the first table is created; for
        pre-existing DBs without it, logs a one-time migration hint.

        Verifies the on-disk schema version isn't newer than what this code
        understands (which would risk silent corruption), and stamps the
        version on a fresh DB."""
        # Open a dedicated connection for setup, separate from _open_db's
        # thread-local cache. We need direct pragma access outside a txn.
        conn = sqlite3.connect(self._db_path, isolation_level=None, timeout=30.0)
        try:
            (current_version,) = conn.execute("PRAGMA user_version").fetchone()
            if current_version > _SCHEMA_VERSION:
                raise RuntimeError(
                    f"Database at {self._db_path} has schema version "
                    f"{current_version}, newer than this code supports "
                    f"({_SCHEMA_VERSION}). Refusing to open to avoid "
                    f"corruption."
                )

            existing_tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name IN ('consumer', 'producer')"
                )
            }
            is_fresh = current_version == 0 and not existing_tables

            if is_fresh:
                # auto_vacuum=INCREMENTAL takes effect only on a virgin DB.
                # Once set, subsequent deletes free pages to a tracked list
                # which `PRAGMA incremental_vacuum` can later return to OS.
                conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
            else:
                (av_mode,) = conn.execute("PRAGMA auto_vacuum").fetchone()
                if av_mode != 2:  # 2 == INCREMENTAL
                    log.warning(
                        "Database at %s lacks auto_vacuum=INCREMENTAL. "
                        "The file will not shrink after deletes — "
                        "free pages will be reused but not returned to "
                        "the OS. To enable (this locks the DB; runtime "
                        "depends on size): "
                        "sqlite3 %s 'PRAGMA auto_vacuum=2; VACUUM;'",
                        self._db_path,
                        self._db_path,
                    )

            # Set durability / mode pragmas. journal_mode=WAL is persistent
            # in the DB file but the others are per-connection; harmless to
            # re-set here.
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")

            # Create the schema in a transaction so a partial failure leaves
            # nothing half-built.
            conn.execute("BEGIN IMMEDIATE")
            try:
                for table in _TABLES:
                    conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table} (
                            id         INTEGER PRIMARY KEY AUTOINCREMENT,
                            payload    BLOB NOT NULL,
                            created_at REAL NOT NULL
                        )
                    """)
                    conn.execute(f"""
                        CREATE INDEX IF NOT EXISTS {table}_created_at_idx
                        ON {table}(created_at)
                    """)
                # PRAGMA user_version doesn't accept a bound parameter;
                # literal interpolation of an int constant is safe.
                conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 49152,
        *,
        db_path: Optional[Union[str, Path]] = None,
        secret_key: Optional[bytes] = None,
        max_queue_size: int = 10_000,
        ttl_seconds: Optional[float] = None,
        reaper_interval: float = 60.0,
        timeout: float = 75.0,
    ) -> None:
        if not isinstance(host, str):
            raise ValueError("host must be a string, got %s" % type(host).__name__)
        if not isinstance(port, int) or not (1 <= port <= 65535):
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
        if max_queue_size < 1:
            raise ValueError("max_queue_size must be >= 1")
        if ttl_seconds is not None and ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0 or None")
        if reaper_interval <= 0:
            raise ValueError("reaper_interval must be > 0")
        if timeout <= 0:
            raise ValueError("timeout must be > 0")

        self._addr: tuple[str, int] = (host, port)
        self._db_path: Optional[str] = str(db_path) if db_path is not None else None
        self._secret_key: Optional[bytes] = (
            bytes(secret_key) if secret_key is not None else None
        )
        self._max_queue_size: int = max_queue_size
        self._ttl_seconds: Optional[float] = ttl_seconds
        self._reaper_interval: float = reaper_interval
        self._timeout: float = timeout

        # Thread-local SQLite connections — one per thread, lazily opened.
        self._tls = threading.local()

        # Server-side runtime state.
        self._workers: list[socket.socket] = []
        self._workers_lock = threading.Lock()
        self._worker_threads: list[threading.Thread] = []
        self._worker_threads_lock = threading.Lock()
        self._shutdown = threading.Event()
        self._ssock: Optional[socket.socket] = None
        self._listen_thread: Optional[threading.Thread] = None
        self._maintenance_thread: Optional[threading.Thread] = None

        # Client-side state.
        self._csock: Optional[socket.socket] = None
        self._csock_lock = threading.Lock()

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

    # ------------------------------------------------------------------ #
    # SQLite layer                                                       #
    # ------------------------------------------------------------------ #

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

        conn = sqlite3.connect(self._db_path, isolation_level=None, timeout=30.0)
        # WAL mode is persistent in the DB file, but we re-set it because
        # this might be the first open. NORMAL synchronous gives us
        # excellent durability with much higher throughput than FULL.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        self._tls.conn = conn
        self._tls.path = self._db_path
        return conn

    @contextmanager
    def _db_txn(self) -> Iterator[sqlite3.Connection]:
        """Context manager that opens a transaction, commits on success,
        rolls back on exception."""
        conn = self._open_db()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        else:
            conn.execute("COMMIT")

    def _db_compact(self) -> None:
        """Reclaim free pages back to the OS and truncate the WAL file.

        Order matters: we checkpoint first to flush WAL frames into the
        main DB (so freed pages are visible to ``incremental_vacuum``),
        then vacuum, then checkpoint again to flush the vacuum's own
        writes and shrink the WAL file.

        ``incremental_vacuum`` returns one row per page being reclaimed
        and the actual work only happens as the cursor is iterated, so
        ``fetchall()`` is required (a bare ``execute()`` reclaims exactly
        one page regardless of the limit). We cap each pass at 1000
        pages (~4MB at default page size) to bound how long any single
        compaction call can take."""
        if self._db_path is None:
            return
        try:
            conn = self._open_db()
            # 1. Flush WAL into main DB so vacuum sees the freed pages.
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
            # 2. Reclaim free pages. Drain the cursor — each row is one
            #    page actually being returned to the OS.
            reclaimed = conn.execute("PRAGMA incremental_vacuum(1000)").fetchall()
            # 3. Truncate the WAL again to flush vacuum's own bookkeeping.
            ckpt = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if reclaimed:
                log.debug(
                    "DB compact: reclaimed %d pages, WAL log=%d ckpt=%d busy=%d",
                    len(reclaimed),
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

    def _enqueue(self, table: str, payload: bytes) -> None:
        """Insert one row, evicting the oldest if we're at capacity."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        now = time.time()
        with self._db_txn() as conn:
            (count,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            if count >= self._max_queue_size:
                log.warning(
                    "%s queue at capacity (%d); dropping oldest",
                    table,
                    self._max_queue_size,
                )
                conn.execute(
                    f"DELETE FROM {table} "
                    f"WHERE id = (SELECT id FROM {table} ORDER BY id LIMIT 1)"
                )
            conn.execute(
                f"INSERT INTO {table} (payload, created_at) VALUES (?, ?)",
                (payload, now),
            )

    def _dequeue(self, table: str) -> Optional[bytes]:
        """Atomically pop the oldest row's payload. Returns None if empty.
        Uses ``DELETE ... RETURNING`` (SQLite >= 3.35, March 2021)."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        with self._db_txn() as conn:
            row = conn.execute(
                f"DELETE FROM {table} "
                f"WHERE id = (SELECT id FROM {table} ORDER BY id LIMIT 1) "
                f"RETURNING payload"
            ).fetchone()
            return row[0] if row else None

    def _table_size(self, table: str) -> int:
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        with self._db_txn() as conn:
            (count,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            return count

    def _table_empty(self, table: str) -> bool:
        """O(1) emptiness check, unlike COUNT(*)."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        with self._db_txn() as conn:
            row = conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
            return row is None

    def _table_clear(self, table: str) -> None:
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        with self._db_txn() as conn:
            conn.execute(f"DELETE FROM {table}")

    def _table_peek(self, table: str) -> Optional[bytes]:
        """Return the oldest payload without removing it. None if empty."""
        if table not in _TABLES:
            raise ValueError("unknown table: %s" % table)
        with self._db_txn() as conn:
            row = conn.execute(
                f"SELECT payload FROM {table} ORDER BY id LIMIT 1"
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

    def _respond_with_item(self, sock: socket.socket, table: str) -> None:
        item = self._dequeue(table)
        if item is None:
            _Frame.write(sock, _OP_EMPTY, b"", self._secret_key)
        else:
            _Frame.write(sock, _OP_ITEM, item, self._secret_key)

    def _serve_connection(self, client_sock: socket.socket) -> None:
        """Handle a single accepted connection until close or shutdown."""
        try:
            while not self._shutdown.is_set():
                try:
                    frame = _Frame.read(client_sock, self._secret_key)
                except ValueError as ex:
                    log.warning("Closing connection: %s", ex)
                    return
                if frame is None:
                    return
                opcode, payload = frame

                if opcode == _OP_GET_CONSUMER:
                    self._respond_with_item(client_sock, "consumer")
                elif opcode == _OP_GET_PRODUCER:
                    self._respond_with_item(client_sock, "producer")
                elif opcode == _OP_PUT_PRODUCER:
                    self._enqueue("producer", payload)
                else:
                    log.warning(
                        "Unknown opcode 0x%02x; closing connection",
                        opcode[0] if opcode else 0,
                    )
                    return
        except (ConnectionError, OSError) as ex:
            log.debug("Connection closed: %s", ex)
        except Exception:
            log.exception("Unexpected error in server connection thread")
        finally:
            with self._workers_lock:
                try:
                    self._workers.remove(client_sock)
                except ValueError:
                    pass
            _close_socket(client_sock)

    def _controlling_loop(self) -> None:
        """Accept incoming connections and spawn a per-connection thread."""
        while not self._shutdown.is_set():
            try:
                client_sock, _addr = self._ssock.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._shutdown.is_set():
                    return
                log.exception("accept() failed")
                sleep(0.5)
                continue
            except Exception:
                log.exception("Unexpected error in accept loop")
                sleep(0.5)
                continue

            try:
                client_sock.settimeout(self._timeout)
                client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError as ex:
                log.warning("Could not configure accepted socket: %s", ex)

            with self._workers_lock:
                self._workers.append(client_sock)
            t = threading.Thread(
                target=self._serve_connection,
                args=(client_sock,),
                name="tcpQueue-worker",
                daemon=True,
            )
            with self._worker_threads_lock:
                self._worker_threads.append(t)
            t.start()

    def _maintenance_loop(self) -> None:
        """Periodic background maintenance: thread reaping, TTL reaping,
        WAL checkpointing."""
        while not self._shutdown.wait(self._reaper_interval):
            # 1. Drop references to finished worker threads.
            with self._worker_threads_lock:
                self._worker_threads = [t for t in self._worker_threads if t.is_alive()]

            # 2. Delete rows whose created_at is past the TTL cutoff.
            if self._ttl_seconds is not None:
                try:
                    deleted = self._reap_expired()
                    for table, n in deleted.items():
                        log.info("Reaped %d expired entries from %s", n, table)
                except Exception:
                    log.exception("TTL reaper iteration failed")

            # 3. Compact the DB: return free pages to the OS, truncate the
            # WAL. Replaces the previous PASSIVE checkpoint — that kept the
            # WAL functionally bounded but didn't shrink either the main DB
            # file or the WAL file on disk.
            try:
                self._db_compact()
            except Exception:
                log.exception("DB compaction failed")

    # ------------------------------------------------------------------ #
    # Server lifecycle                                                   #
    # ------------------------------------------------------------------ #

    def start_server(self) -> None:
        """Bind, listen, and start accepting connections in the background.
        Requires ``db_path`` to have been set at construction."""
        if self._db_path is None:
            raise RuntimeError("start_server requires db_path to be set")
        if self._ssock is not None:
            raise RuntimeError("server already started")

        ssock: Optional[socket.socket] = None
        try:
            ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            ssock.bind(self._addr)
            ssock.listen(128)
            ssock.settimeout(1.0)  # let the accept loop poll the shutdown flag
        except Exception:
            _close_socket(ssock)
            raise

        self._ssock = ssock
        self._shutdown.clear()
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
        """Stop accepting connections, close all server-side sockets, and
        join background threads. Idempotent. No data flushing is needed —
        every operation is already on disk."""
        self._shutdown.set()

        _close_socket(self._ssock)
        self._ssock = None

        with self._workers_lock:
            workers = list(self._workers)
            self._workers.clear()
        for w in workers:
            _close_socket(w)

        for t in (self._listen_thread, self._maintenance_thread):
            if t is not None and t.is_alive():
                t.join(timeout=5.0)
        self._listen_thread = None
        self._maintenance_thread = None

        with self._worker_threads_lock:
            threads = list(self._worker_threads)
            self._worker_threads.clear()
        for t in threads:
            t.join(timeout=1.0)

    def install_signal_handlers(
        self,
        signals: Optional[tuple[int, ...]] = None,
    ) -> None:
        """Install handlers that call ``stop_server()`` on the given signals.

        Defaults to SIGTERM and SIGINT (whichever are available on this
        platform). Must be called from the main thread (Python only allows
        ``signal.signal`` from main).

        The handler is for clean teardown, NOT for data preservation:
        every operation is already durable on disk.
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
                signal.signal(sig, handler)
            except (ValueError, OSError) as ex:
                log.warning("Could not install handler for signal %d: %s", sig, ex)

    def wait_for_shutdown(self, timeout: Optional[float] = None) -> bool:
        """Block until ``stop_server()`` is invoked (e.g. by a signal
        handler). Returns ``True`` if shutdown happened, ``False`` on
        timeout."""
        return self._shutdown.wait(timeout=timeout)

    def _connect_locked(self) -> Optional[socket.socket]:
        """Open a new client socket. Caller MUST hold self._csock_lock."""
        sock: Optional[socket.socket] = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self._timeout)
            sock.connect(self._addr)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return sock
        except Exception as ex:
            log.error("Unable to connect to %s:%d: %s", *self._addr, ex)
            _close_socket(sock)
            return None

    # ------------------------------------------------------------------ #
    # Client side                                                        #
    # ------------------------------------------------------------------ #

    def start_client(self) -> None:
        """Connect to the server. Idempotent."""
        with self._csock_lock:
            if self._csock is not None:
                _close_socket(self._csock)
                self._csock = None
            self._csock = self._connect_locked()

    def close(self) -> None:
        """Close the client connection."""
        with self._csock_lock:
            _close_socket(self._csock)
            self._csock = None

    def _ensure_connected_locked(self) -> Optional[socket.socket]:
        if self._csock is None:
            self._csock = self._connect_locked()
        return self._csock

    def _get(self, opcode: bytes) -> Any:
        with self._csock_lock:
            sock = self._ensure_connected_locked()
            if sock is None:
                raise ConnectionError("not connected")
            try:
                _Frame.write(sock, opcode, b"", self._secret_key)
                frame = _Frame.read(sock, self._secret_key)
            except (OSError, ValueError) as ex:
                _close_socket(self._csock)
                self._csock = None
                raise ConnectionError(str(ex)) from ex

            if frame is None:
                _close_socket(self._csock)
                self._csock = None
                raise ConnectionError("server closed connection")

            resp_opcode, payload = frame
            if resp_opcode == _OP_EMPTY:
                raise Empty()
            if resp_opcode == _OP_ITEM:
                try:
                    return _deserialize(payload)
                except ValueError as ex:
                    raise ConnectionError(str(ex)) from ex
            raise ConnectionError("unexpected response opcode 0x%02x" % resp_opcode[0])

    def get_consumer(self) -> Any:
        """Pop the next item from the consumer queue.

        Raises ``queue.Empty`` if the queue is empty.
        Raises ``ConnectionError`` on socket / protocol failure."""
        return self._get(_OP_GET_CONSUMER)

    def get_producer(self) -> Any:
        """Pop the next item from the producer queue.

        Raises ``queue.Empty`` if the queue is empty.
        Raises ``ConnectionError`` on socket / protocol failure."""
        return self._get(_OP_GET_PRODUCER)

    def send_to_producer(self, blob: Any) -> bool:
        """Send `blob` to the producer queue. Retries up to 3 times with
        random back-off, then drops the message. Returns ``True`` on
        success, ``False`` if the message was dropped.

        Raises ``TypeError`` immediately if `blob` is not JSON-serializable."""
        payload = _serialize(blob)

        for attempt in range(1, 4):
            with self._csock_lock:
                sock = self._ensure_connected_locked()
                if sock is not None:
                    try:
                        _Frame.write(sock, _OP_PUT_PRODUCER, payload, self._secret_key)
                        return True
                    except Exception as ex:
                        log.warning("Send failed (attempt %d/3): %s", attempt, ex)
                        _close_socket(self._csock)
                        self._csock = None
            sleep(random() * 2)

        log.error("Dropping message after 3 failed send attempts")
        return False

    def send_to_consumer(self, blob: Any) -> None:
        """Push `blob` into the consumer queue (server side, direct DB
        write). Requires ``db_path`` to be set.

        Raises ``TypeError`` if `blob` is not JSON-serializable."""
        if self._db_path is None:
            raise RuntimeError("send_to_consumer requires db_path to be set")
        payload = _serialize(blob)
        self._enqueue("consumer", payload)

    # ------------------------------------------------------------------ #
    # Introspection (server side)                                        #
    # ------------------------------------------------------------------ #

    def clear_queues(self) -> None:
        """Clear both queues. Server side only."""
        for table in _TABLES:
            self._table_clear(table)

    def consumer_size(self) -> int:
        return self._table_size("consumer")

    def producer_size(self) -> int:
        return self._table_size("producer")

    def consumer_empty(self) -> bool:
        return self._table_empty("consumer")

    def producer_empty(self) -> bool:
        return self._table_empty("producer")

    def peek_consumer(self) -> Any:
        """Return the next consumer item without removing it. Server side only.
        Raises ``queue.Empty`` if empty."""
        payload = self._table_peek("consumer")
        if payload is None:
            raise Empty()
        return _deserialize(payload)

    def peek_producer(self) -> Any:
        """Return the next producer item without removing it. Server side only.
        Raises ``queue.Empty`` if empty."""
        payload = self._table_peek("producer")
        if payload is None:
            raise Empty()
        return _deserialize(payload)

    def __repr__(self) -> str:
        host, port = self._addr
        return "MyQueue(host=%r, port=%d, db_path=%r, secret_key=%s)" % (
            host,
            port,
            self._db_path,
            "<set>" if self._secret_key else "None",
        )
