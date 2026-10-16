"""Regression tests for tcpQueue. Exercises:
- HMAC-authenticated round trip via SQLite
- Sentinel collision fix ([] / None as legitimate data)
- Empty exception on empty queue
- Bad HMAC rejection
- Clean stop_server() with active connections
- Thread-safe client
- JSON: TypeError on bytes / sets, tuple round-trips as list
- Persistence across server restart
- TTL reaper deletes old rows
- Max queue size evicts oldest / rejects (full_policy)
- Signal handler triggers shutdown, and can be uninstalled
- Schema versioning, v1 -> v2 migration, auto_vacuum, compaction, WAL
- Durability pragma (synchronous) default and override
- Lease / ack / release (at-least-once) incl. expiry and stale tokens
- Long-polling GET / LEASE (wait=) incl. wake on insert and on shutdown
- Error frames keep the connection; protocol errors close it with the
  peer address logged
- max_connections, TCP keepalive, IPv6
- Buffered frame reader: back-to-back frames, mid-frame stall vs idle,
  large payloads, tampered HMAC
- Decompression bomb cap, poison payloads dropped, oversized payloads
  rejected without retries
- ~ expansion in db_path, :memory: rejected, BaseException rollback
"""

import hashlib
import hmac
import logging
import os
import secrets
import shutil
import signal
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import zlib
from contextlib import contextmanager
from pathlib import Path
from queue import Empty, Full
from time import sleep

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tcpQueue
from tcpQueue import (
    _OP_EMPTY,
    _OP_ERROR,
    _OP_GET_CONSUMER,
    _OP_ITEM,
    _OP_PUT_PRODUCER,
    Lease,
    MyQueue,
    PayloadError,
    ProtocolError,
    RemoteError,
    _deserialize,
    _Frame,
    _FrameReader,
)

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

KEY = secrets.token_bytes(32)


class _PortBox:
    """Hands out a fresh port number for each test so they don't collide."""

    def __init__(self, start=49200):
        self.next = start

    def get(self):
        p = self.next
        self.next += 1
        return p


PORTS = _PortBox()


class _DbBox:
    """Per-test temp directory for SQLite files. Cleaned up at end."""

    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="tcpqueue_test_"))

    def path(self, name="queue.db"):
        return self.root / name

    def cleanup(self):
        shutil.rmtree(self.root, ignore_errors=True)


@contextmanager
def running_server(db=None, **kwargs):
    """Start a server on a fresh port (and DB unless one is given); yield
    ``(server, port)``; stop and clean up afterwards."""
    db = db or _DbBox()
    port = PORTS.get()
    kwargs.setdefault("secret_key", KEY)
    server = MyQueue("127.0.0.1", port, db_path=db.path(), **kwargs)
    server.start_server()
    sleep(0.1)
    try:
        yield server, port
    finally:
        server.stop_server()
        db.cleanup()


def make_client(port, **kwargs):
    kwargs.setdefault("secret_key", KEY)
    return MyQueue("127.0.0.1", port, **kwargs)


def _raw_frame(opcode, payload, key=KEY):
    """Build a wire frame by hand, independently of _Frame.write."""
    body = opcode + payload
    if key:
        body += hmac.new(key, body, hashlib.sha256).digest()
    return str(len(body)).encode("ascii") + b":" + body


def _raw_connect(port, timeout=5.0):
    sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    return sock, _FrameReader(sock, KEY)


# --------------------------------------------------------------------------- #
# Original smoke tests                                                        #
# --------------------------------------------------------------------------- #


def test_basic_roundtrip():
    with running_server() as (server, port):
        for i in range(5):
            server.send_to_consumer({"i": i, "msg": f"hello {i}"})
        assert server.consumer_size() == 5

        client = make_client(port)
        client.start_client()
        results = []
        try:
            while True:
                results.append(client.get_consumer())
        except Empty:
            pass
        client.close()

        assert len(results) == 5
        assert results[0] == {"i": 0, "msg": "hello 0"}


def test_sentinel_collision_fix():
    with running_server() as (server, port):
        tricky = [[], None, 0, "", False, [None], {}]
        for v in tricky:
            server.send_to_consumer(v)

        client = make_client(port)
        client.start_client()
        results = [client.get_consumer() for _ in tricky]
        with pytest.raises(Empty):
            client.get_consumer()
        client.close()

        assert results == tricky


def test_producer_path():
    with running_server() as (_server, port):
        client = make_client(port)
        client.start_client()
        for i in range(3):
            assert client.send_to_producer(f"result {i}")
        client.close()

        reader = make_client(port)
        reader.start_client()
        results = []
        try:
            while True:
                results.append(reader.get_producer())
        except Empty:
            pass
        reader.close()

        assert results == ["result 0", "result 1", "result 2"]


def test_bad_hmac_rejected():
    with running_server() as (server, port):
        server.send_to_consumer("secret data")
        client = make_client(port, secret_key=secrets.token_bytes(32))
        client.start_client()
        try:
            with pytest.raises(ConnectionError):
                client.get_consumer()
        finally:
            client.close()
        assert server.consumer_size() == 1, "item must not be consumed by a bad key"


def test_clean_shutdown():
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)

    clients = []
    for _ in range(3):
        c = make_client(port)
        c.start_client()
        clients.append(c)

    sleep(0.1)
    server.stop_server()
    for c in clients:
        c.close()
    db.cleanup()


def test_thread_safety():
    with running_server() as (server, port):
        client = make_client(port)
        client.start_client()

        n_threads, n_per = 5, 50
        errors: list[Exception] = []

        def worker(tid: int):
            try:
                for i in range(n_per):
                    client.send_to_producer([tid, i])
            except Exception as ex:
                errors.append(ex)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        client.close()

        assert not errors, f"errors: {errors}"
        assert server.producer_size() == n_threads * n_per


def test_json_unsupported_types():
    with running_server() as (server, _port):
        for bad in [b"raw bytes", {1, 2, 3}, frozenset([1])]:
            with pytest.raises(TypeError):
                server.send_to_consumer(bad)


def test_tuple_becomes_list():
    with running_server() as (server, port):
        server.send_to_consumer((1, 2, 3))
        client = make_client(port)
        result = client.get_consumer()
        client.close()
        assert result == [1, 2, 3] and isinstance(result, list)


def test_persistence_across_restart():
    """The whole point of on-disk SQLite: data survives the server going
    away. Push some items, stop the server entirely, start a brand-new
    one against the same DB, drain it."""
    db = _DbBox()
    port = PORTS.get()
    db_path = db.path()
    try:
        server1 = MyQueue("127.0.0.1", port, db_path=db_path, secret_key=KEY)
        server1.start_server()
        sleep(0.1)
        for i in range(10):
            server1.send_to_consumer({"id": i})
        assert server1.consumer_size() == 10
        server1.stop_server()
        del server1
        sleep(0.1)

        server2 = MyQueue("127.0.0.1", port, db_path=db_path, secret_key=KEY)
        assert server2.consumer_size() == 10
        server2.start_server()
        sleep(0.1)

        client = make_client(port)
        ids = []
        try:
            while True:
                ids.append(client.get_consumer()["id"])
        except Empty:
            pass
        client.close()

        assert ids == list(range(10)), f"got {ids}"
        server2.stop_server()
    finally:
        db.cleanup()


def test_ttl_reaper():
    with running_server(ttl_seconds=1.0, reaper_interval=0.3) as (server, _port):
        for i in range(5):
            server.send_to_consumer({"i": i})
        assert server.consumer_size() == 5
        sleep(1.6)
        assert server.consumer_size() == 0


def test_max_queue_size_eviction():
    with running_server(max_queue_size=3) as (server, port):
        for i in range(5):
            server.send_to_consumer({"i": i})
        assert server.consumer_size() == 3

        client = make_client(port)
        ids = []
        try:
            while True:
                ids.append(client.get_consumer()["i"])
        except Empty:
            pass
        client.close()
        assert ids == [2, 3, 4], f"got {ids}"


def test_signal_handler_triggers_shutdown():
    """Send ourselves SIGUSR1 (a custom signal so we don't disrupt the test
    runner). Verify stop_server runs and wait_for_shutdown returns."""
    sig = getattr(signal, "SIGUSR1", None)
    if sig is None:
        pytest.skip("SIGUSR1 not available on this platform")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)
    try:
        server.install_signal_handlers(signals=(sig,))

        def fire():
            sleep(0.2)
            os.kill(os.getpid(), sig)

        threading.Thread(target=fire, daemon=True).start()
        assert server.wait_for_shutdown(timeout=5.0), "shutdown did not fire"
        assert server._ssock is None, "stop_server did not run"
    finally:
        server.uninstall_signal_handlers()
        server.stop_server()
        db.cleanup()


def test_peek_does_not_dequeue():
    with running_server() as (server, _port):
        for i in range(3):
            server.send_to_consumer({"i": i})
        a = server.peek_consumer()
        b = server.peek_consumer()
        assert a == b == {"i": 0}
        assert server.consumer_size() == 3
        server.clear_queues()
        with pytest.raises(Empty):
            server.peek_consumer()


def test_schema_version_check():
    """A DB stamped with a higher schema version should be refused."""
    db = _DbBox()
    try:
        conn = sqlite3.connect(str(db.path()))
        conn.execute(f"PRAGMA user_version = {tcpQueue._SCHEMA_VERSION + 999}")
        conn.commit()
        conn.close()
        with pytest.raises(RuntimeError, match="newer than this code supports"):
            MyQueue("127.0.0.1", PORTS.get(), db_path=db.path(), secret_key=KEY)
    finally:
        db.cleanup()


def test_schema_version_stamp():
    db = _DbBox()
    try:
        q = MyQueue("127.0.0.1", PORTS.get(), db_path=db.path(), secret_key=KEY)
        q.close_db()
        conn = sqlite3.connect(str(db.path()))
        (v,) = conn.execute("PRAGMA user_version").fetchone()
        conn.close()
        assert v == tcpQueue._SCHEMA_VERSION
    finally:
        db.cleanup()


def test_autovacuum_set_on_fresh_db():
    db = _DbBox()
    try:
        q = MyQueue("127.0.0.1", PORTS.get(), db_path=db.path(), secret_key=KEY)
        q.close_db()
        conn = sqlite3.connect(str(db.path()))
        (av,) = conn.execute("PRAGMA auto_vacuum").fetchone()
        conn.close()
        assert av == 2, f"expected auto_vacuum=2 (INCREMENTAL), got {av}"
    finally:
        db.cleanup()


def test_compaction_shrinks_db_file():
    """After pushing big payloads, draining, and compacting, the .db file
    should shrink dramatically."""
    db = _DbBox()
    db_path = db.path()
    with running_server(db=db) as (server, _port):
        for i in range(1000):
            server.send_to_consumer({"i": i, "data": os.urandom(500).hex()})
        server.compact()
        size_full = os.path.getsize(db_path)

        server.clear_queues()
        server.compact()
        size_compact = os.path.getsize(db_path)

        assert (
            size_compact < size_full * 0.10
        ), f"file didn't shrink enough: {size_full:,} -> {size_compact:,}"


def test_wal_truncate():
    db = _DbBox()
    db_path = db.path()
    with running_server(db=db) as (server, _port):
        for i in range(500):
            server.send_to_consumer({"i": i, "data": "x" * 500})
        server.compact()
        wal_path = str(db_path) + "-wal"
        wal_after = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
        assert wal_after < 100, f"WAL not truncated: {wal_after:,} bytes"


def test_legacy_db_warns(caplog):
    """Opening a pre-existing DB without auto_vacuum should log a warning
    explaining the manual migration."""
    db = _DbBox()
    try:
        conn = sqlite3.connect(str(db.path()))
        for table in ("consumer", "producer"):
            conn.execute(
                f"CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "payload BLOB, created_at REAL)"
            )
        conn.commit()
        conn.close()

        caplog.set_level(logging.WARNING, logger=tcpQueue.log.name)
        q = MyQueue("127.0.0.1", PORTS.get(), db_path=db.path(), secret_key=KEY)
        q.close_db()

        relevant = [
            r.getMessage() for r in caplog.records if "auto_vacuum" in r.getMessage()
        ]
        assert relevant, f"no auto_vacuum warning: {caplog.text}"
        assert "VACUUM" in relevant[0]
    finally:
        db.cleanup()


# --------------------------------------------------------------------------- #
# Durability, paths, schema                                                   #
# --------------------------------------------------------------------------- #


def test_synchronous_defaults_to_full_and_is_configurable():
    db = _DbBox()
    try:
        q = MyQueue("127.0.0.1", PORTS.get(), db_path=db.path(), secret_key=KEY)
        (sync,) = q._open_db().execute("PRAGMA synchronous").fetchone()
        q.close_db()
        assert sync == 2, "default must be FULL (2) for power-loss durability"

        q = MyQueue(
            "127.0.0.1",
            PORTS.get(),
            db_path=db.path("normal.db"),
            secret_key=KEY,
            synchronous="normal",
        )
        (sync,) = q._open_db().execute("PRAGMA synchronous").fetchone()
        q.close_db()
        assert sync == 1

        with pytest.raises(ValueError):
            MyQueue("127.0.0.1", PORTS.get(), db_path=db.path(), synchronous="OFF")
    finally:
        db.cleanup()


def test_wal_mode_enabled():
    db = _DbBox()
    try:
        q = MyQueue("127.0.0.1", PORTS.get(), db_path=db.path(), secret_key=KEY)
        q.close_db()
        conn = sqlite3.connect(str(db.path()))
        (mode,) = conn.execute("PRAGMA journal_mode").fetchone()
        conn.close()
        assert mode.lower() == "wal"
    finally:
        db.cleanup()


def test_sqlite_version_check(monkeypatch):
    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 34, 0))
    db = _DbBox()
    try:
        with pytest.raises(RuntimeError, match="too old"):
            MyQueue("127.0.0.1", PORTS.get(), db_path=db.path(), secret_key=KEY)
    finally:
        db.cleanup()


def test_tilde_in_db_path_is_expanded(monkeypatch):
    home = Path(tempfile.mkdtemp(prefix="tcpqueue_home_"))
    monkeypatch.setenv("HOME", str(home))
    try:
        q = MyQueue("127.0.0.1", PORTS.get(), db_path="~/sub/q.db", secret_key=KEY)
        assert (home / "sub" / "q.db").exists()
        q.send_to_consumer("x")
        assert q.consumer_size() == 1
        q.close_db()
    finally:
        shutil.rmtree(home, ignore_errors=True)


@pytest.mark.parametrize("bad", [":memory:", "", "file:q.db?mode=memory"])
def test_in_memory_db_path_rejected(bad):
    with pytest.raises(ValueError, match="not supported"):
        MyQueue("127.0.0.1", PORTS.get(), db_path=bad, secret_key=KEY)


def test_schema_v1_migrates_to_v2():
    """A v1 database (no lease columns) is upgraded in place and its rows
    stay readable, in order, through both get and lease."""
    db = _DbBox()
    try:
        conn = sqlite3.connect(str(db.path()))
        for table in ("consumer", "producer"):
            conn.execute(
                f"CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "payload BLOB NOT NULL, created_at REAL NOT NULL)"
            )
            conn.execute(f"CREATE INDEX {table}_created_at_idx ON {table}(created_at)")
        conn.executemany(
            "INSERT INTO consumer (payload, created_at) VALUES (?, ?)",
            [(tcpQueue._serialize({"old": i}), 1.0) for i in range(2)],
        )
        conn.execute("PRAGMA user_version = 1")
        conn.commit()
        conn.close()

        with running_server(db=db) as (server, port):
            conn = sqlite3.connect(str(db.path()))
            cols = {row[1] for row in conn.execute("PRAGMA table_info(consumer)")}
            (version,) = conn.execute("PRAGMA user_version").fetchone()
            conn.close()
            assert {"leased_until", "lease_token"} <= cols
            assert version == tcpQueue._SCHEMA_VERSION
            assert server.consumer_size() == 2

            client = make_client(port)
            lease = client.lease_consumer()
            assert lease.item == {"old": 0}
            assert client.ack(lease)
            assert client.get_consumer() == {"old": 1}
            client.close()
    finally:
        db.cleanup()


def test_db_txn_rolls_back_on_base_exception():
    db = _DbBox()
    try:
        q = MyQueue("127.0.0.1", PORTS.get(), db_path=db.path(), secret_key=KEY)
        with pytest.raises(KeyboardInterrupt), q._db_txn() as conn:
            conn.execute(
                "INSERT INTO consumer (payload, created_at) VALUES (?, ?)",
                (b"x", 0.0),
            )
            raise KeyboardInterrupt
        assert q.consumer_size() == 0, "insert must have been rolled back"
        q.send_to_consumer("ok")  # connection must not be stuck in a transaction
        assert q.consumer_size() == 1
        q.close_db()
    finally:
        db.cleanup()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"lease_seconds": 0},
        {"max_connections": 0},
        {"connect_timeout": -1},
        {"keepalive": "yes"},
        {"timeout": "5"},
        {"reaper_interval": float("nan")},
        {"synchronous": "OFF"},
        {"full_policy": "drop_newest"},
    ],
)
def test_constructor_validation(kwargs):
    with pytest.raises(ValueError):
        MyQueue("127.0.0.1", PORTS.get(), secret_key=KEY, **kwargs)


def test_lone_surrogate_raises_value_error():
    with pytest.raises(ValueError, match="surrogate"):
        tcpQueue._serialize("\ud800")


# --------------------------------------------------------------------------- #
# Lease / ack / release                                                       #
# --------------------------------------------------------------------------- #


def test_lease_ack_flow():
    with running_server() as (server, port):
        for i in range(3):
            server.send_to_consumer({"i": i})
        client = make_client(port)
        try:
            lease = client.lease_consumer()
            assert isinstance(lease, Lease)
            assert lease.item == {"i": 0} and lease.queue == "consumer"
            assert server.consumer_size() == 3
            assert server.consumer_inflight() == 1
            # Other consumers skip the leased row.
            assert client.get_consumer() == {"i": 1}
            assert server.peek_consumer() == {"i": 2}
            assert client.ack(lease) is True
            assert client.ack(lease) is False, "second ack must report the lease gone"
            assert server.consumer_size() == 1
            assert server.consumer_inflight() == 0
        finally:
            client.close()


def test_lease_expiry_redelivers_and_invalidates_old_token():
    with running_server(lease_seconds=0.4) as (server, port):
        server.send_to_consumer("job")
        client = make_client(port)
        try:
            first = client.lease_consumer()
            with pytest.raises(Empty):
                client.lease_consumer()  # invisible while leased
            sleep(0.6)
            second = client.lease_consumer()  # redelivered after expiry
            assert second.item == "job"
            assert second.id == first.id
            assert second.token != first.token
            assert client.ack(first) is False, "stale token must not delete the row"
            assert server.consumer_size() == 1
            assert client.ack(second) is True
            assert server.consumer_size() == 0
        finally:
            client.close()


def test_release_returns_item_immediately():
    with running_server() as (server, port):
        server.send_to_consumer("x")
        client = make_client(port)
        try:
            lease = client.lease_consumer()
            with pytest.raises(Empty):
                client.get_consumer()
            assert client.release(lease) is True
            assert client.release(lease) is False
            assert client.get_consumer() == "x"
        finally:
            client.close()


def test_lease_producer_path():
    with running_server() as (server, port):
        client = make_client(port)
        try:
            assert client.send_to_producer({"job": 1})
            lease = client.lease_producer()
            assert lease.item == {"job": 1} and lease.queue == "producer"
            assert server.producer_inflight() == 1
            assert client.ack(lease)
            assert server.producer_size() == 0
        finally:
            client.close()


def test_ack_rejects_non_lease():
    client = make_client(PORTS.get())
    with pytest.raises(TypeError):
        client.ack("not a lease")
    with pytest.raises(ValueError):
        client.ack(Lease(item=None, queue="bogus", id=1, token=1))


# --------------------------------------------------------------------------- #
# Long polling                                                                #
# --------------------------------------------------------------------------- #


def test_blocking_get_wakes_on_enqueue():
    with running_server() as (server, port):
        client = make_client(port)
        try:

            def feed():
                sleep(0.4)
                server.send_to_consumer("late")

            threading.Thread(target=feed, daemon=True).start()
            t0 = time.monotonic()
            assert client.get_consumer(wait=5.0) == "late"
            elapsed = time.monotonic() - t0
            assert 0.3 <= elapsed < 3.0, elapsed
        finally:
            client.close()


def test_blocking_get_times_out_with_empty():
    with running_server() as (server, port):
        client = make_client(port)
        try:
            t0 = time.monotonic()
            with pytest.raises(Empty):
                client.get_consumer(wait=0.4)
            elapsed = time.monotonic() - t0
            assert 0.35 <= elapsed < 3.0, elapsed
            # The connection is still usable afterwards.
            server.send_to_consumer(1)
            assert client.get_consumer() == 1
        finally:
            client.close()


def test_blocking_lease_wakes_on_remote_put():
    with running_server() as (_server, port):
        waiter = make_client(port)
        producer = make_client(port)
        got: list = []

        def wait_for_item():
            try:
                got.append(waiter.lease_producer(wait=5.0))
            except Exception as ex:  # pragma: no cover - reported via assert
                got.append(ex)

        t = threading.Thread(target=wait_for_item)
        t.start()
        sleep(0.3)
        try:
            assert producer.send_to_producer("remote")
            t.join(5.0)
            assert got and isinstance(got[0], Lease), got
            assert got[0].item == "remote"
            assert waiter.ack(got[0])
        finally:
            waiter.close()
            producer.close()


def test_blocking_get_aborts_on_shutdown():
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)
    client = make_client(port)
    result: list = []

    def waiter():
        try:
            result.append(client.get_consumer(wait=30.0))
        except Exception as ex:
            result.append(ex)

    t = threading.Thread(target=waiter)
    t.start()
    sleep(0.3)
    try:
        t0 = time.monotonic()
        server.stop_server()
        t.join(5.0)
        assert not t.is_alive(), "long-poll did not abort on shutdown"
        assert isinstance(result[0], ConnectionError), result
        assert time.monotonic() - t0 < 4.0
    finally:
        client.close()
        db.cleanup()


def test_wait_argument_validation():
    client = make_client(PORTS.get())
    with pytest.raises(ValueError):
        client.get_consumer(wait=-1)
    with pytest.raises(ValueError):
        client.lease_consumer(wait=tcpQueue._MAX_WAIT_SECONDS + 1)
    with pytest.raises(TypeError):
        client.get_consumer(wait="1")


# --------------------------------------------------------------------------- #
# Backpressure, connection limits, keepalive, IPv6                            #
# --------------------------------------------------------------------------- #


def test_full_policy_reject():
    with running_server(max_queue_size=2, full_policy="reject") as (server, port):
        server.send_to_consumer(1)
        server.send_to_consumer(2)
        with pytest.raises(Full):
            server.send_to_consumer(3)
        assert server.consumer_size() == 2

        client = make_client(port)
        try:
            assert client.send_to_producer("a")
            assert client.send_to_producer("b")
            t0 = time.monotonic()
            with pytest.raises(Full):
                client.send_to_producer("c")
            assert (
                time.monotonic() - t0 < 1.0
            ), "Full must not trigger the retry back-off"
            assert server.producer_size() == 2
            assert (
                client.get_producer() == "a"
            ), "connection must survive a FULL response"
        finally:
            client.close()


def test_max_connections_refuses_extra_clients():
    with running_server(max_connections=1) as (server, port):
        first = make_client(port)
        assert first.start_client()
        sleep(0.2)

        extra = socket.create_connection(("127.0.0.1", port), timeout=3.0)
        try:
            try:
                data = extra.recv(1)
            except OSError:
                data = b""
            assert data == b"", "second connection should have been closed"
        finally:
            extra.close()

        server.send_to_consumer("ok")
        assert first.get_consumer() == "ok", "first connection must be unaffected"
        first.close()
        sleep(0.3)  # let the server notice the close and free the slot

        second = make_client(port)
        server.send_to_consumer("again")
        assert second.get_consumer() == "again"
        second.close()


def test_keepalive_enabled_on_both_ends():
    with running_server() as (server, port):
        client = make_client(port)
        assert client.start_client()
        sleep(0.2)
        try:
            assert client._csock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
            with server._workers_lock:
                workers = list(server._workers)
            assert len(workers) == 1
            assert workers[0].getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
        finally:
            client.close()

    with running_server(keepalive=False) as (server, port):
        client = make_client(port, keepalive=False)
        assert client.start_client()
        sleep(0.2)
        try:
            assert client._csock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 0
            with server._workers_lock:
                workers = list(server._workers)
            assert workers[0].getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 0
        finally:
            client.close()


def test_ipv6_roundtrip():
    if not socket.has_ipv6:
        pytest.skip("IPv6 not supported on this platform")
    db = _DbBox()
    port = PORTS.get()
    try:
        server = MyQueue("::1", port, db_path=db.path(), secret_key=KEY)
        try:
            server.start_server()
        except OSError as ex:
            pytest.skip(f"IPv6 loopback unavailable: {ex}")
        sleep(0.1)
        try:
            server.send_to_consumer({"v": 6})
            client = MyQueue("::1", port, secret_key=KEY)
            assert client.get_consumer() == {"v": 6}
            client.close()
        finally:
            server.stop_server()
    finally:
        db.cleanup()


# --------------------------------------------------------------------------- #
# Error handling and protocol robustness                                      #
# --------------------------------------------------------------------------- #


def test_server_error_frame_keeps_connection(monkeypatch):
    with running_server() as (server, port):
        client = make_client(port)
        try:
            server.send_to_consumer("first")

            def boom(_table):
                raise sqlite3.OperationalError("database is locked (simulated)")

            monkeypatch.setattr(server, "_dequeue", boom)
            with pytest.raises(RemoteError, match="simulated"):
                client.get_consumer()
            sock_before = client._csock
            assert sock_before is not None, "error frame must not drop the connection"

            monkeypatch.undo()
            assert client.get_consumer() == "first"
            assert client._csock is sock_before
        finally:
            client.close()


def test_protocol_errors_log_peer_address(caplog):
    with running_server() as (_server, port):
        caplog.set_level(logging.WARNING, logger=tcpQueue.log.name)
        sock = socket.create_connection(("127.0.0.1", port), timeout=3.0)
        local_port = sock.getsockname()[1]
        sock.sendall(b"abc:")
        try:
            sock.recv(10)
        except OSError:
            pass
        sock.close()
        sleep(0.2)
        messages = [r.getMessage() for r in caplog.records]
        assert any(
            "non-numeric length" in m and f"127.0.0.1:{local_port}" in m
            for m in messages
        ), messages


def test_unknown_opcode_gets_error_frame_then_close():
    with running_server() as (_server, port):
        sock, reader = _raw_connect(port)
        try:
            sock.sendall(_raw_frame(b"\x7f", b""))
            frame = reader.read()
            assert frame is not None
            op, payload = frame
            assert op == _OP_ERROR and b"unknown opcode" in payload
            assert reader.read() is None, "server should close after a protocol error"
        finally:
            sock.close()


def test_empty_put_gets_error_frame_then_close():
    with running_server() as (server, port):
        sock, reader = _raw_connect(port)
        try:
            sock.sendall(_raw_frame(_OP_PUT_PRODUCER, b""))
            op, payload = reader.read()
            assert op == _OP_ERROR and b"empty PUT" in payload
            assert reader.read() is None
        finally:
            sock.close()
        assert server.producer_size() == 0


def test_reader_handles_back_to_back_frames():
    """The buffered reader must keep bytes belonging to the next frame."""
    with running_server() as (server, port):
        server.send_to_consumer(1)
        server.send_to_consumer(2)
        sock, reader = _raw_connect(port)
        try:
            sock.sendall(_raw_frame(_OP_GET_CONSUMER, b"") * 3)
            results = [reader.read() for _ in range(3)]
            assert [op for op, _ in results] == [_OP_ITEM, _OP_ITEM, _OP_EMPTY]
            assert [_deserialize(p) for _, p in results[:2]] == [1, 2]
        finally:
            sock.close()


def test_mid_frame_stall_closes_connection():
    with running_server(timeout=0.5) as (_server, port):
        sock = socket.create_connection(("127.0.0.1", port), timeout=5.0)
        try:
            sock.sendall(b"1")  # first byte of a header, then stall past the timeout
            sleep(1.2)
            try:
                data = sock.recv(10)
            except OSError:
                data = b""
            assert data == b"", "server must drop a peer that stalls mid-frame"
        finally:
            sock.close()


def test_idle_connection_survives_timeout():
    with running_server(timeout=0.5) as (_server, port):
        sock, reader = _raw_connect(port)
        try:
            sleep(1.2)  # idle (no partial frame) for longer than the timeout
            sock.sendall(_raw_frame(_OP_GET_CONSUMER, b""))
            op, _ = reader.read()
            assert op == _OP_EMPTY
        finally:
            sock.close()


def test_frame_writer_reader_roundtrip_large_payload():
    a, b = socket.socketpair()
    try:
        payload = os.urandom(300_000)  # > _RECV_CHUNK: exercises the recv_into path
        writer = threading.Thread(target=_Frame.write, args=(a, _OP_ITEM, payload, KEY))
        writer.start()
        op, got = _FrameReader(b, KEY).read()
        writer.join(5.0)
        assert op == _OP_ITEM and got == payload
    finally:
        a.close()
        b.close()


def test_frame_reader_rejects_tampered_hmac():
    a, b = socket.socketpair()
    try:
        frame = bytearray(_raw_frame(_OP_ITEM, b"hello"))
        frame[-1] ^= 0xFF
        a.sendall(bytes(frame))
        with pytest.raises(ProtocolError, match="HMAC"):
            _FrameReader(b, KEY).read()
    finally:
        a.close()
        b.close()


def test_frame_reader_rejects_bad_headers():
    for raw in (b"0:", b"-5:", b"x" * 20):
        a, b = socket.socketpair()
        try:
            a.sendall(raw)
            a.close()
            with pytest.raises(ProtocolError):
                _FrameReader(b, None).read()
        finally:
            b.close()


# --------------------------------------------------------------------------- #
# Payload safety                                                              #
# --------------------------------------------------------------------------- #


def test_decompression_bomb_rejected(monkeypatch):
    monkeypatch.setattr(tcpQueue, "_MAX_DECOMPRESSED_BYTES", 1024)
    bomb = zlib.compress(b"0" * 100_000)
    assert len(bomb) < 1024, "bomb must be small on the wire"
    with pytest.raises(PayloadError, match="inflates beyond"):
        _deserialize(bomb)
    # Exactly at the limit is accepted; one byte over is not.
    assert _deserialize(zlib.compress(('"' + "x" * 1022 + '"').encode())) == "x" * 1022
    with pytest.raises(PayloadError):
        _deserialize(zlib.compress(('"' + "x" * 1023 + '"').encode()))


def test_malformed_payloads_raise_payload_error():
    with pytest.raises(PayloadError):
        _deserialize(b"not zlib at all")
    with pytest.raises(PayloadError, match="truncated"):
        _deserialize(zlib.compress(b'"abc"')[:-3])
    with pytest.raises(PayloadError):
        _deserialize(zlib.compress(b"{not json"))
    assert issubclass(
        PayloadError, ValueError
    ), "backwards compatible with callers catching ValueError"


def test_poison_payload_raises_payload_error_and_keeps_connection():
    with running_server() as (server, port):
        server._enqueue("consumer", b"this is not zlib")
        server.send_to_consumer("good")
        client = make_client(port)
        try:
            with pytest.raises(PayloadError):
                client.get_consumer()
            sock = client._csock
            assert sock is not None, "a bad payload is not a connection problem"
            assert client.get_consumer() == "good"
            assert client._csock is sock
        finally:
            client.close()


def test_poison_payload_under_lease_is_dropped():
    with running_server() as (server, port):
        server._enqueue("consumer", b"garbage")
        server.send_to_consumer("good")
        client = make_client(port)
        try:
            with pytest.raises(PayloadError):
                client.lease_consumer()
            assert (
                server.consumer_size() == 1
            ), "poison row must be deleted, not re-leased"
            lease = client.lease_consumer()
            assert lease.item == "good"
            assert client.ack(lease)
        finally:
            client.close()


def test_oversized_payload_raises_immediately(monkeypatch):
    with running_server() as (server, port):
        monkeypatch.setattr(tcpQueue, "_MAX_PAYLOAD_BYTES", 256)
        client = make_client(port)
        try:
            big = os.urandom(2048).hex()
            t0 = time.monotonic()
            with pytest.raises(ValueError, match="maximum"):
                client.send_to_producer(big)
            assert time.monotonic() - t0 < 1.0, "must not enter the retry loop"
            with pytest.raises(ValueError, match="maximum"):
                server.send_to_consumer(big)
            assert client.send_to_producer("small") is True
            assert server.producer_size() == 1
        finally:
            client.close()


# --------------------------------------------------------------------------- #
# Lifecycle niceties                                                          #
# --------------------------------------------------------------------------- #


def test_context_manager_stops_server_and_closes_client():
    db = _DbBox()
    port = PORTS.get()
    try:
        with MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY) as server:
            server.start_server()
            sleep(0.1)
            with make_client(port) as client:
                assert client.start_client()
                assert client._csock is not None
            assert client._csock is None
            assert server._ssock is not None
        assert server._ssock is None and server._listen_thread is None
    finally:
        db.cleanup()


def test_signal_handlers_can_be_uninstalled():
    sig = getattr(signal, "SIGUSR2", None)
    if sig is None:
        pytest.skip("SIGUSR2 not available on this platform")
    original = signal.getsignal(sig)
    q = make_client(PORTS.get())
    q.install_signal_handlers(signals=(sig,))
    assert signal.getsignal(sig) is not original
    q.install_signal_handlers(signals=(sig,))  # re-install must not forget the original
    q.uninstall_signal_handlers()
    assert signal.getsignal(sig) == original


def test_start_client_reconnects():
    with running_server() as (_server, port):
        client = make_client(port)
        assert client.start_client()
        first = client._csock
        assert client.start_client()
        assert client._csock is not first, "start_client drops and replaces the socket"
        client.close()
        assert client._csock is None


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
