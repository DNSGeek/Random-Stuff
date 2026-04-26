"""Smoke tests for tcpQueue. Exercises:
- HMAC-authenticated round trip via SQLite
- Sentinel collision fix ([] / None as legitimate data)
- Empty exception on empty queue
- Bad HMAC rejection
- Clean stop_server() with active connections
- Thread-safe client
- JSON: TypeError on bytes / sets, tuple round-trips as list
- Persistence across server restart
- TTL reaper deletes old rows
- Max queue size evicts oldest
- Signal handler triggers shutdown
"""

import logging
import os
import secrets
import shutil
import signal
import sys
import tempfile
import threading
from pathlib import Path
from queue import Empty
from time import sleep

sys.path.insert(0, "/home/claude")
from tcpQueue import MyQueue

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


def test_basic_roundtrip():
    print("\n--- test_basic_roundtrip ---")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)
    try:
        for i in range(5):
            server.send_to_consumer({"i": i, "msg": f"hello {i}"})
        assert server.consumer_size() == 5

        client = MyQueue("127.0.0.1", port, secret_key=KEY)
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
        print(f"  OK: round-tripped {len(results)} items via SQLite")
    finally:
        server.stop_server()
        db.cleanup()


def test_sentinel_collision_fix():
    print("\n--- test_sentinel_collision_fix ---")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)
    try:
        tricky = [[], None, 0, "", False, [None], {}]
        for v in tricky:
            server.send_to_consumer(v)

        client = MyQueue("127.0.0.1", port, secret_key=KEY)
        client.start_client()
        results = [client.get_consumer() for _ in tricky]
        try:
            client.get_consumer()
            assert False
        except Empty:
            pass
        client.close()

        assert results == tricky
        print(f"  OK: {tricky} round-tripped; Empty raised when drained")
    finally:
        server.stop_server()
        db.cleanup()


def test_producer_path():
    print("\n--- test_producer_path ---")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)
    try:
        client = MyQueue("127.0.0.1", port, secret_key=KEY)
        client.start_client()
        for i in range(3):
            assert client.send_to_producer(f"result {i}")
        sleep(0.1)
        client.close()

        reader = MyQueue("127.0.0.1", port, secret_key=KEY)
        reader.start_client()
        results = []
        try:
            while True:
                results.append(reader.get_producer())
        except Empty:
            pass
        reader.close()

        assert results == ["result 0", "result 1", "result 2"]
        print(f"  OK: producer path delivered {results}")
    finally:
        server.stop_server()
        db.cleanup()


def test_bad_hmac_rejected():
    print("\n--- test_bad_hmac_rejected ---")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)
    try:
        server.send_to_consumer("secret data")
        wrong = secrets.token_bytes(32)
        client = MyQueue("127.0.0.1", port, secret_key=wrong)
        client.start_client()
        try:
            client.get_consumer()
            assert False
        except ConnectionError as ex:
            print(f"  OK: bad key rejected ({ex})")
        finally:
            client.close()
    finally:
        server.stop_server()
        db.cleanup()


def test_clean_shutdown():
    print("\n--- test_clean_shutdown ---")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)

    clients = []
    for _ in range(3):
        c = MyQueue("127.0.0.1", port, secret_key=KEY)
        c.start_client()
        clients.append(c)

    sleep(0.1)
    server.stop_server()
    print("  OK: server stopped cleanly with active connections")
    for c in clients:
        c.close()
    db.cleanup()


def test_thread_safety():
    print("\n--- test_thread_safety ---")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)
    try:
        client = MyQueue("127.0.0.1", port, secret_key=KEY)
        client.start_client()

        N_THREADS, N_PER = 5, 50
        errors: list[Exception] = []

        def worker(tid: int):
            try:
                for i in range(N_PER):
                    client.send_to_producer([tid, i])
            except Exception as ex:
                errors.append(ex)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(N_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        sleep(0.3)
        client.close()

        assert not errors, f"errors: {errors}"
        assert server.producer_size() == N_THREADS * N_PER
        print(f"  OK: {N_THREADS * N_PER} concurrent sends all delivered")
    finally:
        server.stop_server()
        db.cleanup()


def test_json_unsupported_types():
    print("\n--- test_json_unsupported_types ---")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)
    try:
        for bad in [b"raw bytes", {1, 2, 3}, frozenset([1])]:
            try:
                server.send_to_consumer(bad)
                assert False, f"expected TypeError for {bad!r}"
            except TypeError:
                pass
        print("  OK: TypeError raised on bytes/set/frozenset")
    finally:
        server.stop_server()
        db.cleanup()


def test_tuple_becomes_list():
    print("\n--- test_tuple_becomes_list ---")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)
    try:
        server.send_to_consumer((1, 2, 3))
        client = MyQueue("127.0.0.1", port, secret_key=KEY)
        client.start_client()
        result = client.get_consumer()
        client.close()
        assert result == [1, 2, 3] and isinstance(result, list)
        print(f"  OK: tuple (1,2,3) -> list {result} (documented)")
    finally:
        server.stop_server()
        db.cleanup()


def test_persistence_across_restart():
    """The whole point of on-disk SQLite: data survives the server going
    away. Push some items, stop the server entirely, start a brand-new
    one against the same DB, drain it."""
    print("\n--- test_persistence_across_restart ---")
    db = _DbBox()
    port = PORTS.get()
    db_path = db.path()
    try:
        # Round 1
        server1 = MyQueue("127.0.0.1", port, db_path=db_path, secret_key=KEY)
        server1.start_server()
        sleep(0.1)
        for i in range(10):
            server1.send_to_consumer({"id": i})
        assert server1.consumer_size() == 10
        server1.stop_server()
        del server1
        sleep(0.1)

        # Round 2 — fresh instance, same DB
        server2 = MyQueue("127.0.0.1", port, db_path=db_path, secret_key=KEY)
        assert (
            server2.consumer_size() == 10
        ), f"expected 10 surviving items, got {server2.consumer_size()}"
        server2.start_server()
        sleep(0.1)

        client = MyQueue("127.0.0.1", port, secret_key=KEY)
        client.start_client()
        ids = []
        try:
            while True:
                ids.append(client.get_consumer()["id"])
        except Empty:
            pass
        client.close()

        assert ids == list(range(10)), f"got {ids}"
        print("  OK: 10 items survived process boundary, drained in order")
        server2.stop_server()
    finally:
        db.cleanup()


def test_ttl_reaper():
    """Set TTL to 1s and a reaper interval of 0.5s. Push items, sleep
    past TTL, verify they're gone."""
    print("\n--- test_ttl_reaper ---")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue(
        "127.0.0.1",
        port,
        db_path=db.path(),
        secret_key=KEY,
        ttl_seconds=1.0,
        reaper_interval=0.3,
    )
    server.start_server()
    sleep(0.1)
    try:
        for i in range(5):
            server.send_to_consumer({"i": i})
        assert server.consumer_size() == 5

        # Wait for items to age past TTL + at least one reaper pass
        sleep(1.6)

        size = server.consumer_size()
        assert size == 0, f"expected 0 after TTL reap, got {size}"
        print("  OK: 5 items expired and were reaped")
    finally:
        server.stop_server()
        db.cleanup()


def test_max_queue_size_eviction():
    print("\n--- test_max_queue_size_eviction ---")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue(
        "127.0.0.1",
        port,
        db_path=db.path(),
        secret_key=KEY,
        max_queue_size=3,
    )
    server.start_server()
    sleep(0.1)
    try:
        for i in range(5):
            server.send_to_consumer({"i": i})
        assert server.consumer_size() == 3, f"got {server.consumer_size()}"

        # The two oldest (i=0, i=1) should have been evicted
        client = MyQueue("127.0.0.1", port, secret_key=KEY)
        client.start_client()
        ids = []
        try:
            while True:
                ids.append(client.get_consumer()["i"])
        except Empty:
            pass
        client.close()
        assert ids == [2, 3, 4], f"got {ids}"
        print("  OK: oldest items evicted when queue full")
    finally:
        server.stop_server()
        db.cleanup()


def test_signal_handler_triggers_shutdown():
    """Send ourselves SIGUSR1 (using a custom signal so we don't disrupt
    other tests). Verify stop_server runs and wait_for_shutdown returns."""
    print("\n--- test_signal_handler_triggers_shutdown ---")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)
    try:
        # Use SIGUSR1 to avoid colliding with the test runner's signals.
        sig = getattr(signal, "SIGUSR1", None)
        if sig is None:
            print("  SKIP: SIGUSR1 not available on this platform")
            return

        server.install_signal_handlers(signals=(sig,))

        # In a background thread, send ourselves the signal after a short delay.
        def fire():
            sleep(0.2)
            os.kill(os.getpid(), sig)

        threading.Thread(target=fire, daemon=True).start()

        # Block on shutdown event with a generous timeout
        got = server.wait_for_shutdown(timeout=5.0)
        assert got, "shutdown did not fire within 5s"
        assert server._ssock is None, "stop_server did not run"
        print("  OK: signal triggered stop_server cleanly")
    finally:
        # stop_server already ran via the handler; calling again is a no-op
        server.stop_server()
        db.cleanup()


def test_peek_does_not_dequeue():
    print("\n--- test_peek_does_not_dequeue ---")
    db = _DbBox()
    port = PORTS.get()
    server = MyQueue("127.0.0.1", port, db_path=db.path(), secret_key=KEY)
    server.start_server()
    sleep(0.1)
    try:
        for i in range(3):
            server.send_to_consumer({"i": i})
        # Peek twice — should return same item, count unchanged
        a = server.peek_consumer()
        b = server.peek_consumer()
        assert a == b == {"i": 0}, f"peek mismatch: {a} {b}"
        assert server.consumer_size() == 3, "peek changed size"
        # Empty case raises
        server.clear_queues()
        try:
            server.peek_consumer()
            assert False
        except Empty:
            pass
        print("  OK: peek returns oldest item without dequeueing")
    finally:
        server.stop_server()
        db.cleanup()


def test_schema_version_check():
    """A DB stamped with a higher schema version should be refused."""
    print("\n--- test_schema_version_check ---")
    db = _DbBox()
    port = PORTS.get()
    db_path = db.path()
    try:
        # Create a fresh DB, bump its user_version past what code supports
        import sqlite3 as _sql

        from tcpQueue import _SCHEMA_VERSION

        future = _SCHEMA_VERSION + 999
        conn = _sql.connect(str(db_path))
        conn.execute(f"PRAGMA user_version = {future}")
        conn.commit()
        conn.close()

        try:
            MyQueue("127.0.0.1", port, db_path=db_path, secret_key=KEY)
            assert False, "should have refused future-schema DB"
        except RuntimeError as ex:
            assert "newer than this code supports" in str(ex), f"wrong message: {ex}"
            print(f"  OK: refused DB with future schema version ({ex})")
    finally:
        db.cleanup()


def test_schema_version_stamp():
    """A fresh DB should be stamped with the current schema version."""
    print("\n--- test_schema_version_stamp ---")
    db = _DbBox()
    port = PORTS.get()
    db_path = db.path()
    try:
        from tcpQueue import _SCHEMA_VERSION

        MyQueue("127.0.0.1", port, db_path=db_path, secret_key=KEY)
        # Read it back
        import sqlite3 as _sql

        conn = _sql.connect(str(db_path))
        (v,) = conn.execute("PRAGMA user_version").fetchone()
        conn.close()
        assert v == _SCHEMA_VERSION, f"expected {_SCHEMA_VERSION}, got {v}"
        print(f"  OK: fresh DB stamped with version {v}")
    finally:
        db.cleanup()


def test_autovacuum_set_on_fresh_db():
    """Fresh DBs should have auto_vacuum=INCREMENTAL (mode 2)."""
    print("\n--- test_autovacuum_set_on_fresh_db ---")
    import sqlite3

    db = _DbBox()
    server = MyQueue("127.0.0.1", PORTS.get(), db_path=db.path(), secret_key=KEY)
    try:
        conn = sqlite3.connect(str(db.path()))
        try:
            (av,) = conn.execute("PRAGMA auto_vacuum").fetchone()
            assert av == 2, f"expected auto_vacuum=2 (INCREMENTAL), got {av}"
            print("  OK: fresh DB has auto_vacuum=INCREMENTAL")
        finally:
            conn.close()
    finally:
        del server
        db.cleanup()


def test_compaction_shrinks_db_file():
    """After pushing big payloads, draining, and compacting, the .db file
    should shrink dramatically. Without auto_vacuum it would stay sized
    for the high-water mark."""
    print("\n--- test_compaction_shrinks_db_file ---")
    import os as _os

    db = _DbBox()
    db_path = db.path()
    server = MyQueue("127.0.0.1", PORTS.get(), db_path=db_path, secret_key=KEY)
    server.start_server()
    sleep(0.1)
    try:
        # Push 1000 messages of pseudo-random data so zlib can't crush them
        # to nothing. Each ~500 bytes of incompressible data.
        for i in range(1000):
            server.send_to_consumer(
                {
                    "i": i,
                    "data": _os.urandom(500).hex(),  # 1KB hex string, random
                }
            )
        # Force pending WAL into the main DB so the size measurement is real.
        server.compact()
        size_full = _os.path.getsize(db_path)

        # Drain everything.
        server.clear_queues()
        # And compact.
        server.compact()
        size_compact = _os.path.getsize(db_path)

        # We expect a large reduction. The remaining bytes are schema +
        # bookkeeping pages, which don't depend on the data volume.
        assert size_compact < size_full * 0.10, (
            f"file didn't shrink enough: {size_full:,} -> {size_compact:,} "
            f"({size_compact / size_full:.0%} of original)"
        )
        print(
            f"  OK: file shrank {size_full:,} -> {size_compact:,} bytes "
            f"({size_compact / size_full:.0%})"
        )
    finally:
        server.stop_server()
        db.cleanup()


def test_wal_truncate():
    """After significant writes the WAL grows; after compact() it should
    be truncated to (near) zero bytes."""
    print("\n--- test_wal_truncate ---")
    db = _DbBox()
    db_path = db.path()
    server = MyQueue("127.0.0.1", PORTS.get(), db_path=db_path, secret_key=KEY)
    server.start_server()
    sleep(0.1)
    try:
        for i in range(500):
            server.send_to_consumer({"i": i, "data": "x" * 500})
        sleep(0.1)

        wal_path = str(db_path) + "-wal"
        wal_before = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0

        server.compact()

        wal_after = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0

        assert (
            wal_after < 100
        ), f"WAL not truncated: {wal_before:,} -> {wal_after:,} bytes"
        print(f"  OK: WAL truncated {wal_before:,} -> {wal_after:,} bytes")
    finally:
        server.stop_server()
        db.cleanup()


def test_legacy_db_warns():
    """Opening a pre-existing DB without auto_vacuum should log a warning
    explaining the manual migration."""
    print("\n--- test_legacy_db_warns ---")
    import sqlite3

    db = _DbBox()
    db_path = db.path()

    try:
        # Manually create a "legacy" DB: tables, no auto_vacuum, no version.
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "CREATE TABLE consumer (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "payload BLOB, created_at REAL)"
        )
        conn.execute(
            "CREATE TABLE producer (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "payload BLOB, created_at REAL)"
        )
        conn.commit()
        conn.close()

        # Capture warnings emitted during MyQueue construction.
        warnings_seen: list[str] = []

        class Capture(logging.Handler):
            def emit(self, record):
                if record.levelno >= logging.WARNING:
                    warnings_seen.append(record.getMessage())

        handler = Capture()
        logger = logging.getLogger("pyTCPQueue")
        logger.addHandler(handler)
        try:
            server = MyQueue(
                "127.0.0.1",
                PORTS.get(),
                db_path=db_path,
                secret_key=KEY,
            )
            del server  # not started, just constructed
        finally:
            logger.removeHandler(handler)

        relevant = [w for w in warnings_seen if "auto_vacuum" in w]
        assert relevant, f"no auto_vacuum warning: {warnings_seen}"
        # Sanity check the message includes the migration command
        assert "VACUUM" in relevant[0], f"warning lacks migration hint: {relevant[0]}"
        print("  OK: legacy DB triggered migration warning")
    finally:
        db.cleanup()


if __name__ == "__main__":
    test_basic_roundtrip()
    test_sentinel_collision_fix()
    test_producer_path()
    test_bad_hmac_rejected()
    test_clean_shutdown()
    test_thread_safety()
    test_json_unsupported_types()
    test_tuple_becomes_list()
    test_persistence_across_restart()
    test_ttl_reaper()
    test_max_queue_size_eviction()
    test_signal_handler_triggers_shutdown()
    test_peek_does_not_dequeue()
    test_schema_version_check()
    test_schema_version_stamp()
    test_autovacuum_set_on_fresh_db()
    test_compaction_shrinks_db_file()
    test_wal_truncate()
    test_legacy_db_warns()
    print("\nAll smoke tests passed.")
