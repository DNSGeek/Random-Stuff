# -*- coding: utf-8 -*-
"""
Unit and integration tests for tcpQueue.py.

Unit tests cover internal helpers and in-process queue operations with no
network involvement.  Integration tests spin up a real loopback server on an
ephemeral port and exercise the full send/receive path.

Run with:
    python -m pytest test_tcpQueue.py -v
"""

import io
import pickle
import socket
import sys
import threading
import time
import types
import unittest
import zlib
from collections import deque
from unittest.mock import MagicMock, call, patch

# ---------------------------------------------------------------------------
# Bootstrap: provide stub modules for optional dependencies so the import
# works in any environment (no nmSys, syslog available on non-Unix systems).
# ---------------------------------------------------------------------------

_syslog_stub = types.ModuleType("syslog")
_syslog_stub.openlog = MagicMock()
_syslog_stub.syslog = MagicMock()
_syslog_stub.closelog = MagicMock()
sys.modules.setdefault("syslog", _syslog_stub)
sys.modules.setdefault("nmSys", types.ModuleType("nmSys"))

import tcpQueue  # noqa: E402  (must come after stub setup)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_free_port() -> int:
    """Ask the OS for an available port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _encode(obj) -> bytes:
    """Compress-pickle an object the same way tcpQueue does internally."""
    return zlib.compress(pickle.dumps(obj), 1)


def _loopback_socket_pair():
    """Return a connected (client_sock, server_sock) pair over loopback."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(("127.0.0.1", port))
    conn, _ = server.accept()
    server.close()
    return client, conn


# ---------------------------------------------------------------------------
# Module-level teardown: clear global queues between test classes
# ---------------------------------------------------------------------------


def _reset_global_queues():
    for q in (
        tcpQueue.consumerQueue,
        tcpQueue.producerQueue,
        tcpQueue.workerQueue,
    ):
        q.clear()


# ============================================================================
# 1. Helpers: _make_frame
# ============================================================================


class TestMakeFrame(unittest.TestCase):

    def test_empty_payload(self):
        frame = tcpQueue._make_frame(b"")
        self.assertEqual(frame, b"0:")

    def test_short_payload(self):
        frame = tcpQueue._make_frame(b"hello")
        self.assertEqual(frame, b"5:hello")

    def test_binary_payload(self):
        payload = bytes(range(256))
        frame = tcpQueue._make_frame(payload)
        self.assertTrue(frame.startswith(b"256:"))
        self.assertEqual(frame[4:], payload)

    def test_length_prefix_is_ascii_digits(self):
        payload = b"x" * 1000
        frame = tcpQueue._make_frame(payload)
        colon_idx = frame.index(b":")
        length = int(frame[:colon_idx])
        self.assertEqual(length, 1000)
        self.assertEqual(frame[colon_idx + 1 :], payload)

    def test_roundtrip_large_payload(self):
        payload = b"a" * 100_000
        frame = tcpQueue._make_frame(payload)
        colon_idx = frame.index(b":")
        length = int(frame[:colon_idx])
        self.assertEqual(length, len(payload))
        self.assertEqual(frame[colon_idx + 1 :], payload)


# ============================================================================
# 2. Helpers: _recv_exactly (via real socket pair)
# ============================================================================


class TestRecvExactly(unittest.TestCase):

    def setUp(self):
        self.client, self.server = _loopback_socket_pair()

    def tearDown(self):
        self.client.close()
        self.server.close()

    def test_receives_exact_bytes(self):
        self.client.sendall(b"hello world")
        result = tcpQueue._recv_exactly(self.server, 11)
        self.assertEqual(result, b"hello world")

    def test_receives_across_multiple_chunks(self):
        # Send in two pieces to exercise the accumulation loop.
        self.client.sendall(b"hello")
        time.sleep(0.01)
        self.client.sendall(b" world")
        result = tcpQueue._recv_exactly(self.server, 11)
        self.assertEqual(result, b"hello world")

    def test_returns_none_on_closed_connection(self):
        self.client.sendall(b"hi")
        self.client.close()
        # Ask for more bytes than were sent — should get None when socket closes.
        result = tcpQueue._recv_exactly(self.server, 100)
        self.assertIsNone(result)

    def test_zero_bytes_returns_empty(self):
        result = tcpQueue._recv_exactly(self.server, 0)
        self.assertEqual(result, b"")

    def test_large_payload_integrity(self):
        payload = bytes(range(256)) * 400  # 102,400 bytes
        self.client.sendall(payload)
        result = tcpQueue._recv_exactly(self.server, len(payload))
        self.assertEqual(result, payload)


# ============================================================================
# 3. Helpers: _read_framed_message (via real socket pair)
# ============================================================================


class TestReadFramedMessage(unittest.TestCase):

    def setUp(self):
        self.client, self.server = _loopback_socket_pair()

    def tearDown(self):
        self.client.close()
        self.server.close()

    def _send_frame(self, payload: bytes):
        self.client.sendall(tcpQueue._make_frame(payload))

    def test_reads_simple_payload(self):
        self._send_frame(b"hello")
        result = tcpQueue._read_framed_message(self.server)
        self.assertEqual(result, b"hello")

    def test_reads_empty_payload(self):
        self._send_frame(b"")
        result = tcpQueue._read_framed_message(self.server)
        self.assertEqual(result, b"")

    def test_reads_binary_payload(self):
        payload = bytes(range(256))
        self._send_frame(payload)
        result = tcpQueue._read_framed_message(self.server)
        self.assertEqual(result, payload)

    def test_reads_multiple_sequential_frames(self):
        for msg in [b"first", b"second", b"third"]:
            self._send_frame(msg)
        self.assertEqual(tcpQueue._read_framed_message(self.server), b"first")
        self.assertEqual(tcpQueue._read_framed_message(self.server), b"second")
        self.assertEqual(tcpQueue._read_framed_message(self.server), b"third")

    def test_returns_none_on_closed_connection(self):
        self.client.close()
        result = tcpQueue._read_framed_message(self.server)
        self.assertIsNone(result)

    def test_raises_on_header_without_colon(self):
        # Send 10 digits with no colon — should trigger the 'header too long' path.
        self.client.sendall(b"1234567890")
        with self.assertRaises(ValueError):
            tcpQueue._read_framed_message(self.server)

    def test_raises_on_empty_header(self):
        # A bare colon with nothing before it is an empty header.
        self.client.sendall(b":")
        with self.assertRaises(ValueError):
            tcpQueue._read_framed_message(self.server)

    def test_raises_on_non_numeric_length(self):
        self.client.sendall(b"abc:data")
        with self.assertRaises((ValueError, Exception)):
            tcpQueue._read_framed_message(self.server)


# ============================================================================
# 4. _EMPTY_PAYLOAD constant
# ============================================================================


class TestEmptyPayload(unittest.TestCase):

    def test_decodes_to_empty_list(self):
        decoded = pickle.loads(zlib.decompress(tcpQueue._EMPTY_PAYLOAD))
        self.assertEqual(decoded, [])

    def test_is_bytes(self):
        self.assertIsInstance(tcpQueue._EMPTY_PAYLOAD, bytes)


# ============================================================================
# 5. myQueue.__init__ validation
# ============================================================================


class TestMyQueueInit(unittest.TestCase):

    def test_valid_construction_defaults(self):
        q = tcpQueue.myQueue()
        self.assertEqual(q.myAddr, ("127.0.0.1", 49152))
        self.assertIsNone(q.ssock)
        self.assertIsNone(q.csock)

    def test_valid_construction_custom(self):
        q = tcpQueue.myQueue("192.168.1.1", 8080)
        self.assertEqual(q.myAddr, ("192.168.1.1", 8080))

    def test_raises_on_non_string_host(self):
        with self.assertRaises(ValueError):
            tcpQueue.myQueue(127001)

    def test_raises_on_none_host(self):
        with self.assertRaises(ValueError):
            tcpQueue.myQueue(None)

    def test_raises_on_port_zero(self):
        with self.assertRaises(ValueError):
            tcpQueue.myQueue("127.0.0.1", 0)

    def test_raises_on_port_too_large(self):
        with self.assertRaises(ValueError):
            tcpQueue.myQueue("127.0.0.1", 65536)

    def test_raises_on_negative_port(self):
        with self.assertRaises(ValueError):
            tcpQueue.myQueue("127.0.0.1", -1)

    def test_raises_on_float_port(self):
        with self.assertRaises(ValueError):
            tcpQueue.myQueue("127.0.0.1", 8080.0)

    def test_boundary_port_1(self):
        q = tcpQueue.myQueue("127.0.0.1", 1)
        self.assertEqual(q.myAddr[1], 1)

    def test_boundary_port_65535(self):
        q = tcpQueue.myQueue("127.0.0.1", 65535)
        self.assertEqual(q.myAddr[1], 65535)


# ============================================================================
# 6. In-process queue operations (no network)
# ============================================================================


class TestInProcessQueueOps(unittest.TestCase):

    def setUp(self):
        _reset_global_queues()
        self.q = tcpQueue.myQueue()

    def tearDown(self):
        _reset_global_queues()

    # --- sendToConsumer / CQSize / isCQEmpty ---

    def test_consumer_queue_starts_empty(self):
        self.assertEqual(self.q.CQSize(), 0)
        self.assertTrue(self.q.isCQEmpty())

    def test_send_to_consumer_increments_size(self):
        self.q.sendToConsumer("hello")
        self.assertEqual(self.q.CQSize(), 1)
        self.assertFalse(self.q.isCQEmpty())

    def test_send_multiple_items_to_consumer(self):
        for i in range(5):
            self.q.sendToConsumer(i)
        self.assertEqual(self.q.CQSize(), 5)

    def test_consumer_queue_contains_compressed_pickled_data(self):
        self.q.sendToConsumer({"key": "value"})
        raw = tcpQueue.consumerQueue[0]
        decoded = pickle.loads(zlib.decompress(raw))
        self.assertEqual(decoded, {"key": "value"})

    # --- producer queue ---

    def test_producer_queue_starts_empty(self):
        self.assertEqual(self.q.PQSize(), 0)
        self.assertTrue(self.q.isPQEmpty())

    # --- clearQueues ---

    def test_clear_queues_empties_both(self):
        self.q.sendToConsumer("a")
        tcpQueue.producerQueue.appendleft(_encode("b"))
        self.q.clearQueues()
        self.assertEqual(self.q.CQSize(), 0)
        self.assertEqual(self.q.PQSize(), 0)

    def test_clear_queues_idempotent_on_empty(self):
        self.q.clearQueues()  # Should not raise
        self.assertEqual(self.q.CQSize(), 0)

    # --- close ---

    def test_close_sets_csock_to_none(self):
        mock_sock = MagicMock()
        self.q.csock = mock_sock
        self.q.close()
        self.assertIsNone(self.q.csock)

    def test_close_calls_shutdown_and_close_on_socket(self):
        mock_sock = MagicMock()
        self.q.csock = mock_sock
        self.q.close()
        mock_sock.shutdown.assert_called_once_with(socket.SHUT_RDWR)
        mock_sock.close.assert_called_once()

    def test_close_tolerates_none_csock(self):
        self.q.csock = None
        self.q.close()  # Should not raise

    def test_close_clears_instance_dicts(self):
        self.q._test_dict = {"a": 1}
        self.q.close()
        self.assertEqual(self.q._test_dict, {})

    def test_close_clears_instance_lists(self):
        self.q._test_list = [1, 2, 3]
        self.q.close()
        self.assertEqual(self.q._test_list, [])

    # --- __str__ ---

    def test_str_returns_string(self):
        self.assertIsInstance(str(self.q), str)

    def test_str_contains_myAddr(self):
        result = str(self.q)
        self.assertIn("myAddr", result)

    def test_str_is_sorted(self):
        result = str(self.q)
        # Parse out the key names and verify they are in alphabetical order.
        pairs = eval(result)  # safe — it's our own __str__ output
        keys = [p[0] for p in pairs]
        self.assertEqual(keys, sorted(keys))


# ============================================================================
# 7. logger
# ============================================================================


class TestLogger(unittest.TestCase):

    def setUp(self):
        self._orig_debug = tcpQueue.DEBUG
        import warnings

        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        tcpQueue.DEBUG = self._orig_debug

    def test_empty_message_does_nothing(self):
        # Verify that an empty message causes an immediate return with no output.
        tcpQueue.DEBUG = True
        with patch("builtins.print") as mock_print:
            tcpQueue.logger("")
            mock_print.assert_not_called()
        tcpQueue.logger("  ")  # whitespace-only str() is non-empty, should log
        # No assertion needed — just confirming it doesn't raise.

    def test_debug_mode_prints(self):
        tcpQueue.DEBUG = True
        with patch("builtins.print") as mock_print:
            tcpQueue.logger("test message", "TESTID")
            mock_print.assert_called_once()
            printed = mock_print.call_args[0][0]
            self.assertIn("TESTID", printed)
            self.assertIn("test message", printed)

    def test_non_string_message_is_stringified(self):
        tcpQueue.DEBUG = True
        with patch("builtins.print") as mock_print:
            tcpQueue.logger(42)
            printed = mock_print.call_args[0][0]
            self.assertIn("42", printed)


# ============================================================================
# 8. _close_socket
# ============================================================================


class TestCloseSocket(unittest.TestCase):

    def test_calls_shutdown_and_close(self):
        mock_sock = MagicMock()
        tcpQueue._close_socket(mock_sock)
        mock_sock.shutdown.assert_called_once_with(socket.SHUT_RDWR)
        mock_sock.close.assert_called_once()

    def test_tolerates_shutdown_exception(self):
        mock_sock = MagicMock()
        mock_sock.shutdown.side_effect = OSError("already closed")
        tcpQueue._close_socket(mock_sock)  # Should not raise
        mock_sock.close.assert_called_once()

    def test_tolerates_close_exception(self):
        mock_sock = MagicMock()
        mock_sock.close.side_effect = OSError("already closed")
        tcpQueue._close_socket(mock_sock)  # Should not raise


# ============================================================================
# 9. Integration tests — full loopback server/client
# ============================================================================


class TestIntegration(unittest.TestCase):
    """Start a real server on a loopback port and exercise the full path."""

    PORT: int = 0  # assigned in setUpClass

    @classmethod
    def setUpClass(cls):
        _reset_global_queues()
        cls.PORT = _find_free_port()
        cls.server_q = tcpQueue.myQueue("127.0.0.1", cls.PORT)
        cls.server_q.startServer()
        time.sleep(0.05)  # Let the listener thread start.

    @classmethod
    def tearDownClass(cls):
        if cls.server_q.ssock:
            cls.server_q.ssock.close()
        _reset_global_queues()

    def setUp(self):
        _reset_global_queues()
        self.client_q = tcpQueue.myQueue("127.0.0.1", self.PORT)
        self.client_q.startClient()
        self.assertIsNotNone(self.client_q.csock, "Client failed to connect")

    def tearDown(self):
        self.client_q.close()
        _reset_global_queues()

    # --- Consumer queue (server pushes, client pulls) ---

    def test_consumer_roundtrip_string(self):
        self.server_q.sendToConsumer("hello")
        result = self.client_q.getConsumer()
        self.assertEqual(result, "hello")

    def test_consumer_roundtrip_integer(self):
        self.server_q.sendToConsumer(42)
        result = self.client_q.getConsumer()
        self.assertEqual(result, 42)

    def test_consumer_roundtrip_list(self):
        data = [1, "two", 3.0, None, True]
        self.server_q.sendToConsumer(data)
        result = self.client_q.getConsumer()
        self.assertEqual(result, data)

    def test_consumer_roundtrip_dict(self):
        data = {"key": "value", "nested": {"a": 1}}
        self.server_q.sendToConsumer(data)
        result = self.client_q.getConsumer()
        self.assertEqual(result, data)

    def test_consumer_roundtrip_bytes(self):
        data = bytes(range(256))
        self.server_q.sendToConsumer(data)
        result = self.client_q.getConsumer()
        self.assertEqual(result, data)

    def test_consumer_roundtrip_large_payload(self):
        data = list(range(10_000))
        self.server_q.sendToConsumer(data)
        result = self.client_q.getConsumer()
        self.assertEqual(result, data)

    def test_consumer_empty_queue_returns_empty_list(self):
        # Queue is empty — server should send the _EMPTY_PAYLOAD sentinel.
        result = self.client_q.getConsumer()
        self.assertEqual(result, [])

    def test_consumer_fifo_order(self):
        # deque.appendleft + deque.pop = FIFO (items come out in push order).
        for i in range(5):
            self.server_q.sendToConsumer(i)
        results = [self.client_q.getConsumer() for _ in range(5)]
        self.assertEqual(results, list(range(5)))

    # --- Producer queue (client pushes, server pulls) ---

    def test_producer_roundtrip_string(self):
        self.client_q.sendToProducer("world")
        time.sleep(0.05)  # Let the server thread push to producerQueue.
        result = self.server_q.getProducer()
        self.assertEqual(result, "world")

    def test_producer_roundtrip_dict(self):
        data = {"status": "ok", "code": 200}
        self.client_q.sendToProducer(data)
        time.sleep(0.05)
        result = self.server_q.getProducer()
        self.assertEqual(result, data)

    def test_producer_empty_queue_returns_empty_list(self):
        result = self.server_q.getProducer()
        self.assertEqual(result, [])

    def test_producer_roundtrip_large_payload(self):
        data = {"items": list(range(5_000))}
        self.client_q.sendToProducer(data)
        time.sleep(0.05)
        result = self.server_q.getProducer()
        self.assertEqual(result, data)

    # --- Bidirectional in one session ---

    def test_bidirectional_exchange(self):
        self.server_q.sendToConsumer("task")
        task = self.client_q.getConsumer()
        self.assertEqual(task, "task")

        self.client_q.sendToProducer("result")
        time.sleep(0.05)
        result = self.server_q.getProducer()
        self.assertEqual(result, "result")

    # --- Queue size helpers ---

    def test_cqsize_reflects_pending_items(self):
        self.assertEqual(self.server_q.CQSize(), 0)
        self.server_q.sendToConsumer("a")
        self.server_q.sendToConsumer("b")
        self.assertEqual(self.server_q.CQSize(), 2)
        self.client_q.getConsumer()
        self.assertEqual(self.server_q.CQSize(), 1)

    def test_pqsize_reflects_pending_items(self):
        self.assertEqual(self.server_q.PQSize(), 0)
        self.client_q.sendToProducer("x")
        time.sleep(0.05)
        self.assertEqual(self.server_q.PQSize(), 1)

    # --- Reconnect after close ---

    def test_getconsumer_reconnects_after_close(self):
        self.client_q.close()
        self.assertIsNone(self.client_q.csock)
        self.server_q.sendToConsumer("reconnect test")
        # getConsumer should transparently reconnect.
        result = self.client_q.getConsumer()
        self.assertEqual(result, "reconnect test")

    # --- sendToProducer retry ---

    def test_sendtoproducer_retries_on_broken_socket(self):
        """Simulate a broken socket on the first attempt; the retry should succeed."""
        real_csock = self.client_q.csock
        call_count = [0]

        # Wrap the real socket in a MagicMock that fails once then delegates.
        mock_sock = MagicMock(wraps=real_csock)

        def flaky_sendall(data):
            call_count[0] += 1
            if call_count[0] == 1:
                # Nullify csock so sendToProducer triggers reconnect on retry.
                self.client_q.csock = None
                raise OSError("simulated broken pipe")
            return real_csock.sendall(data)

        mock_sock.sendall = flaky_sendall
        self.client_q.csock = mock_sock

        self.client_q.sendToProducer("retry me")
        time.sleep(0.1)
        result = self.server_q.getProducer()
        self.assertEqual(result, "retry me")


# ============================================================================
# 10. Concurrent access
# ============================================================================


class TestConcurrentAccess(unittest.TestCase):
    """Verify thread safety of the in-process queue operations."""

    def setUp(self):
        _reset_global_queues()
        self.q = tcpQueue.myQueue()

    def tearDown(self):
        _reset_global_queues()

    def test_concurrent_sendToConsumer(self):
        n = 200
        threads = [
            threading.Thread(target=self.q.sendToConsumer, args=(i,))
            for i in range(n)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(self.q.CQSize(), n)

    def test_concurrent_clearQueues_does_not_corrupt(self):
        for i in range(50):
            self.q.sendToConsumer(i)

        errors = []

        def clear_loop():
            try:
                for _ in range(10):
                    self.q.clearQueues()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=clear_loop) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(
            errors, [], "Exceptions during concurrent clearQueues"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
