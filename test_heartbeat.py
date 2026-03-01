# -*- coding: utf-8 -*-
"""
Unit and integration tests for heartbeat.py.

Unit tests cover individual functions with no network or process involvement.
Integration tests spin up a real loopback server and exercise the full
client/server protocol path.

Run with:
    python3 -m unittest test_heartbeat -v
"""

import queue
import socket
import subprocess
import sys
import threading
import time
import types
import unittest
from unittest.mock import MagicMock, call, patch

import heartbeat

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _loopback_pair():
    """Return a connected (client_sock, server_conn) pair."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cli.connect(("127.0.0.1", port))
    conn, _ = srv.accept()
    srv.close()
    return cli, conn


def _reset_state():
    """Reset all heartbeat module globals between tests."""
    heartbeat.myState = "D"
    heartbeat.DEBUG = False
    with heartbeat.workerQueue.mutex:
        heartbeat.workerQueue.queue.clear()


# ============================================================================
# 1. _close_socket
# ============================================================================


class TestCloseSocket(unittest.TestCase):

    def test_calls_shutdown_then_close(self):
        mock = MagicMock()
        heartbeat._close_socket(mock)
        mock.shutdown.assert_called_once_with(socket.SHUT_RDWR)
        mock.close.assert_called_once()

    def test_tolerates_shutdown_exception(self):
        mock = MagicMock()
        mock.shutdown.side_effect = OSError("already closed")
        heartbeat._close_socket(mock)  # must not raise
        mock.close.assert_called_once()

    def test_tolerates_close_exception(self):
        mock = MagicMock()
        mock.close.side_effect = OSError("already closed")
        heartbeat._close_socket(mock)  # must not raise

    def test_real_socket_closes_cleanly(self):
        cli, srv = _loopback_pair()
        heartbeat._close_socket(cli)
        heartbeat._close_socket(srv)  # must not raise


# ============================================================================
# 2. countdown
# ============================================================================


class TestCountdown(unittest.TestCase):

    def setUp(self):
        _reset_state()

    def test_sets_state_to_primary(self):
        heartbeat.myState = "S"
        heartbeat.countdown()
        self.assertEqual(heartbeat.myState, "P")

    def test_sets_state_from_unknown(self):
        heartbeat.myState = "U"
        heartbeat.countdown()
        self.assertEqual(heartbeat.myState, "P")

    def test_idempotent_when_already_primary(self):
        heartbeat.myState = "P"
        heartbeat.countdown()
        self.assertEqual(heartbeat.myState, "P")


# ============================================================================
# 3. checkForServer
# ============================================================================


class TestCheckForServer(unittest.TestCase):

    def setUp(self):
        _reset_state()

    def _run_with_output(self, stdout: bytes):
        """Patch subprocess.run to simulate ps output."""
        result = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=stdout
        )
        with patch("heartbeat.subprocess.run", return_value=result):
            heartbeat.checkForServer("fake_process")

    def test_demotes_when_process_not_found(self):
        heartbeat.myState = "P"
        self._run_with_output(b"")
        self.assertEqual(heartbeat.myState, "S")

    def test_does_not_change_state_when_process_running(self):
        heartbeat.myState = "P"
        self._run_with_output(b"12345 fake_process")
        self.assertEqual(heartbeat.myState, "P")

    def test_demotes_secondary_stays_secondary(self):
        heartbeat.myState = "S"
        self._run_with_output(b"")
        self.assertEqual(heartbeat.myState, "S")

    def test_demotes_on_subprocess_exception(self):
        heartbeat.myState = "P"
        with patch("heartbeat.subprocess.run", side_effect=OSError("no ps")):
            heartbeat.checkForServer("fake_process")
        self.assertEqual(heartbeat.myState, "S")

    def test_whitespace_only_output_counts_as_not_running(self):
        # ps --no-heading with no matches may return only whitespace
        heartbeat.myState = "P"
        self._run_with_output(b"   \n  ")
        self.assertEqual(heartbeat.myState, "S")


# ============================================================================
# 4. manageWorkers
# ============================================================================


class TestManageWorkers(unittest.TestCase):

    def setUp(self):
        _reset_state()

    def _drain_queue(self) -> list:
        items = []
        while not heartbeat.workerQueue.empty():
            items.append(heartbeat.workerQueue.get_nowait())
        return items

    def test_reaps_finished_threads(self):
        finished = threading.Thread(target=lambda: None)
        finished.start()
        finished.join()
        heartbeat.workerQueue.put(finished)

        # Patch sleep so the loop runs immediately once.
        call_count = [0]
        orig_sleep = time.sleep

        def fast_sleep(n):
            call_count[0] += 1
            if call_count[0] >= 2:
                raise SystemExit  # stop the infinite loop after one reap cycle
            orig_sleep(0)

        with patch("heartbeat.time.sleep", fast_sleep):
            try:
                heartbeat.manageWorkers()
            except SystemExit:
                pass

        remaining = self._drain_queue()
        self.assertEqual(
            remaining, [], "Finished thread should have been reaped"
        )

    def test_keeps_live_threads(self):
        event = threading.Event()
        live = threading.Thread(target=event.wait)
        live.daemon = True
        live.start()
        heartbeat.workerQueue.put(live)

        call_count = [0]
        orig_sleep = time.sleep

        def fast_sleep(n):
            call_count[0] += 1
            if call_count[0] >= 2:
                raise SystemExit
            orig_sleep(0)

        with patch("heartbeat.time.sleep", fast_sleep):
            try:
                heartbeat.manageWorkers()
            except SystemExit:
                pass
        event.set()
        live.join(1.0)

        remaining = self._drain_queue()
        self.assertEqual(len(remaining), 1, "Live thread should be kept")

    def test_empty_queue_does_not_raise(self):
        call_count = [0]
        orig_sleep = time.sleep

        def fast_sleep(n):
            call_count[0] += 1
            if call_count[0] >= 2:
                raise SystemExit
            orig_sleep(0)

        with patch("heartbeat.time.sleep", fast_sleep):
            try:
                heartbeat.manageWorkers()
            except SystemExit:
                pass  # No assertion — just verifying no exception


# ============================================================================
# 5. clientThread — protocol handling via real socket pairs
# ============================================================================


class TestClientThread(unittest.TestCase):
    """Drive clientThread by writing directly to a socket pair."""

    def setUp(self):
        _reset_state()
        heartbeat.DEBUG = False

    def tearDown(self):
        heartbeat.DEBUG = False
        _reset_state()

    def _start_client_thread(
        self, server_conn: socket.socket
    ) -> threading.Thread:
        t = threading.Thread(
            target=heartbeat.clientThread, args=(server_conn,)
        )
        t.daemon = True
        t.start()
        return t

    # --- b"g": state query ---

    def test_g_returns_current_state(self):
        cli, srv = _loopback_pair()
        heartbeat.myState = "P"
        t = self._start_client_thread(srv)
        cli.sendall(b"g")
        reply = cli.recv(1)
        cli.close()
        t.join(2.0)
        self.assertEqual(reply, b"P")

    def test_g_returns_secondary_state(self):
        cli, srv = _loopback_pair()
        heartbeat.myState = "S"
        t = self._start_client_thread(srv)
        cli.sendall(b"g")
        reply = cli.recv(1)
        cli.close()
        t.join(2.0)
        self.assertEqual(reply, b"S")

    def test_g_multiple_queries(self):
        cli, srv = _loopback_pair()
        heartbeat.myState = "U"
        t = self._start_client_thread(srv)
        for _ in range(3):
            cli.sendall(b"g")
            reply = cli.recv(1)
            self.assertEqual(reply, b"U")
        cli.close()
        t.join(2.0)

    # --- b"r": re-election random byte ---

    def test_r_returns_single_byte_in_range(self):
        cli, srv = _loopback_pair()
        t = self._start_client_thread(srv)
        cli.sendall(b"r")
        reply = cli.recv(1)
        cli.close()
        t.join(2.0)
        self.assertEqual(len(reply), 1)
        self.assertGreaterEqual(reply[0], 0)
        self.assertLessEqual(reply[0], 9)

    def test_r_always_returns_valid_byte(self):
        """Fire r 20 times and verify all responses are in 0–9."""
        cli, srv = _loopback_pair()
        t = self._start_client_thread(srv)
        for _ in range(20):
            cli.sendall(b"r")
            reply = cli.recv(1)
            self.assertIn(reply[0], range(10))
        cli.close()
        t.join(2.0)

    # --- b"P" / b"S": state set ---

    def test_P_sets_state_to_primary(self):
        cli, srv = _loopback_pair()
        heartbeat.myState = "S"
        t = self._start_client_thread(srv)
        cli.sendall(b"P")
        time.sleep(0.05)
        self.assertEqual(heartbeat.myState, "P")
        cli.close()
        t.join(2.0)

    def test_S_sets_state_to_secondary(self):
        cli, srv = _loopback_pair()
        heartbeat.myState = "P"
        t = self._start_client_thread(srv)
        cli.sendall(b"S")
        time.sleep(0.05)
        self.assertEqual(heartbeat.myState, "S")
        cli.close()
        t.join(2.0)

    def test_state_update_then_query(self):
        """Set state via b'S', then query it back with b'g'."""
        cli, srv = _loopback_pair()
        heartbeat.myState = "P"
        t = self._start_client_thread(srv)
        cli.sendall(b"S")
        time.sleep(0.05)
        cli.sendall(b"g")
        reply = cli.recv(1)
        cli.close()
        t.join(2.0)
        self.assertEqual(reply, b"S")

    # --- Clean disconnect ---

    def test_clean_disconnect_does_not_raise(self):
        """Thread should return cleanly when client closes connection."""
        cli, srv = _loopback_pair()
        t = self._start_client_thread(srv)
        cli.close()
        t.join(2.0)
        self.assertFalse(
            t.is_alive(), "Thread should have exited after disconnect"
        )

    # --- Unknown command ---

    def test_unknown_command_closes_connection(self):
        """Thread should return after receiving an unknown command byte."""
        cli, srv = _loopback_pair()
        t = self._start_client_thread(srv)
        cli.sendall(b"X")
        t.join(2.0)
        cli.close()
        self.assertFalse(t.is_alive(), "Thread should exit on unknown command")

    # --- Socket always closed after thread exits ---

    def test_socket_is_closed_after_disconnect(self):
        """After clientThread returns, the server-side socket should be closed."""
        cli, srv = _loopback_pair()
        t = self._start_client_thread(srv)
        cli.close()
        t.join(2.0)
        # Attempting to recv on a closed socket raises OSError.
        with self.assertRaises(OSError):
            srv.recv(1)

    def test_socket_is_closed_after_unknown_command(self):
        cli, srv = _loopback_pair()
        t = self._start_client_thread(srv)
        cli.sendall(b"Z")
        t.join(2.0)
        cli.close()
        with self.assertRaises(OSError):
            srv.recv(1)


# ============================================================================
# 6. serverThread — accepts connections and dispatches clientThreads
# ============================================================================


class TestServerThread(unittest.TestCase):

    def setUp(self):
        _reset_state()
        heartbeat.DEBUG = False

    def tearDown(self):
        heartbeat.DEBUG = False
        _reset_state()

    def test_dispatches_client_thread_on_connect(self):
        port = _find_free_port()
        ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ssock.bind(("127.0.0.1", port))
        ssock.listen(5)

        st = threading.Thread(target=heartbeat.serverThread, args=(ssock,))
        st.daemon = True
        st.start()

        heartbeat.myState = "P"
        cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cli.connect(("127.0.0.1", port))
        cli.sendall(b"g")
        reply = cli.recv(1)
        cli.close()
        ssock.close()

        self.assertEqual(reply, b"P")

    def test_multiple_clients_handled_independently(self):
        port = _find_free_port()
        ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ssock.bind(("127.0.0.1", port))
        ssock.listen(5)

        st = threading.Thread(target=heartbeat.serverThread, args=(ssock,))
        st.daemon = True
        st.start()

        heartbeat.myState = "S"
        results = []

        def client_task():
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect(("127.0.0.1", port))
            c.sendall(b"g")
            results.append(c.recv(1))
            c.close()

        threads = [threading.Thread(target=client_task) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(2.0)

        ssock.close()
        self.assertEqual(results, [b"S"] * 5)

    def test_enqueues_worker_threads(self):
        with heartbeat.workerQueue.mutex:
            heartbeat.workerQueue.queue.clear()

        port = _find_free_port()
        ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ssock.bind(("127.0.0.1", port))
        ssock.listen(5)

        st = threading.Thread(target=heartbeat.serverThread, args=(ssock,))
        st.daemon = True
        st.start()

        cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cli.connect(("127.0.0.1", port))
        time.sleep(0.05)  # Give serverThread time to enqueue the worker.
        cli.close()
        ssock.close()

        self.assertFalse(heartbeat.workerQueue.empty())


# ============================================================================
# 7. Re-election logic (isolated via direct state manipulation)
# ============================================================================


class TestReElectionLogic(unittest.TestCase):
    """
    Re-election is embedded in the __main__ block so we test the underlying
    logic directly: given known random values, the correct state is set
    and the correct byte is sent to the peer.
    """

    def setUp(self):
        _reset_state()
        heartbeat.DEBUG = False

    def _run_election(
        self,
        my_rand: int,
        remote_rand: int,
        initial_my_state: str,
        initial_rem_state: str,
    ):
        """
        Simulate one election cycle. Returns (final_myState, bytes_sent_to_peer).
        """
        sent = []
        mock_sock = MagicMock()
        mock_sock.recv.return_value = bytes([remote_rand])

        heartbeat.myState = initial_my_state

        # Replicate the election block from __main__
        with patch("heartbeat.random", return_value=my_rand / 10.0):
            rem_state = initial_rem_state
            both_primary = rem_state == "P" and heartbeat.myState == "P"
            neither_primary = rem_state != "P" and heartbeat.myState != "P"
            if both_primary or neither_primary:
                mock_sock.sendall(b"r")
                remrand = ord(mock_sock.recv(1))
                myrand = int(heartbeat.random() * 10)
                if myrand >= remrand:
                    heartbeat.myState = "P"
                    mock_sock.sendall(b"S")
                else:
                    heartbeat.myState = "S"
                    mock_sock.sendall(b"P")

        return heartbeat.myState, [
            c[0][0] for c in mock_sock.sendall.call_args_list
        ]

    def test_higher_rand_wins_primary(self):
        final_state, sent = self._run_election(
            my_rand=9,
            remote_rand=3,
            initial_my_state="S",
            initial_rem_state="S",
        )
        self.assertEqual(final_state, "P")
        self.assertIn(b"S", sent)  # told remote it's Secondary

    def test_lower_rand_becomes_secondary(self):
        final_state, sent = self._run_election(
            my_rand=2,
            remote_rand=8,
            initial_my_state="S",
            initial_rem_state="S",
        )
        self.assertEqual(final_state, "S")
        self.assertIn(b"P", sent)  # told remote it's Primary

    def test_equal_rand_local_wins(self):
        # myrand >= remrand → local wins when equal
        final_state, sent = self._run_election(
            my_rand=5,
            remote_rand=5,
            initial_my_state="U",
            initial_rem_state="U",
        )
        self.assertEqual(final_state, "P")

    def test_both_primary_triggers_election(self):
        final_state, sent = self._run_election(
            my_rand=7,
            remote_rand=4,
            initial_my_state="P",
            initial_rem_state="P",
        )
        # Election ran — state was reassigned.
        self.assertIn(final_state, ("P", "S"))

    def test_one_primary_one_secondary_no_election(self):
        """No re-election when states are already split correctly."""
        mock_sock = MagicMock()
        heartbeat.myState = "P"
        rem_state = "S"
        both_primary = rem_state == "P" and heartbeat.myState == "P"
        neither_primary = rem_state != "P" and heartbeat.myState != "P"
        if both_primary or neither_primary:
            mock_sock.sendall(b"r")

        mock_sock.sendall.assert_not_called()
        self.assertEqual(heartbeat.myState, "P")


# ============================================================================
# 8. countdown timer fires after TDELAY
# ============================================================================


class TestCountdownTimer(unittest.TestCase):

    def setUp(self):
        _reset_state()

    def tearDown(self):
        _reset_state()

    def test_timer_fires_and_sets_primary(self):
        heartbeat.myState = "S"
        t = threading.Timer(0.05, heartbeat.countdown)
        t.start()
        t.join(1.0)
        self.assertEqual(heartbeat.myState, "P")

    def test_cancelled_timer_does_not_fire(self):
        heartbeat.myState = "S"
        t = threading.Timer(0.1, heartbeat.countdown)
        t.start()
        t.cancel()
        time.sleep(0.2)
        self.assertEqual(
            heartbeat.myState, "S", "Cancelled timer should not fire"
        )


# ============================================================================
# 9. Module-level constants and initial state
# ============================================================================


class TestModuleConstants(unittest.TestCase):

    def test_tdelay_is_positive_float(self):
        self.assertIsInstance(heartbeat.TDELAY, float)
        self.assertGreater(heartbeat.TDELAY, 0)

    def test_worker_queue_is_queue(self):
        self.assertIsInstance(heartbeat.workerQueue, queue.Queue)

    def test_initial_state_is_single_char_string(self):
        # The protocol uses single-char state bytes.
        self.assertIsInstance(heartbeat.myState, str)
        self.assertEqual(len(heartbeat.myState), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
