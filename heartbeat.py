#!/usr/bin/env python3
"""
heartbeat.py — Primary/Secondary failover daemon for two-node clusters.

Two nodes run this service and connect to each other. Each maintains a state
(P = Primary, S = Secondary, U = Unknown). When the two states aren't
complementary (one P + one S), an election runs: the node with the lower
(ip, port) tuple wins and becomes Primary, EXCEPT a node whose locally
managed server process isn't running can never claim Primary.

Wire protocol (single-byte commands)
------------------------------------
    b"g"  → query peer's state; peer replies with its state byte
    b"P"  → notify peer that they should be Primary
    b"S"  → notify peer that they should be Secondary
    b"X"  → graceful goodbye: peer is shutting down cleanly, fail over now

The election does NOT use random numbers (the previous version did, and it
created split-brain about a quarter of the time when both nodes elected
simultaneously). Both nodes apply the same deterministic rule based on
(ip, port) and arrive at the same answer independently. The b"P"/b"S"
notifications are confirmation rather than primary mechanism.

State-change callback
---------------------
Pass ``on_state_change=lambda old, new: ...`` to the constructor to be
notified on every transition. The callback fires synchronously after the
state lock is released; exceptions are caught and logged so a buggy
callback can't take down the daemon. This is the integration point for
"start nginx when I become Primary, stop it when I demote", VIP takeover,
volume mounting, etc.

Graceful shutdown
-----------------
``stop()`` sends ``PROTO_GOODBYE`` to the peer over a short-lived socket
before tearing anything down. The peer immediately marks the failover
flag, bypassing the grace period and promoting in well under a second
instead of waiting the full ``failover_grace`` (default 10s). This only
helps planned shutdowns — SIGKILL, OOM kills, and power loss still go
through the normal grace path because there's no chance to send the
goodbye.

Health check caching
--------------------
``_is_local_server_running`` results are cached for
``health_check_cache_seconds`` (default 1.0). At a 1s heartbeat interval
without caching that's a fork-of-ps every second, plus more during
elections; with the cache it's roughly one ps per cache window.

Signals
-------
``install_signal_handlers()`` installs:
    SIGTERM, SIGINT, SIGHUP → call stop()
    SIGUSR1                 → log a state dump (set install_dump=False
                              to skip)

The dump is also available programmatically via ``dump_state()``.

CLI
---
    heartbeat.py --my-ip 10.0.0.1 --remote-ip 10.0.0.2 [--port 53281]
                 [--managed-process my_server.py] [--debug]
"""

from __future__ import annotations

import argparse
import logging
import signal
import socket
import subprocess
import sys
import threading
import time
from typing import Callable, Optional

log = logging.getLogger("heartbeat")


def _close_socket(sock: Optional[socket.socket]) -> None:
    """Best-effort shutdown and close. Safe with None."""
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


class Heartbeat:
    """Two-node failover daemon with deterministic election."""

    # Wire protocol
    PROTO_QUERY_STATE = b"g"
    PROTO_NOTIFY_PRIMARY = b"P"
    PROTO_NOTIFY_SECONDARY = b"S"
    PROTO_GOODBYE = b"X"  # peer announces graceful departure

    # States
    STATE_PRIMARY = "P"
    STATE_SECONDARY = "S"
    STATE_UNKNOWN = "U"

    def __init__(
        self,
        my_ip: str,
        peer_ip: str,
        port: int = 53281,
        peer_port: Optional[int] = None,
        *,
        managed_process: str = "my_server.py",
        health_check: Optional[Callable[[], bool]] = None,
        health_check_cache_seconds: float = 1.0,
        on_state_change: Optional[Callable[[str, str], None]] = None,
        heartbeat_interval: float = 1.0,
        heartbeat_timeout: float = 5.0,
        local_check_interval: float = 10.0,
        failover_grace: float = 10.0,
    ) -> None:
        if not isinstance(port, int) or not (1 <= port <= 65535):
            raise ValueError("port must be 1-65535")
        if peer_port is None:
            peer_port = port
        if not isinstance(peer_port, int) or not (1 <= peer_port <= 65535):
            raise ValueError("peer_port must be 1-65535")
        if heartbeat_interval <= 0 or heartbeat_timeout <= 0:
            raise ValueError("intervals must be > 0")
        if heartbeat_timeout <= heartbeat_interval:
            log.warning(
                "heartbeat_timeout (%.1fs) <= heartbeat_interval (%.1fs); "
                "consider increasing the timeout",
                heartbeat_timeout,
                heartbeat_interval,
            )
        if health_check_cache_seconds < 0:
            raise ValueError("health_check_cache_seconds must be >= 0")

        self._my_ip = my_ip
        self._peer_ip = peer_ip
        self._port = port
        self._peer_port = peer_port
        self._managed_process = managed_process
        self._custom_health_check = health_check
        self._health_check_cache_seconds = health_check_cache_seconds
        self._on_state_change = on_state_change
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._local_check_interval = local_check_interval
        self._failover_grace = failover_grace

        # State, protected by _state_lock for compound reads/writes.
        self._state = self.STATE_UNKNOWN
        self._state_lock = threading.Lock()

        self._shutdown = threading.Event()

        # Cached health-check result. Forking ps on every heartbeat (which
        # can fire from multiple threads at once during a reconcile) adds
        # up; cache for `health_check_cache_seconds` to amortize.
        self._health_cache_value: Optional[bool] = None
        self._health_cache_time: float = 0.0
        self._health_cache_lock = threading.Lock()

        # Peer-departed flag, set by _serve_connection when a PROTO_GOODBYE
        # frame arrives. _handle_peer_unreachable checks and clears it to
        # bypass the failover grace period for planned shutdowns.
        self._peer_announced_departure = threading.Event()

        # Listener (server) side.
        self._ssock: Optional[socket.socket] = None
        self._listen_thread: Optional[threading.Thread] = None
        self._reaper_thread: Optional[threading.Thread] = None
        self._workers: list[socket.socket] = []
        self._workers_lock = threading.Lock()
        self._worker_threads: list[threading.Thread] = []
        self._worker_threads_lock = threading.Lock()

        # Outbound (client) side — our connection to the peer.
        self._csock: Optional[socket.socket] = None
        self._csock_lock = threading.Lock()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._last_successful_contact: Optional[float] = None
        self._loop_start_time: Optional[float] = None

    # ------------------------------------------------------------------ #
    # State accessors                                                    #
    # ------------------------------------------------------------------ #

    def get_state(self) -> str:
        with self._state_lock:
            return self._state

    def _fire_state_change(self, old: str, new: str) -> None:
        """Invoke the user-supplied callback. Outside any lock so a slow
        or buggy callback can't block other state operations."""
        if self._on_state_change is None:
            return
        try:
            self._on_state_change(old, new)
        except Exception:
            log.exception("on_state_change callback raised")

    def _set_state(self, new: str, reason: str = "") -> bool:
        """Set state to ``new``. Returns True iff the state actually changed.
        Fires the ``on_state_change`` callback (outside the lock) on a real
        transition. Callback exceptions are caught and logged."""
        with self._state_lock:
            old = self._state
            if old == new:
                return False
            self._state = new
        suffix = f" ({reason})" if reason else ""
        log.info("State: %s -> %s%s", old, new, suffix)
        self._fire_state_change(old, new)
        return True

    def _do_health_check(self) -> bool:
        """The actual probe — uncached. Don't call this directly; go
        through ``_is_local_server_running`` so the result is cached."""
        if self._custom_health_check is not None:
            try:
                return bool(self._custom_health_check())
            except Exception as ex:
                log.warning("health_check raised: %s", ex)
                return False
        try:
            result = subprocess.run(
                ["/bin/ps", "-C", self._managed_process, "--no-heading"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=5.0,
            )
            return bool(result.stdout.strip())
        except Exception as ex:
            log.warning("Unable to check for %s: %s", self._managed_process, ex)
            return False

    # ------------------------------------------------------------------ #
    # Health checks                                                      #
    # ------------------------------------------------------------------ #

    def _is_local_server_running(self) -> bool:
        """Return True if our locally managed server process is alive.

        Result is cached for ``health_check_cache_seconds`` to amortize
        the cost of forking ``ps`` (or running the user-supplied check)
        when the loop and the reconciler hit it back-to-back."""
        # Cache fast-path.
        now = time.monotonic()
        with self._health_cache_lock:
            if (
                self._health_cache_value is not None
                and now - self._health_cache_time < self._health_check_cache_seconds
            ):
                return self._health_cache_value

        # Cache miss: run the actual probe outside the lock so concurrent
        # callers don't serialize on subprocess.run / the user's callable.
        result = self._do_health_check()

        with self._health_cache_lock:
            # Use a fresh `now` so the cache reflects when the *probe*
            # completed, not when the cache miss was detected.
            self._health_cache_value = result
            self._health_cache_time = time.monotonic()
        return result

    def _check_local_server(self) -> None:
        """Demote to Secondary if we're Primary but the local server is down."""
        if self._is_local_server_running():
            return
        # Read state under the lock; defer mutation to _set_state so the
        # callback fires consistently with all other transitions.
        with self._state_lock:
            current = self._state
        if current != self.STATE_PRIMARY:
            return
        self._set_state(
            self.STATE_SECONDARY,
            reason=f"{self._managed_process} not running",
        )

    def _serve_connection(self, client_sock: socket.socket) -> None:
        """Handle a single peer connection until close or shutdown."""
        try:
            while not self._shutdown.is_set():
                try:
                    cmd = client_sock.recv(1)
                except socket.timeout:
                    continue
                except OSError as ex:
                    log.debug("Peer connection error: %s", ex)
                    return

                if not cmd:
                    log.debug("Peer closed connection")
                    return

                if cmd == self.PROTO_QUERY_STATE:
                    state = self.get_state()
                    try:
                        client_sock.sendall(state.encode())
                    except OSError as ex:
                        log.debug("Failed to send state: %s", ex)
                        return
                elif cmd == self.PROTO_NOTIFY_PRIMARY:
                    self._set_state(self.STATE_PRIMARY, reason="peer notification")
                elif cmd == self.PROTO_NOTIFY_SECONDARY:
                    self._set_state(self.STATE_SECONDARY, reason="peer notification")
                elif cmd == self.PROTO_GOODBYE:
                    log.info("Peer announced graceful departure")
                    self._peer_announced_departure.set()
                    return  # close this connection; peer is going away
                else:
                    log.warning("Unknown command %r — closing connection", cmd)
                    return
        finally:
            with self._workers_lock:
                try:
                    self._workers.remove(client_sock)
                except ValueError:
                    pass
            _close_socket(client_sock)

    # ------------------------------------------------------------------ #
    # Listener side                                                      #
    # ------------------------------------------------------------------ #

    def _accept_loop(self) -> None:
        """Accept incoming connections; spawn a per-connection thread."""
        while not self._shutdown.is_set():
            try:
                client_sock, _addr = self._ssock.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._shutdown.is_set():
                    return
                log.exception("accept() failed; backing off")
                time.sleep(0.5)
                continue
            except Exception:
                log.exception("Unexpected error in accept loop")
                time.sleep(0.5)
                continue

            try:
                client_sock.settimeout(self._heartbeat_timeout * 2)
                client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError as ex:
                log.warning("Could not configure accepted socket: %s", ex)

            with self._workers_lock:
                self._workers.append(client_sock)
            t = threading.Thread(
                target=self._serve_connection,
                args=(client_sock,),
                name="hb-worker",
                daemon=True,
            )
            with self._worker_threads_lock:
                self._worker_threads.append(t)
            t.start()

    def _reaper_loop(self) -> None:
        """Periodically drop references to finished worker threads."""
        while not self._shutdown.wait(10.0):
            with self._worker_threads_lock:
                self._worker_threads = [t for t in self._worker_threads if t.is_alive()]

    # ------------------------------------------------------------------ #
    # Heartbeat sender (client side)                                     #
    # ------------------------------------------------------------------ #

    def _connect_locked(self) -> Optional[socket.socket]:
        """Open a fresh client socket to the peer. Caller holds _csock_lock."""
        sock: Optional[socket.socket] = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self._heartbeat_timeout)
            sock.connect((self._peer_ip, self._peer_port))
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return sock
        except OSError as ex:
            log.debug(
                "Cannot connect to peer %s:%d: %s",
                self._peer_ip,
                self._peer_port,
                ex,
            )
            _close_socket(sock)
            return None

    def _invalidate_csock(self, sock: socket.socket) -> None:
        """Close ``sock`` and clear ``self._csock`` if it still points at it.
        Acquires ``_csock_lock`` briefly; closes outside the lock."""
        with self._csock_lock:
            if self._csock is sock:
                self._csock = None
        _close_socket(sock)

    def _handle_peer_unreachable(self) -> None:
        """Peer is unreachable. Claim Primary if grace period has elapsed
        AND our local server is running. If the peer sent ``PROTO_GOODBYE``
        (graceful shutdown), bypass the grace period entirely."""
        if self._peer_announced_departure.is_set():
            # Reset so the next involuntary disconnect uses the grace path.
            self._peer_announced_departure.clear()
            log.info("Peer announced departure — bypassing failover grace")
            elapsed_ok = True
        else:
            if self._last_successful_contact is None:
                elapsed = time.monotonic() - (self._loop_start_time or time.monotonic())
            else:
                elapsed = time.monotonic() - self._last_successful_contact
            elapsed_ok = elapsed >= self._failover_grace
            if not elapsed_ok:
                log.debug(
                    "Peer unreachable; %.1fs since last contact (grace=%.1fs)",
                    elapsed,
                    self._failover_grace,
                )
                return

        with self._state_lock:
            current = self._state
            if current == self.STATE_PRIMARY:
                return  # already P, nothing to do

        if not self._is_local_server_running():
            log.warning(
                "Peer unreachable but local server not running — staying %s",
                current,
            )
            return

        self._set_state(
            self.STATE_PRIMARY,
            reason=(
                "peer announced departure"
                if self._peer_announced_departure.is_set()
                else "peer unreachable past grace period"
            ),
        )

    def _i_should_be_primary(self) -> bool:
        """Endpoint-based tiebreaker: lower (ip, port) wins.
        Used only when both nodes are healthy and undecided — peer's reported
        state takes precedence in most cases (see _decide_election)."""
        return (self._my_ip, self._port) < (self._peer_ip, self._peer_port)

    def _decide_election(
        self,
        my: str,
        peer_state: str,
        local_running: bool,
    ) -> tuple[str, str]:
        """Return ``(new_state, reason)`` for an election.

        Called only when (my, peer) is non-stable (i.e. not (P, S) or (S, P)).
        The peer's reported state takes precedence over the endpoint rule —
        an "S" from the peer means "I conceded" (typically because they're
        unhealthy or already elected themselves S), and the endpoint rule
        is only used as a tiebreaker when both nodes are simultaneously
        undecided or both claim the same role.

        Two-node failover policy: STICKY. A live Primary keeps the role
        when a peer rejoins as U/S; we don't disrupt working state for
        cosmetic preference."""
        # Hard rule: an unhealthy node is never Primary.
        if not local_running:
            return (self.STATE_SECONDARY, "local server not running")

        # I'm currently Primary.
        if my == self.STATE_PRIMARY:
            if peer_state == self.STATE_PRIMARY:
                # Split-brain. Endpoint rule decides who yields.
                if self._i_should_be_primary():
                    return (
                        self.STATE_PRIMARY,
                        "split-brain: keeping P (lower endpoint)",
                    )
                return (
                    self.STATE_SECONDARY,
                    "split-brain: yielding (higher endpoint)",
                )
            # Peer is U or S; I'm the only one claiming P. Stay P.
            return (self.STATE_PRIMARY, "asserting P")

        # I'm not Primary. Look at what peer reports.
        if peer_state == self.STATE_PRIMARY:
            return (self.STATE_SECONDARY, "peer is P")

        if peer_state == self.STATE_SECONDARY:
            if my == self.STATE_SECONDARY:
                # Both Secondary — no leader. Endpoint rule.
                if self._i_should_be_primary():
                    return (
                        self.STATE_PRIMARY,
                        "both-S: claiming (lower endpoint)",
                    )
                return (
                    self.STATE_SECONDARY,
                    "both-S: staying (higher endpoint)",
                )
            # I'm U, peer conceded. Take Primary.
            return (self.STATE_PRIMARY, "peer is S, claiming P")

        # Peer is U. Apply endpoint rule.
        if self._i_should_be_primary():
            return (self.STATE_PRIMARY, "election: lower endpoint")
        return (self.STATE_SECONDARY, "election: higher endpoint")

    def _reconcile_state(
        self,
        peer_state: str,
        sock: socket.socket,
    ) -> None:
        """Given peer's reported state, run an election if non-stable.
        Does NOT require any external lock held."""
        # Probe local server outside _state_lock — subprocess calls
        # shouldn't be done while holding contended locks.
        local_running = self._is_local_server_running()

        with self._state_lock:
            my = self._state
            stable = (
                my == self.STATE_PRIMARY and peer_state == self.STATE_SECONDARY
            ) or (my == self.STATE_SECONDARY and peer_state == self.STATE_PRIMARY)
            if stable:
                return
            new_state, reason = self._decide_election(my, peer_state, local_running)

        # Apply outside the lock: _set_state acquires it again briefly and
        # fires on_state_change. Returns False if state was already `new_state`
        # (e.g., a clientThread notification got there first).
        if my == new_state:
            return

        changed = self._set_state(
            new_state,
            reason=f"election: my={my} peer={peer_state} -> {new_state} [{reason}]",
        )

        # Notify peer only when our state actually changed. Sending
        # unconditionally caused a cascade in the both-unhealthy case: each
        # side told the other "you should be P", the clientThread set state
        # to P, and the next election cycle demoted again — flapping forever.
        if not changed:
            return
        peer_should_be = (
            self.STATE_SECONDARY
            if new_state == self.STATE_PRIMARY
            else self.STATE_PRIMARY
        )
        try:
            sock.sendall(peer_should_be.encode())
        except OSError as ex:
            log.warning("Failed to notify peer of election result: %s", ex)

    def _heartbeat_loop(self) -> None:
        """The main heartbeat sender. Periodically:
            1. Check that our local managed server is still up.
            2. Send b"g" to the peer; receive its state byte.
            3. If states aren't complementary, run a deterministic election.
            4. If peer is unreachable past the grace period, claim Primary
               (provided our local server is running).

        ``_csock_lock`` is held only briefly to grab/install the socket
        reference. The actual send/recv runs lock-free, so ``stop()`` can
        close the socket immediately rather than waiting up to
        ``heartbeat_timeout`` seconds for the lock."""
        self._loop_start_time = time.monotonic()
        last_local_check = 0.0

        # Initial state probe: if local server isn't running, start as S.
        if not self._is_local_server_running():
            self._set_state(self.STATE_SECONDARY, reason="local server not running")

        valid_states = {
            self.STATE_PRIMARY,
            self.STATE_SECONDARY,
            self.STATE_UNKNOWN,
        }

        while not self._shutdown.is_set():
            # Periodic local server health check.
            now = time.monotonic()
            if now - last_local_check >= self._local_check_interval:
                self._check_local_server()
                last_local_check = now

            # Take lock briefly: grab existing or open a new connection.
            with self._csock_lock:
                sock = self._csock
                if sock is None:
                    sock = self._connect_locked()
                    if sock is not None:
                        self._csock = sock

            if sock is None:
                self._handle_peer_unreachable()
                if self._shutdown.wait(self._heartbeat_interval):
                    return
                continue

            # I/O: lock-free.
            try:
                sock.sendall(self.PROTO_QUERY_STATE)
                reply = sock.recv(1)
            except socket.timeout:
                log.info(
                    "No heartbeat reply within %.1fs",
                    self._heartbeat_timeout,
                )
                self._invalidate_csock(sock)
                self._handle_peer_unreachable()
                if self._shutdown.wait(self._heartbeat_interval):
                    return
                continue
            except OSError as ex:
                log.info("Heartbeat error: %s", ex)
                self._invalidate_csock(sock)
                if self._shutdown.wait(self._heartbeat_interval):
                    return
                continue

            if not reply:
                log.info("Peer closed the heartbeat connection")
                self._invalidate_csock(sock)
                if self._shutdown.wait(self._heartbeat_interval):
                    return
                continue

            self._last_successful_contact = time.monotonic()
            # Peer is alive again; clear any stale "departed" flag from a
            # previous shutdown cycle so the next involuntary outage
            # honors the grace period.
            self._peer_announced_departure.clear()
            peer_state = reply.decode("ascii", errors="replace")

            # Validate: peer must report one of the three known states.
            # Anything else is a protocol violation; drop the connection.
            if peer_state not in valid_states:
                log.warning(
                    "Peer reported invalid state byte %r — dropping connection",
                    reply,
                )
                self._invalidate_csock(sock)
                if self._shutdown.wait(self._heartbeat_interval):
                    return
                continue

            self._reconcile_state(peer_state, sock)

            if self._shutdown.wait(self._heartbeat_interval):
                return

    # ------------------------------------------------------------------ #
    # Lifecycle                                                          #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Bind, listen, and start the heartbeat sender. Raises on bind failure."""
        if self._ssock is not None:
            raise RuntimeError("heartbeat already started")

        ssock: Optional[socket.socket] = None
        try:
            ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            ssock.bind((self._my_ip, self._port))
            ssock.listen(128)
            ssock.settimeout(1.0)  # so accept() can poll the shutdown flag
        except Exception:
            _close_socket(ssock)
            raise
        self._ssock = ssock

        self._shutdown.clear()
        self._listen_thread = threading.Thread(
            target=self._accept_loop, name="hb-listener", daemon=True
        )
        self._listen_thread.start()
        self._reaper_thread = threading.Thread(
            target=self._reaper_loop, name="hb-reaper", daemon=True
        )
        self._reaper_thread.start()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="hb-sender", daemon=True
        )
        self._heartbeat_thread.start()

    def _notify_peer_goodbye(self) -> None:
        """Open a short-lived socket and send ``PROTO_GOODBYE``. Best
        effort — failure is logged at debug level and otherwise ignored
        (peer will fail over via the grace period instead)."""
        sock: Optional[socket.socket] = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((self._peer_ip, self._peer_port))
            sock.sendall(self.PROTO_GOODBYE)
        except Exception as ex:
            log.debug("Could not send goodbye to peer: %s", ex)
        finally:
            _close_socket(sock)

    def stop(self) -> None:
        """Stop accepting connections, close all sockets, join threads. Idempotent.

        Best-effort: sends ``PROTO_GOODBYE`` to the peer over a fresh
        short-lived socket so the peer can fail over without waiting for
        the grace period. The notification is sent on a separate socket
        (rather than via ``self._csock``) to avoid contention with the
        heartbeat loop's lock."""
        if self._shutdown.is_set():
            return  # idempotent: already stopping or stopped
        self._shutdown.set()

        # Notify peer before tearing anything down.
        self._notify_peer_goodbye()

        _close_socket(self._ssock)
        self._ssock = None

        with self._csock_lock:
            _close_socket(self._csock)
            self._csock = None

        with self._workers_lock:
            workers = list(self._workers)
            self._workers.clear()
        for w in workers:
            _close_socket(w)

        for t in (
            self._listen_thread,
            self._reaper_thread,
            self._heartbeat_thread,
        ):
            if t is not None and t.is_alive():
                t.join(timeout=5.0)
        self._listen_thread = None
        self._reaper_thread = None
        self._heartbeat_thread = None

        with self._worker_threads_lock:
            threads = list(self._worker_threads)
            self._worker_threads.clear()
        for t in threads:
            t.join(timeout=1.0)

    def install_signal_handlers(
        self,
        signals: Optional[tuple[int, ...]] = None,
        install_dump: bool = True,
    ) -> None:
        """Install handlers that call ``stop()`` on the given signals.
        Defaults to SIGTERM, SIGINT, SIGHUP. Main thread only.

        If ``install_dump`` is True (default) and SIGUSR1 is available,
        also installs a handler that writes a state dump to the log.
        Useful for diagnosing a confused node at 3am without having to
        attach a debugger."""
        if signals is None:
            signals = tuple(
                s
                for s in (
                    getattr(signal, "SIGTERM", None),
                    getattr(signal, "SIGINT", None),
                    getattr(signal, "SIGHUP", None),
                )
                if s is not None
            )

        def stop_handler(signum: int, _frame) -> None:
            log.info("Signal %d received; shutting down", signum)
            self.stop()

        for sig in signals:
            try:
                signal.signal(sig, stop_handler)
            except (ValueError, OSError) as ex:
                log.warning("Could not install handler for signal %d: %s", sig, ex)

        if install_dump:
            sigusr1 = getattr(signal, "SIGUSR1", None)
            if sigusr1 is not None:

                def dump_handler(signum: int, _frame) -> None:
                    log.info("SIGUSR1 received — state dump:\n%s", self.dump_state())

                try:
                    signal.signal(sigusr1, dump_handler)
                except (ValueError, OSError) as ex:
                    log.warning("Could not install SIGUSR1 dump handler: %s", ex)

    def dump_state(self) -> str:
        """Multi-line summary of current internals, intended for ops
        diagnostics (logged on SIGUSR1, also callable directly)."""
        with self._workers_lock:
            num_workers = len(self._workers)
        with self._worker_threads_lock:
            num_alive = sum(1 for t in self._worker_threads if t.is_alive())
        last_contact = self._last_successful_contact
        if last_contact is not None:
            ago = f"{time.monotonic() - last_contact:.1f}s ago"
        else:
            ago = "never"
        # Health check goes through the cache so this is cheap on SIGUSR1.
        try:
            healthy = self._is_local_server_running()
        except Exception as ex:
            healthy = f"<probe failed: {ex}>"
        lines = [
            f"  state:                  {self.get_state()}",
            f"  my endpoint:            {self._my_ip}:{self._port}",
            f"  peer endpoint:          {self._peer_ip}:{self._peer_port}",
            f"  csock connected:        {self._csock is not None}",
            f"  last successful contact:{ago}",
            f"  local server healthy:   {healthy}",
            f"  peer announced depart:  {self._peer_announced_departure.is_set()}",
            f"  shutdown set:           {self._shutdown.is_set()}",
            f"  active worker conns:    {num_workers}",
            f"  live worker threads:    {num_alive}",
        ]
        return "\n".join(lines)

    def wait_for_shutdown(self, timeout: Optional[float] = None) -> bool:
        return self._shutdown.wait(timeout=timeout)


# --------------------------------------------------------------------------- #
# CLI entry point                                                             #
# --------------------------------------------------------------------------- #


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="heartbeat")
    parser.add_argument(
        "-m",
        "--my-ip",
        required=True,
        help="IP to bind the heartbeat listener on this host",
    )
    parser.add_argument(
        "-r",
        "--remote-ip",
        required=True,
        help="IP of the peer's heartbeat listener",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=53281,
        help="TCP port for the heartbeat protocol (default: 53281)",
    )
    parser.add_argument(
        "--managed-process",
        default="my_server.py",
        help="Process name to monitor; demote to Secondary if missing",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        format="%(asctime)s %(levelname)s\t%(message)s",
        level=logging.DEBUG if args.debug else logging.INFO,
    )

    hb = Heartbeat(
        my_ip=args.my_ip,
        peer_ip=args.remote_ip,
        port=args.port,
        managed_process=args.managed_process,
    )

    try:
        hb.start()
    except OSError as ex:
        log.critical("Unable to start heartbeat listener: %s", ex)
        return 2

    hb.install_signal_handlers()
    hb.wait_for_shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
