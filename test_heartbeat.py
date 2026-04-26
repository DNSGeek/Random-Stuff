"""Smoke tests for heartbeat.py. Exercises:
- Two-node election convergence (P + S, not split)
- Steady-state stability (no spurious flapping)
- Deterministic rule (lower endpoint always wins)
- Failover when peer dies (after grace period)
- Re-join after a previous Primary comes back
- Local-server-down node never claims Primary
- Signal handler triggers clean stop
"""

import logging
import os
import signal
import sys
import threading
from time import monotonic, sleep

sys.path.insert(0, "/home/claude")
from heartbeat import Heartbeat

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s"
)


class _PortBox:
    def __init__(self, start=53400):
        self.next = start

    def pair(self):
        a, b = self.next, self.next + 1
        self.next += 2
        return a, b


PORTS = _PortBox()


def _wait_for(predicate, timeout=15.0, interval=0.1):
    """Spin until predicate() returns truthy or timeout. Returns last value."""
    deadline = monotonic() + timeout
    last = None
    while monotonic() < deadline:
        last = predicate()
        if last:
            return last
        sleep(interval)
    return last


def _make_pair(port_a, port_b, **kwargs):
    """Two heartbeats wired to each other on 127.0.0.1 with different ports.
    Tight intervals so tests don't take forever."""
    defaults = dict(
        health_check=lambda: True,
        heartbeat_interval=0.2,
        heartbeat_timeout=1.0,
        local_check_interval=10.0,
        failover_grace=1.5,
    )
    defaults.update(kwargs)

    a = Heartbeat(
        "127.0.0.1", "127.0.0.1", port=port_a, peer_port=port_b, **defaults
    )
    b = Heartbeat(
        "127.0.0.1", "127.0.0.1", port=port_b, peer_port=port_a, **defaults
    )
    return a, b


def test_two_nodes_converge():
    """Both nodes start, should converge to one P + one S."""
    print("\n--- test_two_nodes_converge ---")
    pa, pb = PORTS.pair()
    a, b = _make_pair(pa, pb)
    a.start()
    b.start()
    try:
        ok = _wait_for(
            lambda: {a.get_state(), b.get_state()} == {"P", "S"}, timeout=10
        )
        assert ok, f"never converged: a={a.get_state()} b={b.get_state()}"
        # Lower port should be Primary by deterministic rule.
        assert (
            a.get_state() == "P" and b.get_state() == "S"
        ), f"wrong winner: a(port {pa})={a.get_state()} b(port {pb})={b.get_state()}"
        print(f"  OK: converged to a=P (port {pa}), b=S (port {pb})")
    finally:
        a.stop()
        b.stop()


def test_steady_state_is_stable():
    """Once converged, states shouldn't flap."""
    print("\n--- test_steady_state_is_stable ---")
    pa, pb = PORTS.pair()
    a, b = _make_pair(pa, pb)
    a.start()
    b.start()
    try:
        assert _wait_for(lambda: {a.get_state(), b.get_state()} == {"P", "S"})
        snapshots = []
        for _ in range(15):  # 3s of observations
            snapshots.append((a.get_state(), b.get_state()))
            sleep(0.2)
        unique = set(snapshots)
        assert unique == {("P", "S")}, f"flapped: {unique}"
        print(f"  OK: held ('P', 'S') across {len(snapshots)} snapshots")
    finally:
        a.stop()
        b.stop()


def test_failover_when_peer_dies():
    """Stop the Secondary's view of the Primary — Secondary should claim P
    after the failover grace period."""
    print("\n--- test_failover_when_peer_dies ---")
    pa, pb = PORTS.pair()
    a, b = _make_pair(pa, pb, failover_grace=1.0)
    a.start()
    b.start()
    try:
        assert _wait_for(lambda: {a.get_state(), b.get_state()} == {"P", "S"})
        # a is P, b is S. Stop a; b should become P.
        a.stop()
        ok = _wait_for(lambda: b.get_state() == "P", timeout=8)
        assert ok, f"b never claimed P; still {b.get_state()}"
        print(f"  OK: surviving node b became P after a stopped")
    finally:
        b.stop()


def test_rejoin_after_failover():
    """Sticky failover: the surviving Primary keeps Primary when the old
    one comes back. The rejoining node accepts Secondary."""
    print("\n--- test_rejoin_after_failover ---")
    pa, pb = PORTS.pair()
    a, b = _make_pair(pa, pb, failover_grace=1.0)
    a.start()
    b.start()
    try:
        assert _wait_for(lambda: {a.get_state(), b.get_state()} == {"P", "S"})
        # a was P. Stop a. b should become P.
        a.stop()
        assert _wait_for(lambda: b.get_state() == "P", timeout=8)
        # Bring a back. b is currently P; sticky policy says b stays P.
        a2 = Heartbeat(
            "127.0.0.1",
            "127.0.0.1",
            port=pa,
            peer_port=pb,
            health_check=lambda: True,
            heartbeat_interval=0.2,
            heartbeat_timeout=1.0,
            failover_grace=1.0,
        )
        a2.start()
        try:
            ok = _wait_for(
                lambda: a2.get_state() == "S" and b.get_state() == "P",
                timeout=10,
            )
            assert (
                ok
            ), f"didn't reconcile: a2={a2.get_state()} b={b.get_state()}"
            print(
                "  OK: rejoining node accepted Secondary; b kept Primary (sticky)"
            )
        finally:
            a2.stop()
    finally:
        b.stop()


def test_unhealthy_node_never_becomes_primary():
    """If a node's local server is down, election should give it S even when
    it has the lower endpoint."""
    print("\n--- test_unhealthy_node_never_becomes_primary ---")
    pa, pb = PORTS.pair()
    # a (lower port) is unhealthy — should NOT win. b (higher port) should be P.
    a = Heartbeat(
        "127.0.0.1",
        "127.0.0.1",
        port=pa,
        peer_port=pb,
        health_check=lambda: False,  # always unhealthy
        heartbeat_interval=0.2,
        heartbeat_timeout=1.0,
        failover_grace=1.0,
    )
    b = Heartbeat(
        "127.0.0.1",
        "127.0.0.1",
        port=pb,
        peer_port=pa,
        health_check=lambda: True,
        heartbeat_interval=0.2,
        heartbeat_timeout=1.0,
        failover_grace=1.0,
    )
    a.start()
    b.start()
    try:
        ok = _wait_for(
            lambda: a.get_state() == "S" and b.get_state() == "P",
            timeout=10,
        )
        assert ok, f"unhealthy node won?: a={a.get_state()} b={b.get_state()}"
        print(f"  OK: unhealthy a stayed S, healthy b became P")
    finally:
        a.stop()
        b.stop()


def test_both_unhealthy_no_cascade():
    """When both nodes are unhealthy, both should sit at S without flapping.
    The previous code sent unconditional 'you should be P' notifications,
    which caused both sides to ping-pong between P and S forever."""
    print("\n--- test_both_unhealthy_no_cascade ---")
    pa, pb = PORTS.pair()
    a = Heartbeat(
        "127.0.0.1",
        "127.0.0.1",
        port=pa,
        peer_port=pb,
        health_check=lambda: False,
        heartbeat_interval=0.2,
        heartbeat_timeout=1.0,
        failover_grace=10.0,
    )
    b = Heartbeat(
        "127.0.0.1",
        "127.0.0.1",
        port=pb,
        peer_port=pa,
        health_check=lambda: False,
        heartbeat_interval=0.2,
        heartbeat_timeout=1.0,
        failover_grace=10.0,
    )
    a.start()
    b.start()
    try:
        # Give them time to settle and run several heartbeat cycles
        sleep(2.0)
        snapshots = [
            (a.get_state(), b.get_state())
            for _ in range(10)
            for _ in [sleep(0.1)]
        ]
        unique = set(snapshots)
        assert unique == {("S", "S")}, f"flapped or wrong state: {unique}"
        print(
            f"  OK: both stayed S across {len(snapshots)} snapshots, no cascade"
        )
    finally:
        a.stop()
        b.stop()


def test_invalid_peer_byte_drops_connection():
    """If peer sends a junk state byte, we should drop the connection."""
    print("\n--- test_invalid_peer_byte_drops_connection ---")
    pa, pb = PORTS.pair()
    # Stand up a fake peer that responds to b"g" with junk.
    import socket as _sock

    listener = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    listener.setsockopt(_sock.SOL_SOCKET, _sock.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", pb))
    listener.listen(1)

    received = []

    def fake_peer():
        try:
            conn, _ = listener.accept()
            cmd = conn.recv(1)
            received.append(cmd)
            conn.sendall(b"X")  # invalid state byte
            # Wait briefly to see whether the client closes
            conn.settimeout(2.0)
            try:
                trailing = conn.recv(64)
                received.append(("trailing", trailing))
            except _sock.timeout:
                received.append("timeout")
            conn.close()
        finally:
            listener.close()

    threading.Thread(target=fake_peer, daemon=True).start()

    a = Heartbeat(
        "127.0.0.1",
        "127.0.0.1",
        port=pa,
        peer_port=pb,
        health_check=lambda: True,
        heartbeat_interval=0.2,
        heartbeat_timeout=1.0,
        failover_grace=10.0,
    )
    a.start()
    try:
        ok = _wait_for(lambda: received and received[0] == b"g", timeout=5)
        assert ok, "fake peer never received query"
        sleep(0.5)
        # Real proof: a's csock should have been dropped after the bad byte
        with a._csock_lock:
            assert (
                a._csock is None
            ), "expected csock to be invalidated after junk byte"
        print(f"  OK: junk peer byte caused connection drop")
    finally:
        a.stop()


def test_signal_triggers_stop():
    """Send ourselves SIGUSR1; heartbeat should stop cleanly. Disable the
    SIGUSR1 dump handler so this test can repurpose the signal."""
    print("\n--- test_signal_triggers_stop ---")
    pa, pb = PORTS.pair()
    a, b = _make_pair(pa, pb)
    a.start()
    b.start()
    try:
        sig = getattr(signal, "SIGUSR1", None)
        if sig is None:
            print("  SKIP: SIGUSR1 not available")
            return
        # install_dump=False so SIGUSR1 isn't claimed by the dump handler
        a.install_signal_handlers(signals=(sig,), install_dump=False)

        def fire():
            sleep(0.3)
            os.kill(os.getpid(), sig)

        threading.Thread(target=fire, daemon=True).start()
        got = a.wait_for_shutdown(timeout=5.0)
        assert got, "shutdown didn't fire within 5s"
        assert a._ssock is None, "stop didn't run"
        print("  OK: signal triggered stop")
    finally:
        a.stop()
        b.stop()


def test_state_change_callback_fires():
    """on_state_change should fire on every transition with (old, new)."""
    print("\n--- test_state_change_callback_fires ---")
    pa, pb = PORTS.pair()
    transitions_a: list[tuple[str, str]] = []
    transitions_b: list[tuple[str, str]] = []

    a = Heartbeat(
        "127.0.0.1",
        "127.0.0.1",
        port=pa,
        peer_port=pb,
        health_check=lambda: True,
        heartbeat_interval=0.2,
        heartbeat_timeout=1.0,
        failover_grace=1.0,
        on_state_change=lambda o, n: transitions_a.append((o, n)),
    )
    b = Heartbeat(
        "127.0.0.1",
        "127.0.0.1",
        port=pb,
        peer_port=pa,
        health_check=lambda: True,
        heartbeat_interval=0.2,
        heartbeat_timeout=1.0,
        failover_grace=1.0,
        on_state_change=lambda o, n: transitions_b.append((o, n)),
    )
    a.start()
    b.start()
    try:
        assert _wait_for(lambda: {a.get_state(), b.get_state()} == {"P", "S"})
        sleep(0.3)
        # Each side should have transitioned at least once (U -> P or U -> S).
        assert transitions_a, "a got no callback"
        assert transitions_b, "b got no callback"
        # Final transitions should land at the converged states.
        assert transitions_a[-1][1] == a.get_state()
        assert transitions_b[-1][1] == b.get_state()
        print(
            f"  OK: callbacks fired (a: {transitions_a}, b: {transitions_b})"
        )
    finally:
        a.stop()
        b.stop()


def test_callback_exception_does_not_crash():
    """A buggy callback must not take down the daemon."""
    print("\n--- test_callback_exception_does_not_crash ---")
    pa, pb = PORTS.pair()

    def angry_callback(old: str, new: str) -> None:
        raise RuntimeError("intentionally broken callback")

    a = Heartbeat(
        "127.0.0.1",
        "127.0.0.1",
        port=pa,
        peer_port=pb,
        health_check=lambda: True,
        heartbeat_interval=0.2,
        heartbeat_timeout=1.0,
        failover_grace=1.0,
        on_state_change=angry_callback,
    )
    b = Heartbeat(
        "127.0.0.1",
        "127.0.0.1",
        port=pb,
        peer_port=pa,
        health_check=lambda: True,
        heartbeat_interval=0.2,
        heartbeat_timeout=1.0,
        failover_grace=1.0,
    )
    a.start()
    b.start()
    try:
        ok = _wait_for(
            lambda: {a.get_state(), b.get_state()} == {"P", "S"},
            timeout=8,
        )
        assert (
            ok
        ), f"didn't converge despite buggy callback: a={a.get_state()} b={b.get_state()}"
        print("  OK: convergence succeeded despite raising callback")
    finally:
        a.stop()
        b.stop()


def test_graceful_goodbye_skips_grace_period():
    """When the Primary stops cleanly, the Secondary should promote
    near-instantly via PROTO_GOODBYE, not wait for the failover grace."""
    print("\n--- test_graceful_goodbye_skips_grace_period ---")
    pa, pb = PORTS.pair()
    # Long failover_grace so we can clearly distinguish "instant via goodbye"
    # from "after grace expires".
    GRACE = 8.0
    a, b = _make_pair(pa, pb, failover_grace=GRACE)
    a.start()
    b.start()
    try:
        assert _wait_for(lambda: {a.get_state(), b.get_state()} == {"P", "S"})
        # a is Primary. Stop it cleanly.
        t0 = monotonic()
        a.stop()
        # b should promote within a couple seconds, not after GRACE.
        ok = _wait_for(lambda: b.get_state() == "P", timeout=GRACE / 2)
        elapsed = monotonic() - t0
        assert (
            ok
        ), f"b never promoted within {GRACE/2:.1f}s; elapsed={elapsed:.1f}s"
        assert elapsed < GRACE, (
            f"b promoted at {elapsed:.1f}s but grace was {GRACE}s — "
            f"goodbye notification didn't bypass grace"
        )
        print(f"  OK: b promoted in {elapsed:.2f}s (grace was {GRACE}s)")
    finally:
        b.stop()


def test_health_check_caching():
    """Repeated _is_local_server_running calls within the cache TTL should
    NOT re-invoke the underlying check."""
    print("\n--- test_health_check_caching ---")
    pa, pb = PORTS.pair()
    call_count = [0]

    def counting_check() -> bool:
        call_count[0] += 1
        return True

    a = Heartbeat(
        "127.0.0.1",
        "127.0.0.1",
        port=pa,
        peer_port=pb,
        health_check=counting_check,
        health_check_cache_seconds=2.0,
        heartbeat_interval=0.1,
        heartbeat_timeout=1.0,
        failover_grace=10.0,
    )
    # Don't start a partner — we just want to count probe invocations.
    a.start()
    try:
        sleep(1.0)  # ~10 heartbeat iterations
        # With caching at 2.0s, expect at most 1-2 actual invocations
        # (initial probe + maybe one cache refresh).
        assert (
            call_count[0] <= 3
        ), f"expected <=3 probes in 1s with 2s cache, got {call_count[0]}"
        print(f"  OK: {call_count[0]} probes in 1s (~10 heartbeats, cache=2s)")
    finally:
        a.stop()


def test_dump_state_format():
    """dump_state should produce a multi-line string with the expected fields."""
    print("\n--- test_dump_state_format ---")
    pa, pb = PORTS.pair()
    a, b = _make_pair(pa, pb)
    a.start()
    b.start()
    try:
        assert _wait_for(lambda: {a.get_state(), b.get_state()} == {"P", "S"})
        dump = a.dump_state()
        for field in (
            "state:",
            "my endpoint:",
            "peer endpoint:",
            "csock connected:",
            "last successful contact:",
            "local server healthy:",
            "shutdown set:",
        ):
            assert field in dump, f"dump missing field {field!r}: {dump}"
        print(f"  OK: dump_state contains all expected fields")
    finally:
        a.stop()
        b.stop()


def test_no_split_brain_under_simultaneous_start():
    """Run the convergence test 10 times. The OLD random-byte protocol would
    have produced split-brain (both P) in roughly 1/5 of runs. The new
    deterministic protocol should be 0/10."""
    print("\n--- test_no_split_brain_under_simultaneous_start ---")
    splits = 0
    for i in range(10):
        pa, pb = PORTS.pair()
        a, b = _make_pair(pa, pb)
        a.start()
        b.start()
        try:
            ok = _wait_for(
                lambda: {a.get_state(), b.get_state()} == {"P", "S"},
                timeout=8,
            )
            if not ok:
                splits += 1
                print(
                    f"    run {i}: did not converge: a={a.get_state()} b={b.get_state()}"
                )
            elif a.get_state() == b.get_state() == "P":
                splits += 1
                print(f"    run {i}: SPLIT-BRAIN")
        finally:
            a.stop()
            b.stop()
    assert splits == 0, f"{splits}/10 runs split or failed to converge"
    print("  OK: 10/10 runs converged to (P, S) with no split-brain")


if __name__ == "__main__":
    test_two_nodes_converge()
    test_steady_state_is_stable()
    test_failover_when_peer_dies()
    test_rejoin_after_failover()
    test_unhealthy_node_never_becomes_primary()
    test_both_unhealthy_no_cascade()
    test_invalid_peer_byte_drops_connection()
    test_signal_triggers_stop()
    test_state_change_callback_fires()
    test_callback_exception_does_not_crash()
    test_graceful_goodbye_skips_grace_period()
    test_health_check_caching()
    test_dump_state_format()
    test_no_split_brain_under_simultaneous_start()
    print("\nAll smoke tests passed.")
