"""
test_quova.py — Unit tests for the Python 3 Quova emulator.

Run with:  python -m pytest test_quova.py -v
"""

from __future__ import annotations

import socket
import sys
import threading
import types
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Stub out hard-to-import native dependencies so tests run without real libs.
# ---------------------------------------------------------------------------

# pygeoip stub
_pygeoip_mod = types.ModuleType("pygeoip")


class _GeoIPStub:
    def __init__(self, path: str) -> None:
        pass

    def record_by_addr(self, ip: str) -> dict[str, Any] | None:
        return None


_pygeoip_mod.GeoIP = _GeoIPStub  # type: ignore[attr-defined]
sys.modules.setdefault("pygeoip", _pygeoip_mod)

# daemon stub
_daemon_mod = types.ModuleType("daemon")


class _DaemonContextStub:
    def __enter__(self) -> _DaemonContextStub:
        return self

    def __exit__(self, *args: object) -> None:
        pass


_daemon_mod.DaemonContext = _DaemonContextStub  # type: ignore[attr-defined]
sys.modules.setdefault("daemon", _daemon_mod)

import quova  # noqa: E402  (must come after stubs)

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

SAMPLE_IPINFO: quova.GeoIPRecord = {
    "country_name": "United States",
    "region_name": "Kansas",
    "city": "Lebanon",
    "country_code": "US",
    "dma_code": "616",
    "area_code": "785",
    "postal_code": "66952",
    "metro_code": "616",
    "latitude": "39.8333",
    "longitude": "-98.5833",
}

# ---------------------------------------------------------------------------
# decode_quova
# ---------------------------------------------------------------------------


class TestDecodeQuova(unittest.TestCase):

    def test_valid_packet_extracts_last_four_bytes(self) -> None:
        packet: bytes = b"\x00" * 10 + bytes([1, 2, 3, 4])
        self.assertEqual(quova.decode_quova(packet), "1.2.3.4")

    def test_exactly_four_bytes(self) -> None:
        self.assertEqual(
            quova.decode_quova(bytes([192, 168, 0, 1])), "192.168.0.1"
        )

    def test_too_short_returns_default(self) -> None:
        self.assertEqual(quova.decode_quova(b"\x01\x02\x03"), quova.DEFAULT_IP)

    def test_empty_bytes_returns_default(self) -> None:
        self.assertEqual(quova.decode_quova(b""), quova.DEFAULT_IP)

    def test_all_zeros(self) -> None:
        self.assertEqual(quova.decode_quova(b"\x00\x00\x00\x00"), "0.0.0.0")

    def test_all_255(self) -> None:
        self.assertEqual(
            quova.decode_quova(b"\xff\xff\xff\xff"), "255.255.255.255"
        )


# ---------------------------------------------------------------------------
# encode_quova
# ---------------------------------------------------------------------------


class TestEncodeQuova(unittest.TestCase):

    def setUp(self) -> None:
        quova.statcounter = 0

    def test_returns_bytes(self) -> None:
        result: bytes = quova.encode_quova(SAMPLE_IPINFO, "1.2.3.4")
        self.assertIsInstance(result, bytes)

    def test_increments_statcounter(self) -> None:
        quova.encode_quova(SAMPLE_IPINFO, "1.2.3.4")
        self.assertEqual(quova.statcounter, 1)

    def test_country_name_in_response(self) -> None:
        result: bytes = quova.encode_quova(SAMPLE_IPINFO, "1.2.3.4")
        self.assertIn(b"united states", result)

    def test_city_in_response(self) -> None:
        result: bytes = quova.encode_quova(SAMPLE_IPINFO, "1.2.3.4")
        self.assertIn(b"lebanon", result)

    def test_latitude_in_response(self) -> None:
        result: bytes = quova.encode_quova(SAMPLE_IPINFO, "1.2.3.4")
        self.assertIn(b"39.8333", result)

    def test_longitude_in_response(self) -> None:
        result: bytes = quova.encode_quova(SAMPLE_IPINFO, "1.2.3.4")
        self.assertIn(b"-98.5833", result)

    def test_header_starts_with_16_null_bytes(self) -> None:
        result: bytes = quova.encode_quova(SAMPLE_IPINFO, "1.2.3.4")
        self.assertEqual(result[:16], b"\x00" * 16)

    def test_error_response_on_bad_ipinfo(self) -> None:
        result: bytes = quova.encode_quova(None, "bad")  # type: ignore[arg-type]
        self.assertEqual(result, quova._ERROR_RESPONSE)

    def test_empty_ipinfo_uses_empty_strings(self) -> None:
        result: bytes = quova.encode_quova(quova.GeoIPRecord(), "1.2.3.4")
        self.assertIsInstance(result, bytes)
        self.assertGreater(len(result), 32)

    def test_unicode_city_name(self) -> None:
        info: quova.GeoIPRecord = {**SAMPLE_IPINFO, "city": "Düsseldorf"}
        result: bytes = quova.encode_quova(info, "1.2.3.4")
        self.assertIn("düsseldorf".encode("utf-8"), result)

    def test_counter_increments_per_call(self) -> None:
        for _ in range(5):
            quova.encode_quova(SAMPLE_IPINFO, "1.2.3.4")
        self.assertEqual(quova.statcounter, 5)


# ---------------------------------------------------------------------------
# _append_field
# ---------------------------------------------------------------------------


class TestAppendField(unittest.TestCase):

    def test_appends_sep_then_field_id_and_length(self) -> None:
        buf: bytearray = bytearray()
        result: bytearray = quova._append_field(buf, b"\x06", "hello")
        self.assertIn(b"hello", result)
        self.assertTrue(result.startswith(quova._SEP))

    def test_value_lowercased(self) -> None:
        buf: bytearray = bytearray()
        result: bytearray = quova._append_field(buf, b"\x03", "UNITED STATES")
        self.assertIn(b"united states", result)

    def test_empty_value(self) -> None:
        buf: bytearray = bytearray()
        result: bytearray = quova._append_field(buf, b"\x03", "")
        self.assertIsInstance(result, bytearray)


# ---------------------------------------------------------------------------
# process_connection — unit-level socket mocking
# ---------------------------------------------------------------------------


class TestProcessConnection(unittest.TestCase):

    def _make_mock_sock(self, recv_data: list[bytes]) -> MagicMock:
        """Return a socket mock that yields *recv_data* chunks in order."""
        sock: MagicMock = MagicMock(spec=socket.socket)
        sock.recv.side_effect = recv_data
        return sock

    def test_stats_command_sends_counter_and_closes(self) -> None:
        quova.statcounter = 42
        sock: MagicMock = self._make_mock_sock([b"stats-padded"])
        geoip_mock: MagicMock = MagicMock()
        with patch("quova.pygeoip.GeoIP", return_value=geoip_mock):
            quova.process_connection(sock)
        sent: bytes = sock.sendall.call_args[0][0]
        self.assertIn(b"42", sent)

    def test_empty_recv_closes_cleanly(self) -> None:
        sock: MagicMock = self._make_mock_sock([b""])
        geoip_mock: MagicMock = MagicMock()
        with patch("quova.pygeoip.GeoIP", return_value=geoip_mock):
            quova.process_connection(sock)
        sock.shutdown.assert_called()

    def test_normal_lookup_sends_response(self) -> None:
        packet: bytes = b"\x00" * 10 + bytes([8, 8, 8, 8])
        sock: MagicMock = self._make_mock_sock([packet, b""])
        geoip_mock: MagicMock = MagicMock()
        geoip_mock.record_by_addr.return_value = SAMPLE_IPINFO
        with patch("quova.pygeoip.GeoIP", return_value=geoip_mock):
            quova.process_connection(sock)
        sock.sendall.assert_called_once()
        response: bytes = sock.sendall.call_args[0][0]
        self.assertIn(b"united states", response)

    def test_none_geoip_record_falls_back_to_default(self) -> None:
        packet: bytes = b"\x00" * 10 + bytes([255, 255, 255, 1])
        sock: MagicMock = self._make_mock_sock([packet, b""])
        geoip_mock: MagicMock = MagicMock()
        # First call (unknown IP) → None; second call (DEFAULT_IP) → fallback record.
        geoip_mock.record_by_addr.side_effect = [None, SAMPLE_IPINFO]
        with patch("quova.pygeoip.GeoIP", return_value=geoip_mock):
            quova.process_connection(sock)
        sock.sendall.assert_called_once()

    def test_db_open_failure_closes_socket(self) -> None:
        sock: MagicMock = MagicMock(spec=socket.socket)
        with patch("quova.pygeoip.GeoIP", side_effect=OSError("no db")):
            quova.process_connection(sock)
        sock.shutdown.assert_called()


# ---------------------------------------------------------------------------
# Integration smoke test — real loopback socket
# ---------------------------------------------------------------------------


class TestIntegration(unittest.TestCase):
    """Spin up a real listening socket, connect, and check round-trip."""

    listen_sock: socket.socket
    port: int
    geoip_mock: MagicMock

    def setUp(self) -> None:
        quova.statcounter = 0
        self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listen_sock.bind(("127.0.0.1", 0))  # OS picks a free port
        self.listen_sock.listen(1)
        self.port = self.listen_sock.getsockname()[1]

        self.geoip_mock = MagicMock()
        self.geoip_mock.record_by_addr.return_value = SAMPLE_IPINFO

        def _accept_and_handle() -> None:
            client: socket.socket
            client, _ = self.listen_sock.accept()
            with patch("quova.pygeoip.GeoIP", return_value=self.geoip_mock):
                quova.process_connection(client)

        t: threading.Thread = threading.Thread(
            target=_accept_and_handle, daemon=True
        )
        t.start()

    def tearDown(self) -> None:
        self.listen_sock.close()

    def test_full_round_trip(self) -> None:
        with socket.create_connection(
            ("127.0.0.1", self.port), timeout=2
        ) as s:
            packet: bytes = b"\x00" * 10 + bytes([8, 8, 8, 8])
            s.sendall(packet)
            response: bytes = s.recv(4096)
        self.assertIn(b"united states", response)
        self.assertIn(b"kansas", response)

    def test_stats_round_trip(self) -> None:
        with socket.create_connection(
            ("127.0.0.1", self.port), timeout=2
        ) as s:
            s.sendall(b"stats-padded")
            response: bytes = s.recv(64)
        self.assertIn(b"0", response)  # statcounter starts at 0


if __name__ == "__main__":
    unittest.main()
