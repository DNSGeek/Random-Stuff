#!/usr/bin/env python3 -O

import logging
import random
import unittest
from base64 import b85decode, b85encode
from hashlib import sha256
from logging import error, warning
from pickle import dumps, loads
from typing import Any, Optional

from cryptography.fernet import Fernet, InvalidToken

# Number of segments the key is split into before interleaving with data.
# Changing this constant is a breaking change — existing cookies will be unreadable.
_KEY_SEGMENTS: int = 5

# Separator used to delimit segments within the cookie string.
# Must be a character that never appears in base85 output.
# base85 uses: 0-9, A-Z, a-z, and: ! # $ % & ( ) * + - ; < = > ? @ ^ _ ` { | } ~
# So a plain '.' is safe.
_SEP: str = "."


def _split_bytes(data: bytes, n: int) -> list[bytes]:
    """Split bytes into n roughly equal chunks."""
    size, remainder = divmod(len(data), n)
    chunks: list[bytes] = []
    offset: int = 0
    for i in range(n):
        chunk_size: int = size + (1 if i < remainder else 0)
        chunks.append(data[offset : offset + chunk_size])
        offset += chunk_size
    return chunks


def _interleave(
    key_segments: list[str], data_segments: list[str], seed: int
) -> str:
    """Randomly interleave key_segments into data_segments using a seeded RNG
    so the positions are deterministic but non-obvious.

    Layout: we have (n_data + n_key) total slots. We choose n_key positions
    for the key segments; the rest are data. A '.' separator and a single-char
    type prefix ('k' or 'd') are prepended to each segment so parsing is
    unambiguous and self-validating.
    """
    total: int = len(key_segments) + len(data_segments)
    rng: random.Random = random.Random(seed)
    key_positions: set[int] = set(rng.sample(range(total), len(key_segments)))

    result: list[str] = []
    ki: int = 0
    di: int = 0
    for i in range(total):
        if i in key_positions:
            result.append(f"k{key_segments[ki]}")
            ki += 1
        else:
            result.append(f"d{data_segments[di]}")
            di += 1
    return _SEP.join(result)


def _deinterleave(cookie: str, seed: int) -> tuple[bytes, bytes]:
    """Reconstruct key and data bytes from an interleaved cookie string.
    Raises ValueError if the cookie is malformed."""
    segments: list[str] = cookie.split(_SEP)
    total: int = len(segments)
    n_key: int = _KEY_SEGMENTS
    n_data: int = total - n_key

    if n_data < 1:
        raise ValueError(
            f"Cookie has too few segments: expected at least {n_key + 1}, got {total}"
        )

    rng: random.Random = random.Random(seed)
    key_positions: set[int] = set(rng.sample(range(total), n_key))

    key_parts: list[bytes] = []
    data_parts: list[bytes] = []
    for i, seg in enumerate(segments):
        if not seg or seg[0] not in ("k", "d"):
            raise ValueError(
                f"Segment {i} has invalid type prefix: {seg[:5]!r}"
            )
        payload: bytes = seg[1:].encode("utf-8")
        if i in key_positions:
            if seg[0] != "k":
                raise ValueError(
                    f"Segment {i} expected key prefix 'k', got {seg[0]!r}"
                )
            key_parts.append(b85decode(payload))
        else:
            if seg[0] != "d":
                raise ValueError(
                    f"Segment {i} expected data prefix 'd', got {seg[0]!r}"
                )
            data_parts.append(b85decode(payload))

    return b"".join(key_parts), b"".join(data_parts)


def makeCookie(data: Any, key: bytes = b"") -> str:
    """Encrypt and serialise arbitrary data into a cookie string.

    If no key is provided, a fresh Fernet key is generated. If you supply
    your own key it must be 32 url-safe bytes encoded with base64 (44 chars).

    The key is split into _KEY_SEGMENTS pieces and interleaved with the
    encrypted data at positions determined by a seeded PRNG. The seed is
    derived from a hash of the encrypted payload, making it deterministic
    without storing the positions explicitly.

    Note: embedding the key in the cookie provides tamper-evidence but NOT
    confidentiality against someone who also has this source code.
    """
    if not key:
        key = Fernet.generate_key()
    try:
        f = Fernet(key)
        pdata: bytes = dumps(data)
        cdata: bytes = f.encrypt(pdata)

        # Derive a deterministic seed from the encrypted payload.
        seed: int = int(sha256(cdata).hexdigest(), 16)

        # Split raw bytes first, then b85-encode each chunk individually.
        # This avoids chunk boundaries falling mid-character in the encoded string.
        key_segments: list[str] = [
            b85encode(chunk).decode("utf-8")
            for chunk in _split_bytes(key, _KEY_SEGMENTS)
        ]
        # Split data into the same number of chunks as key segments.
        data_segments: list[str] = [
            b85encode(chunk).decode("utf-8")
            for chunk in _split_bytes(cdata, _KEY_SEGMENTS)
        ]

        return _interleave(key_segments, data_segments, seed)

    except Exception as ex:
        error(f"Error encoding the cookie: {ex}")
        return ""


def eatCookie(cookie: str) -> Optional[Any]:
    """Decrypt and deserialise a cookie string produced by makeCookie.

    Returns the original data on success.
    Returns None if the cookie is malformed, tampered with, or decryption fails.
    """
    if not cookie:
        warning("Empty cookie received.")
        return None

    # We need the seed to deinterleave, but the seed was derived from cdata —
    # which we don't have yet. So we do a two-pass approach:
    #   Pass 1: extract all segments, separate key vs data by trying each possible
    #           seed implied by the structure. Since the seed is derived from cdata
    #           and cdata is what we're trying to extract, we instead recompute:
    #           reassemble cdata from data segments, hash it, and verify consistency.
    #
    # Practically: we can't know the seed until we have cdata, and we can't get
    # cdata without the seed. We break this chicken-and-egg by trying the only
    # seed that's consistent with the data — we extract segments assuming the
    # positions encoded in the cookie match a seed derived from the reassembled data,
    # then verify that the seed is self-consistent after decryption.
    #
    # Simpler approach that works because we store type prefixes ('k'/'d') on each
    # segment: just read the prefixes directly. The seed is only needed to PLACE
    # segments during encoding; during decoding the prefixes tell us which is which.

    try:
        segments: list[str] = cookie.split(_SEP)
        key_parts: list[bytes] = []
        data_parts: list[bytes] = []
        for i, seg in enumerate(segments):
            if not seg or seg[0] not in ("k", "d"):
                raise ValueError(
                    f"Segment {i} has invalid type prefix: {seg[:5]!r}"
                )
            payload: bytes = seg[1:].encode("utf-8")
            if seg[0] == "k":
                key_parts.append(b85decode(payload))
            else:
                data_parts.append(b85decode(payload))

        if len(key_parts) != _KEY_SEGMENTS:
            raise ValueError(
                f"Expected {_KEY_SEGMENTS} key segments, found {len(key_parts)}"
            )
        if not data_parts:
            raise ValueError("No data segments found in cookie")

        key: bytes = b"".join(key_parts)
        cdata: bytes = b"".join(data_parts)

        # Verify the seed is self-consistent: recompute from cdata and confirm
        # that the key positions in the cookie match what the seed would produce.
        seed: int = int(sha256(cdata).hexdigest(), 16)
        total: int = len(segments)
        rng: random.Random = random.Random(seed)
        expected_key_positions: set[int] = set(
            rng.sample(range(total), _KEY_SEGMENTS)
        )
        actual_key_positions: set[int] = {
            i for i, seg in enumerate(segments) if seg[0] == "k"
        }
        if expected_key_positions != actual_key_positions:
            warning(
                "Cookie segment positions are inconsistent — possible tampering."
            )
            return None

        f = Fernet(key)
        pdata: bytes = f.decrypt(cdata)
        return loads(pdata)

    except InvalidToken:
        warning(
            "Cookie failed Fernet authentication — data may have been tampered with."
        )
        return None
    except ValueError as ex:
        warning(f"Malformed cookie: {ex}")
        return None
    except Exception as ex:
        error(f"Error decoding the cookie: {ex}")
        return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCookies(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # The error-path tests intentionally trigger warnings and errors.
        # Suppress them so the test output isn't polluted with expected noise.
        logging.disable(logging.CRITICAL)

    @classmethod
    def tearDownClass(cls) -> None:
        logging.disable(logging.NOTSET)

    # --- makeCookie ---

    def test_roundtrip_string(self) -> None:
        data: str = "This is a test"
        self.assertEqual(eatCookie(makeCookie(data)), data)

    def test_roundtrip_int(self) -> None:
        self.assertEqual(eatCookie(makeCookie(42)), 42)

    def test_roundtrip_list(self) -> None:
        data: list[Any] = [1, "two", 3.0, None]
        self.assertEqual(eatCookie(makeCookie(data)), data)

    def test_roundtrip_dict(self) -> None:
        data: dict[str, Any] = {"key": "value", "number": 99}
        self.assertEqual(eatCookie(makeCookie(data)), data)

    def test_roundtrip_none(self) -> None:
        self.assertIsNone(eatCookie(makeCookie(None)))

    def test_roundtrip_empty_string(self) -> None:
        self.assertEqual(eatCookie(makeCookie("")), "")

    def test_roundtrip_bytes(self) -> None:
        data: bytes = b"\x00\x01\x02\xff"
        self.assertEqual(eatCookie(makeCookie(data)), data)

    def test_cookie_is_string(self) -> None:
        self.assertIsInstance(makeCookie("hello"), str)

    def test_cookie_is_not_empty(self) -> None:
        self.assertNotEqual(makeCookie("hello"), "")

    def test_two_cookies_differ(self) -> None:
        """Each call generates a fresh key, so cookies should not be identical."""
        self.assertNotEqual(makeCookie("same data"), makeCookie("same data"))

    def test_provided_key_roundtrip(self) -> None:
        key: bytes = Fernet.generate_key()
        data: str = "keyed cookie"
        self.assertEqual(eatCookie(makeCookie(data, key=key)), data)

    # --- eatCookie: malformed input ---

    def test_empty_cookie_returns_none(self) -> None:
        self.assertIsNone(eatCookie(""))

    def test_garbage_cookie_returns_none(self) -> None:
        self.assertIsNone(eatCookie("notacookie"))

    def test_truncated_cookie_returns_none(self) -> None:
        cookie: str = makeCookie("hello")
        self.assertIsNone(eatCookie(cookie[:20]))

    def test_wrong_number_of_segments_returns_none(self) -> None:
        # Strip the last segment to produce the wrong segment count.
        cookie: str = makeCookie("hello")
        truncated: str = _SEP.join(cookie.split(_SEP)[:-1])
        self.assertIsNone(eatCookie(truncated))

    # --- eatCookie: tamper detection ---

    def test_flipped_bit_returns_none(self) -> None:
        cookie: str = makeCookie("tamper me")
        # Flip a character in the middle of the cookie.
        mid: int = len(cookie) // 2
        tampered: str = (
            cookie[:mid]
            + ("X" if cookie[mid] != "X" else "Y")
            + cookie[mid + 1 :]
        )
        self.assertIsNone(eatCookie(tampered))

    def test_swapped_segments_returns_none(self) -> None:
        """Swapping two segments changes their type-prefix positions,
        which should fail the seed consistency check."""
        cookie: str = makeCookie("swap me")
        parts: list[str] = cookie.split(_SEP)
        if len(parts) >= 2:
            parts[0], parts[1] = parts[1], parts[0]
            self.assertIsNone(eatCookie(_SEP.join(parts)))

    def test_extra_segment_returns_none(self) -> None:
        cookie: str = makeCookie("extra")
        self.assertIsNone(eatCookie(cookie + _SEP + "dXXXX"))

    def test_wrong_type_prefix_returns_none(self) -> None:
        """Replace a 'k' prefix with 'd' to misclassify a key segment."""
        cookie: str = makeCookie("prefix")
        parts: list[str] = cookie.split(_SEP)
        for i, seg in enumerate(parts):
            if seg.startswith("k"):
                parts[i] = "d" + seg[1:]
                break
        self.assertIsNone(eatCookie(_SEP.join(parts)))


if __name__ == "__main__":
    unittest.main()
