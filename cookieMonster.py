from base64 import b64decode, b64encode
from cryptography.fernet import Fernet
from hashlib import sha512
from logging import error, warn
from pickle import dumps, loads
from typing import Any, Union
import unittest


class TestCookies(unittest.TestCase):
    def test_cookies(self):
        data = "This is a test"
        cookie = makeCookie(data)
        self.assertNotEqual(cookie, "")
        self.assertEqual(eatCookie(cookie), data)


def makeCookie(data: Any, key: bytes = b"") -> str:
    """If you pass in a key it must be 32 url-safe bytes encoded
    with base64, totaling 44 characters of base64. e.g.
    k = b'12345678901234567890123456789012'
    len(k)
    32
    key = b64encode(k)
    len(key)
    44"""
    if not key:
        key = Fernet.generate_key()
    try:
        f = Fernet(key)
        pdata = dumps(data)
        cdata = b64encode(f.encrypt(pdata))
        sha = sha512(cdata).hexdigest().upper()
    except Exception as ex:
        error(f"Error encoding the cookie: {ex}")
        return ""
    return f"{key.decode('utf-8')}{cdata.decode('utf-8')}{sha}"


def eatCookie(cookie: str) -> Union[Any, None]:
    """Pass in a string generated from makeCookie to get the
    original data, with full error checking to ensure that the
    data wasn't tampered with in transit."""
    key = cookie[:44].encode("utf-8")
    cdata = cookie[44:-128].encode("utf-8")
    sha = sha512(cdata).hexdigest().upper()
    digest = cookie[-128:]
    if sha != digest:
        warn("The data has been tampered with.")
        return None
    try:
        f = Fernet(key)
        pdata = f.decrypt(b64decode(cdata))
        data = loads(pdata)
        return data
    except Exception as ex:
        error(f"Error decoding the cookie: {ex}")
        return None


if __name__ == "__main__":
    unittest.main()
