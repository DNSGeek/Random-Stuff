#!/usr/bin/python3 -uO
lllllllllllllll, llllllllllllllI, lllllllllllllIl = Exception, str, bytes

import unittest
from base64 import b85decode as IlIIllllIIlllI
from base64 import b85encode as llllIlIlllllII
from hashlib import sha512 as IllIIllIlIIIIl
from logging import error as IIIllllIlIIlII
from logging import warn as IIIlllIIlIllIl
from pickle import dumps as IlllIlIlIIlIlI
from pickle import loads as lIIIlIlIlIIllI
from typing import Any as llIIlIIllIIIlI
from typing import Union as IIIIlllllIlllI

from cryptography.fernet import Fernet as lllllllIlIIIII


def IlIlIllIlllIlIllll(
    IIlllIllllllIIllll: llIIlIIllIIIlI,
    lIIlIllIllIlIllIIl: lllllllllllllIl = b"",
) -> llllllllllllllI:
    """If you pass in a key it must be 32 url-safe bytes encoded
    with base64, totaling 44 characters of base64. e.g.
    k = b'12345678901234567890123456789012'
    len(k)
    32
    key = b64encode(k)
    len(key)
    44"""
    if not lIIlIllIllIlIllIIl:
        lIIlIllIllIlIllIIl = lllllllIlIIIII.generate_key()
    try:
        IIIIIIlIIlIIllIIII = lllllllIlIIIII(lIIlIllIllIlIllIIl)
        IIllIIIIllIIIIlIIl = llllIlIlllllII(lIIlIllIllIlIllIIl)
        IllllllIIIlIllIlll = IlllIlIlIIlIlI(IIlllIllllllIIllll)
        IllIIlIIIIlIIllIll = llllIlIlllllII(
            IIIIIIlIIlIIllIIII.encrypt(IllllllIIIlIllIlll)
        )
        llIIIlIIlIIIIIlllI = llllIlIlllllII(
            IllIIllIlIIIIl(IllIIlIIIIlIIllIll).hexdigest().encode("utf-8")
        ).decode("utf-8")
    except lllllllllllllll as IIllIIIlllIIIllIlI:
        IIIllllIlIIlII(f"Error encoding the cookie: {IIllIIIlllIIIllIlI}")
        return ""
    return f"{IIllIIIIllIIIIlIIl.decode('utf-8')}{IllIIlIIIIlIIllIll.decode('utf-8')}{llIIIlIIlIIIIIlllI}"


def llIlIlllllIlIlIIlI(
    IlllIIllllIIIIIlll: llllllllllllllI,
) -> IIIIlllllIlllI[llIIlIIllIIIlI, None]:
    """Pass in a string generated from IlIlIllIlllIlIllll to get the
    original data, with full error checking to ensure that the
    data wasn't tampered with in transit."""
    IIllIIIIllIIIIlIIl = IlllIIllllIIIIIlll[:55].encode("utf-8")
    lIIlIllIllIlIllIIl = IlIIllllIIlllI(IIllIIIIllIIIIlIIl)
    IllIIlIIIIlIIllIll = IlllIIllllIIIIIlll[55:-160].encode("utf-8")
    llIIIlIIlIIIIIlllI = llllIlIlllllII(
        IllIIllIlIIIIl(IllIIlIIIIlIIllIll).hexdigest().encode("utf-8")
    ).decode("utf-8")
    lllIIIIlIllllIIIll = IlllIIllllIIIIIlll[-160:]
    if llIIIlIIlIIIIIlllI != lllIIIIlIllllIIIll:
        IIIlllIIlIllIl("The data has been tampered with.")
        return None
    try:
        IIIIIIlIIlIIllIIII = lllllllIlIIIII(lIIlIllIllIlIllIIl)
        IllllllIIIlIllIlll = IIIIIIlIIlIIllIIII.decrypt(
            IlIIllllIIlllI(IllIIlIIIIlIIllIll)
        )
        IIlllIllllllIIllll = lIIIlIlIlIIllI(IllllllIIIlIllIlll)
        return IIlllIllllllIIllll
    except lllllllllllllll as IIllIIIlllIIIllIlI:
        IIIllllIlIIlII(f"Error decoding the cookie: {IIllIIIlllIIIllIlI}")
        return None


class TestData(unittest.TestCase):
    def test_IlIlIllIlllIlIllll(self):
        data = "This is a test"
        IIlIlllIIIlllllIlII = IlIlIllIlllIlIllll(data)
        self.assertNotEqual(IIlIlllIIIlllllIlII, "")
        self.assertEqual(llIlIlllllIlIlIIlI(IIlIlllIIIlllllIlII), data)


if __name__ == "__main__":
    unittest.main()
