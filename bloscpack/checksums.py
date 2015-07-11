#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import hashlib
import struct
import zlib


from .exceptions import (NoSuchChecksum,
                         )


class Hash(object):
    """ Uniform hash object.

    Parameters
    ----------
    name : str
        the name of the hash
    size : int
        the length of the digest in bytes
    function : callable
        the hash function implementation

    Notes
    -----
    The 'function' argument should return the raw bytes as string.

    """

    def __init__(self, name, size, function):
        self.name, self.size, self._function = name, size, function

    def __call__(self, data):
        return self._function(data)


def zlib_hash(func):
    """ Wrapper for zlib hashes. """
    def hash_(data):
        # The binary OR is recommended to obtain uniform hashes on all python
        # versions and platforms. The result of the checksum is a 'uint32'
        # https://docs.python.org/3.4/library/zlib.html
        return struct.pack('<I', func(data) & 0xffffffff)
    return 4, hash_


def hashlib_hash(func):
    """ Wrapper for hashlib hashes. """
    def hash_(data):
        return func(data).digest()
    return func().digest_size, hash_


CHECKSUMS = [Hash('None', 0, lambda data: b''),
             Hash('adler32', *zlib_hash(zlib.adler32)),
             Hash('crc32', *zlib_hash(zlib.crc32)),
             Hash('md5', *hashlib_hash(hashlib.md5)),
             Hash('sha1', *hashlib_hash(hashlib.sha1)),
             Hash('sha224', *hashlib_hash(hashlib.sha224)),
             Hash('sha256', *hashlib_hash(hashlib.sha256)),
             Hash('sha384', *hashlib_hash(hashlib.sha384)),
             Hash('sha512', *hashlib_hash(hashlib.sha512)),
             ]
CHECKSUMS_AVAIL = [c.name for c in CHECKSUMS]
CHECKSUMS_LOOKUP = dict(((c.name, c) for c in CHECKSUMS))


def check_valid_checksum(checksum):
    """ Check the validity of a checksum.

    Parameters
    ----------
    checksum : str
        the string descriptor of the checksum

    Raises
    ------
    NoSuchChecksum
        if no such checksum exists.
    """
    if checksum not in CHECKSUMS_AVAIL:
        raise NoSuchChecksum("checksum '%s' does not exist" % checksum)
