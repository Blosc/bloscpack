#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


import nose.tools as nt


from bloscpack import checksums


def test_checksusm_exist():
    nt.assert_equal(len(checksums.CHECKSUMS), 9)
    checksums_avail = ['None',
                       'adler32',
                       'crc32',
                       'md5',
                       'sha1',
                       'sha224',
                       'sha256',
                       'sha384',
                       'sha512']
    nt.assert_equal(checksums.CHECKSUMS_AVAIL, checksums_avail)


def test_checksusm_are_sane():
    # just make sure the hashes do actually compute something.
    csum_targets = [
        b'',
        b'\x13\x02\xc1\x03',
        b'\xbd\xfa.\xaa',
        b'\x04\x8fD\xd46\xd5$M\xd7c0\xb1$mUC',
        b'\xae\xea\xddm\x86t\x86v\r\x96O\x9fuPh\x1a\x01!#\xe6',
        b' (W\xc8\x1b\x14\x16w\xec\xc4\xd7\x89xU\xc5\x02*\x15\xb4q' +
        b'\xe09\xd0$\xe2+{\x0e',
        b's\x83U6N\x81\xa7\xd8\xd3\xce)E/\xa5N\xde\xda\xa6\x1c\x90*\xb0q&m=' +
        b'\xea6\xc0\x02\x11-',
        b'to\xef\xf2go\x08\xcf#\x9e\x05\x8d~\xa0R\xc1\x93/\xa5\x0b\x8b9' +
        b'\x91E\nKDYW\x1d\xff\x84\xbe\x11\x02X\xd1)"(\x0cO\tJ=\xf5f\x94',
        b'\x12w\xc9V/\x84\xe4\x0cd\xf0@\xd2U:Ae\xd9\x9b\xfbm\xe2^*\xdc\x96KG' +
        b'\x06\xa9\xc7\xee\x02\x1d\xac\x08\xf3\x9a*/\x02\x8b\x89\xa0\x0b' +
        b'\xa5=r\xd2\x9b\xf5Z\xf0\xe9z\xb6d\xa7\x00\x12<7\x11\x08e',
        ]
    for i, csum in enumerate(checksums.CHECKSUMS):
        digest = csum(b"\x23\x42\xbe\xef")
        yield nt.assert_equal, len(digest), csum.size
        yield nt.assert_equal, digest, csum_targets[i]
