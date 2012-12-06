#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

from __future__ import print_function

import os.path as path
import tempfile
import contextlib
import shutil
import struct
import atexit
import numpy
import nose.tools as nt
from collections import namedtuple
from cStringIO import StringIO
import bloscpack
from bloscpack import *

def test_hashes():
    nt.assert_equal(len(CHECKSUMS), 9)
    checksums_avail = ['None', 'adler32', 'crc32',
            'md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512']
    nt.assert_equal(CHECKSUMS_AVAIL, checksums_avail)
    # just make sure the hashes do actually compute something.
    csum_targets=[
        '',
        '\x13\x02\xc1\x03',
        '\xbd\xfa.\xaa',
        '\x04\x8fD\xd46\xd5$M\xd7c0\xb1$mUC',
        '\xae\xea\xddm\x86t\x86v\r\x96O\x9fuPh\x1a\x01!#\xe6',
        ' (W\xc8\x1b\x14\x16w\xec\xc4\xd7\x89xU\xc5\x02*\x15\xb4q\xe09\xd0$'+\
            '\xe2+{\x0e',
        's\x83U6N\x81\xa7\xd8\xd3\xce)E/\xa5N\xde\xda\xa6\x1c\x90*\xb0q&m='+\
            '\xea6\xc0\x02\x11-',
        'to\xef\xf2go\x08\xcf#\x9e\x05\x8d~\xa0R\xc1\x93/\xa5\x0b\x8b9'+\
            '\x91E\nKDYW\x1d\xff\x84\xbe\x11\x02X\xd1)"(\x0cO\tJ=\xf5f\x94',
        '\x12w\xc9V/\x84\xe4\x0cd\xf0@\xd2U:Ae\xd9\x9b\xfbm\xe2^*\xdc\x96KG'+\
            '\x06\xa9\xc7\xee\x02\x1d\xac\x08\xf3\x9a*/\x02\x8b\x89\xa0\x0b'+\
            '\xa5=r\xd2\x9b\xf5Z\xf0\xe9z\xb6d\xa7\x00\x12<7\x11\x08e',]
    for i,csum in enumerate(CHECKSUMS):
        nt.assert_equal(csum("\x23\x42\xbe\xef"), csum_targets[i])

def test_print_verbose():
    nt.assert_raises(TypeError, print_verbose, 'message', 'MAXIMUM')
    bloscpack.LEVEL = DEBUG
    # should probably hijack the print statement
    print_verbose('notification')
    bloscpack.LEVEL = NORMAL

def test_error():
    # switch out the exit, to make sure test-suite doesn't fall over
    backup = bloscpack.sys.exit
    bloscpack.sys.exit = lambda x: x
    # should probably hijack the print statement
    error('error')
    bloscpack.sys.exit = backup

def test_pretty_filesieze():

    nt.assert_equal('0B', pretty_size(0))
    nt.assert_equal('9.0T', pretty_size(9898989898879))
    nt.assert_equal('4.78G', pretty_size(5129898234))
    nt.assert_equal('12.3M', pretty_size(12898234))
    nt.assert_equal('966.7K', pretty_size(989898))
    nt.assert_equal('128.0B', pretty_size(128))
    nt.assert_equal(0, reverse_pretty('0B'))
    nt.assert_equal(8, reverse_pretty('8B'))
    nt.assert_equal(8192, reverse_pretty('8K'))
    nt.assert_equal(134217728, reverse_pretty('128M'))
    nt.assert_equal(2147483648, reverse_pretty('2G'))
    nt.assert_equal(2199023255552, reverse_pretty('2T'))
    # can't handle Petabytes, yet
    nt.assert_raises(ValueError, reverse_pretty, '2P')

def test_parser():
    # hmmm I guess we could override the error
    parser = create_parser()

def test_check_files():
    args = namedtuple('Args', 'force')(False)
    # check input_file exists
    nt.assert_raises(FileNotFound, check_files,
            'nosuchfile', 'nosuchfile', args)
    # check that output_file does not exists
    nt.assert_raises(FileNotFound, check_files, 'test_bloscpack.py',
            'test_bloscpack.py', args)
    # check that everything is fine
    args = namedtuple('Args', 'force')(True)
    nt.assert_equal(check_files('test_bloscpack.py',
        'test_bloscpack.py', args), None)

def test_calculate_nchunks():
    # tests for nchunks given
    # odd with no remainder
    nt.assert_equal((3, 3, 3), calculate_nchunks(9, nchunks=3))
    # even with no remaider
    nt.assert_equal((4, 2, 2), calculate_nchunks(8, nchunks=4))
    # odd with nchunks 2
    nt.assert_equal((2, 3, 4), calculate_nchunks(7, nchunks=2))
    # even with nchunks 2
    nt.assert_equal((2, 4, 4), calculate_nchunks(8, nchunks=2))
    # odd with nchunks 1
    nt.assert_equal((1, 0, 9), calculate_nchunks(9, nchunks=1))
    # even with nchunks 1
    nt.assert_equal((1, 0, 8), calculate_nchunks(8, nchunks=1))

    # once, from beginning to end
    nt.assert_equal((1,  0,  23), calculate_nchunks(23, nchunks=1))
    nt.assert_equal((2,  11, 12), calculate_nchunks(23, nchunks=2))
    nt.assert_equal((3,  11, 1),  calculate_nchunks(23, nchunks=3))
    nt.assert_equal((4,  7,  2),  calculate_nchunks(23, nchunks=4))
    nt.assert_equal((5,  5,  3),  calculate_nchunks(23, nchunks=5))
    nt.assert_equal((6,  4,  3),  calculate_nchunks(23, nchunks=6))
    nt.assert_equal((7,  3,  5),  calculate_nchunks(23, nchunks=7))
    nt.assert_equal((8,  3,  2),  calculate_nchunks(23, nchunks=8))
    nt.assert_equal((9,  2,  7),  calculate_nchunks(23, nchunks=9))
    nt.assert_equal((10, 2,  5),  calculate_nchunks(23, nchunks=10))
    nt.assert_equal((11, 2,  3),  calculate_nchunks(23, nchunks=11))
    nt.assert_equal((12, 2,  1),  calculate_nchunks(23, nchunks=12))
    nt.assert_equal((13, 1,  11), calculate_nchunks(23, nchunks=13))
    nt.assert_equal((14, 1,  10), calculate_nchunks(23, nchunks=14))
    nt.assert_equal((15, 1,  9),  calculate_nchunks(23, nchunks=15))
    nt.assert_equal((16, 1,  8),  calculate_nchunks(23, nchunks=16))
    nt.assert_equal((17, 1,  7),  calculate_nchunks(23, nchunks=17))
    nt.assert_equal((18, 1,  6),  calculate_nchunks(23, nchunks=18))
    nt.assert_equal((19, 1,  5),  calculate_nchunks(23, nchunks=19))
    nt.assert_equal((20, 1,  4),  calculate_nchunks(23, nchunks=20))
    nt.assert_equal((21, 1,  3),  calculate_nchunks(23, nchunks=21))
    nt.assert_equal((22, 1,  2),  calculate_nchunks(23, nchunks=22))
    nt.assert_equal((23, 1,  1),  calculate_nchunks(23, nchunks=23))

    # some more random spot tests
    nt.assert_equal((2, 8, 9), calculate_nchunks(17, nchunks=2))
    nt.assert_equal((4, 2, 2), calculate_nchunks(8, nchunks=4))
    nt.assert_equal((7, 3, 2), calculate_nchunks(20, nchunks=7))

    # the special case of no remainder and an empty last chunk
    nt.assert_equal((5, 4, 0), calculate_nchunks(16, nchunks=5))

    # check for nchunks bigger than in_file_size
    nt.assert_raises(ChunkingException, calculate_nchunks,
            23, nchunks=24)
    # check for zero or negative nchunks
    nt.assert_raises(ChunkingException, calculate_nchunks,
            23, nchunks=0)
    nt.assert_raises(ChunkingException, calculate_nchunks,
            23, nchunks=-1)

    # check for chunk_size bigger than in_file_size
    nt.assert_raises(ChunkingException, calculate_nchunks,
            23, chunk_size=24)
    # check for zero or negative chunk_size
    nt.assert_raises(ChunkingException, calculate_nchunks,
            23, chunk_size=0)
    nt.assert_raises(ChunkingException, calculate_nchunks,
            23, chunk_size=-1)

    nt.assert_equal((9, 1, 1), calculate_nchunks(9, chunk_size=1))
    nt.assert_equal((5, 2, 1), calculate_nchunks(9, chunk_size=2))
    nt.assert_equal((3, 3, 3), calculate_nchunks(9, chunk_size=3))
    nt.assert_equal((3, 4, 1), calculate_nchunks(9, chunk_size=4))
    nt.assert_equal((2, 5, 4), calculate_nchunks(9, chunk_size=5))
    nt.assert_equal((2, 6, 3), calculate_nchunks(9, chunk_size=6))
    nt.assert_equal((2, 7, 2), calculate_nchunks(9, chunk_size=7))
    nt.assert_equal((2, 8, 1), calculate_nchunks(9, chunk_size=8))
    nt.assert_equal((1, 0, 9), calculate_nchunks(9, chunk_size=9))

    # single byte file
    nt.assert_equal((1, 0,  1),
            calculate_nchunks(1, nchunks=1))

    # check that giving both arguments raises an error
    nt.assert_raises(ValueError, calculate_nchunks,
            128, nchunks=23, chunk_size=23)

    # check that giving neither argument raises an error
    nt.assert_raises(ValueError, calculate_nchunks, 128)

    # check that a zero length file raises and error
    nt.assert_raises(ValueError, calculate_nchunks, 0)

    # check overflow of nchunks due to chunk_size being too small
    # and thus stuff not fitting into the header
    nt.assert_raises(ChunkingException, calculate_nchunks,
            MAX_CHUNKS+1, chunk_size=1)
    # check overflow of chunk-size due to nchunks being too small
    nt.assert_raises(ChunkingException,
            calculate_nchunks, blosc.BLOSC_MAX_BUFFERSIZE*2+1, nchunks=2)

    # check underflow due to nchunks being too large
    nt.assert_raises(ChunkingException, calculate_nchunks,
            128, nchunks=129)
    # check underflow due to chunk_size being too large
    nt.assert_raises(ChunkingException, calculate_nchunks,
            128, chunk_size=129)

def test_decode_blosc_header():
    array_ = numpy.linspace(0, 100, 2e4).tostring()
    # basic test case
    blosc_args = DEFAULT_BLOSC_ARGS
    compressed = blosc.compress(array_, **blosc_args)
    header = decode_blosc_header(compressed)
    expected = {'versionlz': 1,
                'blocksize': 131072,
                'ctbytes': len(compressed),
                'version': 2,
                'flags': 1,
                'nbytes': len(array_),
                'typesize': blosc_args['typesize']}
    nt.assert_equal(expected, header)
    # deactivate shuffle
    blosc_args['shuffle'] = False
    compressed = blosc.compress(array_, **blosc_args)
    header = decode_blosc_header(compressed)
    expected = {'versionlz': 1,
                'blocksize': 131072,
                'ctbytes': len(compressed),
                'version': 2,
                'flags': 0, # no shuffle flag
                'nbytes': len(array_),
                'typesize': blosc_args['typesize']}
    nt.assert_equal(expected, header)
    # uncompressible data
    array_ = numpy.asarray(numpy.random.randn(23),
            dtype=numpy.float32).tostring()
    blosc_args['shuffle'] = True
    compressed = blosc.compress(array_, **blosc_args)
    header = decode_blosc_header(compressed)
    expected = {'versionlz': 1,
                'blocksize': 88,
                'ctbytes': len(array_) + 16, # original + 16 header bytes
                'version': 2,
                'flags': 3, # 1 for shuffle 2 for non-compressed
                'nbytes': len(array_),
                'typesize': blosc_args['typesize']}
    nt.assert_equal(expected, header)

def test_create_options():
    nt.assert_equal('00000001', create_options())
    nt.assert_equal('00000001', create_options(offsets=True))
    nt.assert_equal('00000000', create_options(offsets=False))

    nt.assert_equal('00000001', create_options(metadata=False))
    nt.assert_equal('00000011', create_options(metadata=True))

    nt.assert_equal('00000000', create_options(offsets=False, metadata=False))
    nt.assert_equal('00000010', create_options(offsets=False, metadata=True))
    nt.assert_equal('00000001', create_options(offsets=True, metadata=False))
    nt.assert_equal('00000011', create_options(offsets=True, metadata=True))


def test_decode_options():
    nt.assert_equal({'offsets': False,
        'metadata': False},
            decode_options('00000000'))
    nt.assert_equal({'offsets': False,
        'metadata': True},
            decode_options('00000010'))
    nt.assert_equal({'offsets': True,
        'metadata': False},
            decode_options('00000001'))
    nt.assert_equal({'offsets': True,
        'metadata': True},
            decode_options('00000011'))

    nt.assert_raises(ValueError, decode_options, '0000000')
    nt.assert_raises(ValueError, decode_options, '000000000')
    nt.assert_raises(ValueError, decode_options, '0000000a')
    nt.assert_raises(ValueError, decode_options, 'abc')

    nt.assert_raises(ValueError, decode_options, '00000100')
    nt.assert_raises(ValueError, decode_options, '00001100')
    nt.assert_raises(ValueError, decode_options, '11111100')

def test_create_metadata_options():
    nt.assert_equal('00000000', create_metadata_options())


def test_decode_metadata_options():
    nt.assert_equal({}, decode_metadata_options('00000000'))
    nt.assert_raises(ValueError, decode_metadata_options, '0000000')
    nt.assert_raises(ValueError, decode_metadata_options, '000000000')
    nt.assert_raises(ValueError, decode_metadata_options, '0000000a')
    nt.assert_raises(ValueError, decode_metadata_options, 'abc')

    nt.assert_raises(ValueError, decode_metadata_options, '00000001')
    nt.assert_raises(ValueError, decode_metadata_options, '00001111')
    nt.assert_raises(ValueError, decode_metadata_options, '11111111')


def test_create_bloscpack_header_arguments():
    # check format_version
    nt.assert_raises(ValueError, create_bloscpack_header, format_version=-1)
    nt.assert_raises(ValueError, create_bloscpack_header,
            format_version=MAX_FORMAT_VERSION+1)
    nt.assert_raises(TypeError, create_bloscpack_header, format_version='foo')
    # check checksum
    nt.assert_raises(ValueError, create_bloscpack_header, checksum=-1)
    nt.assert_raises(ValueError, create_bloscpack_header,
            checksum=len(CHECKSUMS)+1)
    nt.assert_raises(TypeError, create_bloscpack_header, checksum='foo')
    # check options argument
    # check for non-string
    nt.assert_raises(TypeError, create_bloscpack_header, options=0)
    nt.assert_raises(TypeError, create_bloscpack_header, options=1)
    # check for lengths too small and too large
    nt.assert_raises(ValueError, create_bloscpack_header, options='0')
    nt.assert_raises(ValueError, create_bloscpack_header, options='1')
    nt.assert_raises(ValueError, create_bloscpack_header, options='0000000')
    nt.assert_raises(ValueError, create_bloscpack_header, options='000000000')
    nt.assert_raises(ValueError, create_bloscpack_header, options='1111111')
    nt.assert_raises(ValueError, create_bloscpack_header, options='111111111')
    # check for non zeros and ones
    nt.assert_raises(ValueError, create_bloscpack_header, options='0000000a')
    nt.assert_raises(ValueError, create_bloscpack_header, options='aaaaaaaa')
    # check the typesize
    nt.assert_raises(ValueError, create_bloscpack_header, typesize=-1)
    nt.assert_raises(ValueError, create_bloscpack_header,
            typesize=blosc.BLOSC_MAX_TYPESIZE+1)
    # check chunk_size
    nt.assert_raises(ValueError, create_bloscpack_header,
            chunk_size=blosc.BLOSC_MAX_BUFFERSIZE+1)
    nt.assert_raises(ValueError, create_bloscpack_header, chunk_size=-2)
    nt.assert_raises(TypeError, create_bloscpack_header, chunk_size='foo')
    # check last_chunk
    nt.assert_raises(ValueError, create_bloscpack_header,
            last_chunk=blosc.BLOSC_MAX_BUFFERSIZE+1)
    nt.assert_raises(ValueError, create_bloscpack_header, last_chunk=-2)
    nt.assert_raises(TypeError, create_bloscpack_header, last_chunk='foo')
    # check value of nchunks
    nt.assert_raises(ValueError, create_bloscpack_header, nchunks=MAX_CHUNKS+1)
    nt.assert_raises(ValueError, create_bloscpack_header, nchunks=-2)
    nt.assert_raises(TypeError, create_bloscpack_header, nchunks='foo')
    # errors caused by metadata
    nt.assert_raises(ValueError, create_bloscpack_header, meta_size=-1)
    nt.assert_raises(ValueError, create_bloscpack_header, options='0000010',
            meta_size=1)
    nt.assert_raises(ValueError, create_bloscpack_header, options='0000010',
            meta_size=MAX_META_SIZE)

def test_create_bloscpack_header():
    # test with no arguments
    format_version =struct.pack('<B', FORMAT_VERSION)
    expected = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header())
    # test with arbitrary format_version
    expected = MAGIC + struct.pack('<B', 23) + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(format_version=23))
    # test with options
    expected = MAGIC + format_version + \
        '\x01\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(options='00000001'))
    expected = MAGIC + format_version + \
        '\x02\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(options='00000010'))
    expected = MAGIC + format_version + \
        '\xff\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(options='11111111'))
    # test with checksum
    expected = MAGIC + format_version + \
        '\x00\x01\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(checksum=1))
    expected = MAGIC + format_version + \
        '\x00\x08\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(checksum=8))
    # test with typesize
    expected = MAGIC + format_version + \
        '\x00\x00\x01\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(typesize=1))
    expected = MAGIC + format_version + \
        '\x00\x00\x02\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(typesize=2))
    expected = MAGIC + format_version + \
        '\x00\x00\x04\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(typesize=4))
    expected = MAGIC + format_version + \
        '\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(typesize=255))
    # test with chunksize
    expected = MAGIC + format_version + \
        '\x00\x00\x00\x01\x00\x00\x00\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(chunk_size=1))
    expected = MAGIC + format_version + \
        '\x00\x00\x00\x00\x00\x10\x00\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected,
            create_bloscpack_header(chunk_size=reverse_pretty('1M')))
    expected = MAGIC + format_version + \
        '\x00\x00\x00\xef\xff\xff\x7f\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected,
            create_bloscpack_header(chunk_size=blosc.BLOSC_MAX_BUFFERSIZE))
    # test with last_chunk
    expected = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\x01\x00\x00\x00'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(last_chunk=1))
    expected = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\x00\x00\x10\x00'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected,
            create_bloscpack_header(last_chunk=reverse_pretty('1M')))
    expected = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\xef\xff\xff\x7f'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected,
            create_bloscpack_header(last_chunk=blosc.BLOSC_MAX_BUFFERSIZE))
    # test nchunks
    expected = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(nchunks=0))
    expected = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(nchunks=1))
    expected = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\x7f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(nchunks=127))
    expected = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\x7f\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(nchunks=MAX_CHUNKS))
    # test with meta_size
    expected = MAGIC + format_version + \
        '\x02\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(options='00000010',
            meta_size=0))
    expected = MAGIC + format_version + \
        '\x02\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x01\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(options='00000010',
            meta_size=1))
    expected = MAGIC + format_version + \
        '\x02\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x7f\x00\x00\x00\x00'
    nt.assert_equal(expected, create_bloscpack_header(options='00000010',
            meta_size=MAX_META_SIZE))

def test_decode_bloscpack_header():
    no_arg_return  = {
            'format_version': FORMAT_VERSION,
            'options':       '00000000',
            'checksum':      0,
            'typesize':      0,
            'chunk_size':    -1,
            'last_chunk':    -1,
            'nchunks':       -1,
            'meta_size':     0,
            'RESERVED':      0,
            }
    def copy_and_set_return(key, value):
        copy_ = no_arg_return.copy()
        copy_[key] = value
        return copy_

    format_version =struct.pack('<B', FORMAT_VERSION)
    no_arg_input = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'
    def copy_and_set_input(offset, value):
        return no_arg_input[0:offset] + value + \
            no_arg_input[offset+len(value):]
    # check with no args
    nt.assert_equal(no_arg_return, decode_bloscpack_header(no_arg_input))
    # check with format_version
    format_version_set = copy_and_set_input(4, '\x17')
    format_version_set_return = copy_and_set_return('format_version', 23)
    nt.assert_equal(format_version_set_return,
            decode_bloscpack_header(format_version_set))
    # check with options
    nt.assert_equal(copy_and_set_return('options', '00000001'),
            decode_bloscpack_header(copy_and_set_input(5, '\x01')))
    nt.assert_equal(copy_and_set_return('options', '11111111'),
            decode_bloscpack_header(copy_and_set_input(5, '\xff')))
    # check with checksum
    nt.assert_equal(copy_and_set_return('checksum', 1),
            decode_bloscpack_header(copy_and_set_input(6, '\x01')))
    nt.assert_equal(copy_and_set_return('checksum', 7),
            decode_bloscpack_header(copy_and_set_input(6, '\x07')))
    # check with typesize
    nt.assert_equal(copy_and_set_return('typesize', 1),
            decode_bloscpack_header(copy_and_set_input(7, '\x01')))
    nt.assert_equal(copy_and_set_return('typesize', 2),
            decode_bloscpack_header(copy_and_set_input(7, '\x02')))
    nt.assert_equal(copy_and_set_return('typesize', 4),
            decode_bloscpack_header(copy_and_set_input(7, '\x04')))
    nt.assert_equal(copy_and_set_return('typesize', 8),
            decode_bloscpack_header(copy_and_set_input(7, '\x08')))
    nt.assert_equal(copy_and_set_return('typesize', blosc.BLOSC_MAX_TYPESIZE),
            decode_bloscpack_header(copy_and_set_input(7, '\xff')))
    # check with chunk_size
    nt.assert_equal(copy_and_set_return('chunk_size', 1),
            decode_bloscpack_header(copy_and_set_input(8, '\x01\x00\x00\x00')))
    nt.assert_equal(copy_and_set_return('chunk_size', reverse_pretty('1M')),
            decode_bloscpack_header(copy_and_set_input(8, '\x00\x00\x10\x00')))
    nt.assert_equal(
            copy_and_set_return('chunk_size', blosc.BLOSC_MAX_BUFFERSIZE),
            decode_bloscpack_header(copy_and_set_input(8, '\xef\xff\xff\x7f')))
    # check with last_chunk
    nt.assert_equal(copy_and_set_return('last_chunk', 1),
            decode_bloscpack_header(copy_and_set_input(12, '\x01\x00\x00\x00')))
    nt.assert_equal(copy_and_set_return('last_chunk', reverse_pretty('1M')),
            decode_bloscpack_header(copy_and_set_input(12, '\x00\x00\x10\x00')))
    nt.assert_equal(
            copy_and_set_return('last_chunk', blosc.BLOSC_MAX_BUFFERSIZE),
            decode_bloscpack_header(copy_and_set_input(12, '\xef\xff\xff\x7f')))
    # check with nchunks
    nt.assert_equal(copy_and_set_return('nchunks', 1),
            decode_bloscpack_header(copy_and_set_input(16,
                '\x01\x00\x00\x00\x00\x00\x00\x00')))
    nt.assert_equal(copy_and_set_return('nchunks', reverse_pretty('1M')),
            decode_bloscpack_header(copy_and_set_input(16,
                '\x00\x00\x10\x00\x00\x00\x00\x00')))
    nt.assert_equal(
            copy_and_set_return('nchunks', MAX_CHUNKS),
            decode_bloscpack_header(copy_and_set_input(16,
                '\xff\xff\xff\xff\xff\xff\xff\x7f')))

def create_array(repeats, in_file, progress=False):
    with open(in_file, 'w') as in_fp:
        create_array_fp(repeats, in_fp, progress=progress)

def create_array_fp(repeats, in_fp, progress=False):
    if progress:
        def progress(i):
            if i % 10 == 0:
                print('.', end='')
            sys.stdout.flush()
    for i in range(repeats):
        array_ = numpy.linspace(i, i+1, 2e6)
        in_fp.write(array_.tostring())
        if progress:
            progress(i)
    if progress:
        print('done')

def atexit_tmpremover(dirname):
    try:
        shutil.rmtree(dirname)
        print("Removed temporary directory on abort: %s" % dirname)
    except OSError:
        # if the temp dir was removed already, by the context manager
        pass

@contextlib.contextmanager
def create_tmp_files():
    tdir = tempfile.mkdtemp(prefix='blpk')
    in_file = path.join(tdir, 'file')
    out_file = path.join(tdir, 'file.blp')
    dcmp_file = path.join(tdir, 'file.dcmp')
    # register the temp dir remover, safeguard against abort
    atexit.register(atexit_tmpremover, tdir)
    yield tdir, in_file, out_file, dcmp_file
    # context manager remover
    shutil.rmtree(tdir)

def test_offsets():
    blosc_args = DEFAULT_BLOSC_ARGS
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        bloscpack.pack_file(in_file, out_file, blosc_args, nchunks=6)
        with open(out_file, 'r+b') as input_fp:
            bloscpack_header_raw = input_fp.read(BLOSCPACK_HEADER_LENGTH)
            bloscpack_header = decode_bloscpack_header(bloscpack_header_raw)
            nchunks = bloscpack_header['nchunks']
            offsets_raw = input_fp.read(8 * nchunks)
            offsets = [decode_int64(offsets_raw[j - 8: j])
                    for j in xrange(8, nchunks * 8 + 1, 8)]
            # First chunks should start after header and offsets
            first = BLOSCPACK_HEADER_LENGTH + 8 * nchunks
            # We assume that the others are correct
            nt.assert_equal(offsets[0], first)
            nt.assert_equal([80, 585990, 1071780, 1546083, 2003986, 2460350],
                    offsets)
            # try to read the second header
            input_fp.seek(585990, 0)
            blosc_header_raw = input_fp.read(BLOSC_HEADER_LENGTH)
            expected = {'versionlz': 1,
                        'blocksize': 131072,
                        'ctbytes':   485786,
                        'version':   2,
                        'flags':     1,
                        'nbytes':    3200000,
                        'typesize':  8}
            blosc_header = decode_blosc_header(blosc_header_raw)
            nt.assert_equal(expected, blosc_header)

def test_metadata():
    test_metadata = "{'dtype': 'float64', 'shape': [1024], 'others': []}"
    received_metadata = pack_unpack_fp(1, nchunks=20, metadata=test_metadata)
    nt.assert_equal(test_metadata, received_metadata)

def test_metadata_mismatch():
    test_metadata = "{'dtype': 'float64', 'shape': [1024], 'others': []}"
    in_fp, out_fp, dcmp_fp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, in_fp)
    in_fp_size = in_fp.tell()
    in_fp.seek(0)
    bloscpack._pack_fp(in_fp, out_fp, in_fp_size,
            DEFAULT_BLOSC_ARGS,
            test_metadata,
            1,
            None,
            DEFAULT_OFFSETS,
            DEFAULT_CHECKSUM)
    # remove the metadata bit
    options = create_options(metadata=False)
    options_binary = encode_uint8(int(options, 2))
    out_fp.seek(5)
    out_fp.write(options_binary)
    out_fp.seek(0)
    nt.assert_raises(MetaDataMismatch, bloscpack._unpack_fp, out_fp, dcmp_fp)

def test_metadata_opportunisitic_compression():
    # make up some metadata that can be compressed with benefit
    test_metadata = ("{'dtype': 'float64', 'shape': [1024], 'others': [],"
            "'original_container': 'carray'}")
    in_fp, out_fp, dcmp_fp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, in_fp)
    in_fp_size = in_fp.tell()
    in_fp.seek(0)
    bloscpack._pack_fp(in_fp, out_fp, in_fp_size,
            DEFAULT_BLOSC_ARGS,
            test_metadata,
            1,
            None,
            DEFAULT_OFFSETS,
            DEFAULT_CHECKSUM)
    out_fp.seek(0)
    raw_header = out_fp.read(32)
    header = decode_bloscpack_header(raw_header)
    raw_options = header['options']
    options = decode_options(raw_options)
    #nt.assert_true(options['compress_meta'])

    # now do the same thing, but use badly compressible metadata
    test_metadata = "abc"
    in_fp, out_fp, dcmp_fp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, in_fp)
    in_fp_size = in_fp.tell()
    in_fp.seek(0)
    bloscpack._pack_fp(in_fp, out_fp, in_fp_size,
            DEFAULT_BLOSC_ARGS,
            test_metadata,
            1,
            None,
            DEFAULT_OFFSETS,
            DEFAULT_CHECKSUM)
    out_fp.seek(0)
    raw_header = out_fp.read(32)
    header = decode_bloscpack_header(raw_header)
    raw_options = header['options']
    options = decode_options(raw_options)
    # bloscpack should have decided that there is no benefit to compressing the
    # metadata and thus deactivated it
    #nt.assert_false(options['compress_meta'])

def test_invalid_format():
    # this will cause a bug if we ever reach 255 format versions
    bloscpack.FORMAT_VERSION = MAX_FORMAT_VERSION
    blosc_args = DEFAULT_BLOSC_ARGS
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        bloscpack.pack_file(in_file, out_file, blosc_args, nchunks=1)
        nt.assert_raises(FormatVersionMismatch, unpack_file, out_file, dcmp_file)
    bloscpack.FORMAT_VERSION = FORMAT_VERSION

def test_file_corruption():
    blosc_args = DEFAULT_BLOSC_ARGS
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        pack_file(in_file, out_file, blosc_args,
                nchunks=1)
        # now go in and modify a byte in the file
        with open(out_file, 'r+b') as input_fp:
            # read the header
            bloscpack_header_raw = input_fp.read(BLOSCPACK_HEADER_LENGTH)
            bloscpack_header = decode_bloscpack_header(bloscpack_header_raw)
            # read the offsets
            input_fp.read(8 * bloscpack_header['nchunks'])
            # read the blosc header of the first chunk
            input_fp.read(BLOSC_HEADER_LENGTH)
            # read four bytes
            input_fp.read(4)
            # read the fifth byte
            fifth = input_fp.read(1)
            # figure out what to replcae it by
            replace = '\x00' if fifth == '\xff' else '\xff'
            # seek one byte back relative to current position
            input_fp.seek(-1, 1)
            # write the flipped byte
            input_fp.write(replace)
        # now attempt to unpack it
        nt.assert_raises(ChecksumMismatch, unpack_file, out_file, dcmp_file)

def pack_unpack(repeats, nchunks=None, chunk_size=None, progress=False):
    blosc_args = DEFAULT_BLOSC_ARGS
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        if progress:
            print("Creating test array")
        create_array(repeats, in_file, progress=progress)
        if progress:
            print("Compressing")
        pack_file(in_file, out_file, blosc_args,
                nchunks=nchunks, chunk_size=chunk_size)
        if progress:
            print("Decompressing")
        unpack_file(out_file, dcmp_file)
        if progress:
            print("Verifying")
        cmp(in_file, dcmp_file)

def pack_unpack_fp(repeats, nchunks=None, chunk_size=None,
        progress=False, metadata=None):
    blosc_args = DEFAULT_BLOSC_ARGS
    offsets = DEFAULT_OFFSETS
    checksum = DEFAULT_CHECKSUM
    in_fp, out_fp, dcmp_fp = StringIO(), StringIO(), StringIO()
    if progress:
        print("Creating test array")
    create_array_fp(repeats, in_fp, progress=progress)
    in_fp_size = in_fp.tell()
    if progress:
        print("Compressing")
    in_fp.seek(0)
    bloscpack._pack_fp(in_fp, out_fp, in_fp_size,
            blosc_args, metadata,
            nchunks, chunk_size, offsets, checksum)
    out_fp.seek(0)
    if progress:
        print("Decompressing")
    metadata = bloscpack._unpack_fp(out_fp, dcmp_fp)
    if progress:
        print("Verifying")
    cmp_fp(in_fp, dcmp_fp)
    if metadata:
        return metadata

def test_pack_unpack():
    pack_unpack(1, nchunks=20)
    pack_unpack(1, nchunks=1)
    pack_unpack(1, nchunks=100)
    pack_unpack(1, chunk_size=reverse_pretty('1M'))
    pack_unpack(1, chunk_size=reverse_pretty('2M'))
    pack_unpack(1, chunk_size=reverse_pretty('4M'))
    pack_unpack(1, chunk_size=reverse_pretty('8M'))

def test_pack_unpack_fp():
    pack_unpack_fp(1, nchunks=20)
    pack_unpack_fp(1, nchunks=1)
    pack_unpack_fp(1, nchunks=100)
    pack_unpack_fp(1, chunk_size=reverse_pretty('1M'))
    pack_unpack_fp(1, chunk_size=reverse_pretty('2M'))
    pack_unpack_fp(1, chunk_size=reverse_pretty('4M'))
    pack_unpack_fp(1, chunk_size=reverse_pretty('8M'))

def pack_unpack_hard():
    """ Test on somewhat larger arrays, but be nice to memory. """
    # Array is apprx. 1.5 GB large
    # should make chunk-size of apprx. 1MB
    pack_unpack(100, nchunks=1536, progress=True)
    # should make apprx 1536 chunks
    pack_unpack(100, chunk_size=reverse_pretty('1M'), progress=True)

def pack_unpack_extreme():
    """ Test on somewhat larer arrays, uses loads of memory. """
    # this will create a huge array, and then use the
    # blosc.BLOSC_MAX_BUFFERSIZE as chunk-szie
    pack_unpack(300, chunk_size=blosc.BLOSC_MAX_BUFFERSIZE, progress=True)

def cmp(file1, file2):
    """ File comparison utility with a small chunksize """
    with open_two_file(open(file1, 'rb'), open(file2, 'rb')) as \
            (fp1, fp2):
        cmp_fp(fp1, fp2)

def cmp_fp(fp1, fp2):
    chunk_size = reverse_pretty(DEFAULT_CHUNK_SIZE)
    while True:
        a = fp1.read(chunk_size)
        b = fp2.read(chunk_size)
        if a == '' and b == '':
            return True
        else:
            nt.assert_equal(a, b)
