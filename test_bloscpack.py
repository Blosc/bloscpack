#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

import os.path as path
import tempfile
import numpy
import nose
import nose.tools as nt
import bloscpack
from bloscpack import *

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

    nt.assert_equal('9.0T', pretty_size(9898989898879))
    nt.assert_equal('4.78G', pretty_size(5129898234))
    nt.assert_equal('12.3M', pretty_size(12898234))
    nt.assert_equal('966.7K', pretty_size(989898))
    nt.assert_equal('128.0B', pretty_size(128))
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

def test_nchunks():
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

    # perhaps zero length files should not be supported
    nt.assert_equal((0,0,  0),
            calculate_nchunks(0))
    # single byte file
    nt.assert_equal((1,0,  1),
            calculate_nchunks(1))

    # just less than max
    nt.assert_equal((1,0,  blosc.BLOSC_MAX_BUFFERSIZE-1),
            calculate_nchunks(blosc.BLOSC_MAX_BUFFERSIZE-1))
    # exactly max
    nt.assert_equal((1,0,  blosc.BLOSC_MAX_BUFFERSIZE),
            calculate_nchunks(blosc.BLOSC_MAX_BUFFERSIZE))
    # just more than max
    nt.assert_equal((2, blosc.BLOSC_MAX_BUFFERSIZE, 1),
            calculate_nchunks(blosc.BLOSC_MAX_BUFFERSIZE+1))
    # max plus half max
    nt.assert_equal(
            (2, blosc.BLOSC_MAX_BUFFERSIZE, blosc.BLOSC_MAX_BUFFERSIZE/2),
            calculate_nchunks(blosc.BLOSC_MAX_BUFFERSIZE+
                blosc.BLOSC_MAX_BUFFERSIZE/2))
    # 4 * max +1
    nt.assert_equal((5, blosc.BLOSC_MAX_BUFFERSIZE, 1),
            calculate_nchunks(4*blosc.BLOSC_MAX_BUFFERSIZE+1))

    # check that giving both arguments raises an error
    nt.assert_raises(ValueError, calculate_nchunks,
            128, nchunks=23, chunk_size=23)
    # check overflow of nchunks due to chunk_size being too small
    nt.assert_raises(ChunkingException, calculate_nchunks,
            blosc.BLOSC_MAX_BUFFERSIZE*23, chunk_size=1)
    # check overflow of BLOSC_MAX_BUFFERSIZE due to nchunks being too small
    nt.assert_raises(ChunkingException,
            calculate_nchunks, blosc.BLOSC_MAX_BUFFERSIZE*2+1, nchunks=2)

def test_decode_blosc_header():
    array_ = numpy.linspace(0, 100, 2e4).tostring()
    # basic test case
    blosc_args = {'typesize': 4,
                  'clevel' : 7,
                  'shuffle' : True}
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
    array_ = numpy.random.randn(2e4).tostring()
    blosc_args['shuffle'] = True
    compressed = blosc.compress(array_, **blosc_args)
    header = decode_blosc_header(compressed)
    expected = {'versionlz': 1,
                'blocksize': 131072,
                'ctbytes': len(array_) + 16, # original + 16 header bytes
                'version': 2,
                'flags': 3, # 1 for shuffle 2 for non-compressed
                'nbytes': len(array_),
                'typesize': blosc_args['typesize']}
    nt.assert_equal(expected, header)

def test_create_bloscpack_header():
    nt.assert_equal('%s\x00\x00\x00\x00' % MAGIC, create_bloscpack_header(0))
    nt.assert_equal('%s\x01\x00\x00\x00' % MAGIC, create_bloscpack_header(1))
    nt.assert_equal('%s\xff\xff\xff\xff' % MAGIC,
            create_bloscpack_header(4294967295))
    nt.assert_raises(Exception, create_bloscpack_header, 4294967296)

def test_decode_bloscpack_header():
    nt.assert_equal(0, decode_bloscpack_header('%s\x00\x00\x00\x00' % MAGIC))
    nt.assert_equal(1, decode_bloscpack_header('%s\x01\x00\x00\x00' % MAGIC))
    nt.assert_equal(4294967295,
            decode_bloscpack_header('%s\xff\xff\xff\xff' % MAGIC))
    nt.assert_raises(ValueError, decode_bloscpack_header, 'blpk')
    nt.assert_raises(ValueError, decode_bloscpack_header, 'xxxxxxxx')

def test_pack_unpack():
    pack_unpack(2e6)
    pack_unpack(2e6, nchunks=20)

def pack_unpack_extended():
    pack_unpack(2e8)

def pack_unpack(nnumbers, nchunks=None):
    tdir = tempfile.mkdtemp()
    blosc_args = {'typesize': 4,
                  'clevel' : 7,
                  'shuffle' : True}
    in_file = path.join(tdir, 'file')
    out_file = path.join(tdir, 'file.blp')
    dcmp_file = path.join(tdir, 'file.dcmp')
    array_ = numpy.linspace(0, 100, nnumbers)
    with open(in_file, 'wb') as in_fp:
        in_fp.write(array_.tostring())
    pack_file(in_file, out_file, blosc_args, nchunks=nchunks)
    unpack_file(out_file, dcmp_file)
    cmp(in_file, dcmp_file)

def cmp(file1, file2):
    """ File comparison utility with a 500 MB chunksize """
    with open(file1, 'rb') as afp, open(file2, 'rb') as bfp:
        while True:
            a = afp.read(524288000)
            b = bfp.read(524288000)
            if a == '' and b == '':
                return True
            else:
                nt.assert_equal(a, b)
