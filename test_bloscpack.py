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
    bloscpack.sys.exit = lambda x: x+2
    # should probably hijack the print statement
    error('error')
    bloscpack.sys.exit = backup

def test_parser():
    # hmmm I guess we could override the error
    parser = create_parser()

def test_nchunks():
    nt.assert_equal((3, 3, 3), calculate_nchunks(9, nchunks=3))
    nt.assert_equal((9, 1, 1), calculate_nchunks(9, chunk_size=1))
    nt.assert_equal((2, 4, 5), calculate_nchunks(9, chunk_size=4))
    nt.assert_equal((2, 3, 4), calculate_nchunks(7, nchunks=2))
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
