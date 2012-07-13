#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

import os.path as path
import tempfile
import contextlib
import shutil
import struct
import numpy
import nose.tools as nt
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
    # and thus stuff not fitting into the header
    nt.assert_raises(ChunkingException, calculate_nchunks,
            MAX_CHUNKS+1, chunk_size=1)
    # check overflow of chunk-size due to nchunks being too small
    nt.assert_raises(ChunkingException,
            calculate_nchunks, blosc.BLOSC_MAX_BUFFERSIZE*2+1, nchunks=2)

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

def test_create_bloscpack_header():
    nt.assert_equal('%s\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' %
            MAGIC, create_bloscpack_header(0))
    nt.assert_equal('%s\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00' %
            MAGIC, create_bloscpack_header(1))
    nt.assert_equal('%s\x01\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff' %
            MAGIC, create_bloscpack_header(None))
    nt.assert_equal('%s\x01\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\x7f' %
            MAGIC, create_bloscpack_header(MAX_CHUNKS))
    nt.assert_equal('%s\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' %
            MAGIC, create_bloscpack_header(nchunks=0, format_version=2))
    nt.assert_equal('%s\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' %
            MAGIC, create_bloscpack_header(nchunks=0,
                format_version=MAX_FORMAT_VERSION))
    nt.assert_equal('%s\xff\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00' %
            MAGIC, create_bloscpack_header(nchunks=1,
                format_version=MAX_FORMAT_VERSION))
    nt.assert_raises(struct.error, create_bloscpack_header, nchunks=1,
            format_version=MAX_FORMAT_VERSION+1)
    nt.assert_raises(struct.error, create_bloscpack_header, nchunks=1,
            format_version=-1)

def test_decode_bloscpack_header():
    nt.assert_equal((0, 1), decode_bloscpack_header(
        '%s\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' % MAGIC))
    nt.assert_equal((1, 1), decode_bloscpack_header(
        '%s\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00' % MAGIC))
    nt.assert_equal((MAX_CHUNKS, 1), decode_bloscpack_header(
        '%s\x01\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\x7f' % MAGIC))
    nt.assert_raises(ValueError, decode_bloscpack_header, 'blpk')
    nt.assert_raises(ValueError, decode_bloscpack_header, 'xxxxxxxx')

def create_array(repeats, in_file, progress=None):
    with open(in_file, 'w') as in_fp:
        for i in range(repeats):
            array_ = numpy.linspace(i, i+1, 2e6)
            in_fp.write(array_.tostring())
            if progress is not None:
                progress(i)

@contextlib.contextmanager
def create_tmp_files():
    tdir = tempfile.mkdtemp()
    in_file = path.join(tdir, 'file')
    out_file = path.join(tdir, 'file.blp')
    dcmp_file = path.join(tdir, 'file.dcmp')
    yield tdir, in_file, out_file, dcmp_file
    shutil.rmtree(tdir)

def test_pack_unpack():
    pack_unpack(1)
    pack_unpack(1, nchunks=20)
    pack_unpack(1, nchunks=1)
    pack_unpack(1, nchunks=100)
    pack_unpack(1, chunk_size=reverse_pretty('1M'))
    pack_unpack(1, chunk_size=reverse_pretty('2M'))
    pack_unpack(1, chunk_size=reverse_pretty('4M'))
    pack_unpack(1, chunk_size=reverse_pretty('8M'))

def test_invalid_format():
    def raising_error(message):
        raise ValueError(message)
    bloscpack.error = raising_error
    # this will cause a bug if we ever reach 255 format versions
    bloscpack.FORMAT_VERSION = MAX_FORMAT_VERSION
    blosc_args = DEFAULT_BLOSC_ARGS
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        bloscpack.pack_file(in_file, out_file, blosc_args)
        nt.assert_raises(ValueError, unpack_file, out_file, dcmp_file)
    bloscpack.error = error
    bloscpack.FORMAT_VERSION = FORMAT_VERSION

def pack_unpack_hard():
    """ Test on somewhat larer arrays, but be nice to memory. """
    # Array is apprx. 1.5 GB large
    # should make chunk-size of apprx. 1MB
    pack_unpack(100, nchunks=1536)
    # should make apprx 1536 chunks
    pack_unpack(100, chunk_size=reverse_pretty('1M'))

def pack_unpack_extreme():
    """ Test on somewhat larer arrays, uses loads of memory. """
    # this will create a huge array, and then use the
    # blosc.BLOSC_MAX_BUFFERSIZE as chunk-szie
    pack_unpack(1000)

def pack_unpack(repeats, nchunks=None, chunk_size=None):
    blosc_args = DEFAULT_BLOSC_ARGS
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(repeats, in_file)
        pack_file(in_file, out_file, blosc_args,
                nchunks=nchunks, chunk_size=chunk_size)
        unpack_file(out_file, dcmp_file)
        cmp(in_file, dcmp_file)

def cmp(file1, file2):
    """ File comparison utility with a small chunksize """
    chunk_size = reverse_pretty(DEFAULT_CHUNK_SIZE)
    with open(file1, 'rb') as afp, open(file2, 'rb') as bfp:
        while True:
            a = afp.read(chunk_size)
            b = bfp.read(chunk_size)
            if a == '' and b == '':
                return True
            else:
                nt.assert_equal(a, b)
