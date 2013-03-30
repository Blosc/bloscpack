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

def test_codecs():
    nt.assert_equal(CODECS_AVAIL, ['None', 'zlib'])
    random_str = "4KzGCl7SxTsYLaerommsMWyZg1TXbV6wsR9Xk"
    for i,c in enumerate(CODECS):
        nt.assert_equal(random_str, c.decompress(
            c.compress(random_str, DEFAULT_META_LEVEL)))

def test_serializers():
    nt.assert_equal(SERIZLIALIZERS_AVAIL, ['JSON'])
    output = '{"dtype":"float64","shape":[1024],"others":[]}'
    input_ = eval(output)
    for s in SERIZLIALIZERS:
        nt.assert_equal(output, s.dumps(input_))
        nt.assert_equal(input_, s.loads(output))


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


def test_check_blosc_arguments():
    missing = DEFAULT_BLOSC_ARGS.copy()
    missing.pop('typesize')
    nt.assert_raises(ValueError, bloscpack._check_blosc_args, missing)
    extra = DEFAULT_BLOSC_ARGS.copy()
    extra['wtf'] = 'wtf'
    nt.assert_raises(ValueError, bloscpack._check_blosc_args, extra)


def test_check_bloscpack_arguments():
    missing = DEFAULT_BLOSCPACK_ARGS.copy()
    missing.pop('offsets')
    nt.assert_raises(ValueError, bloscpack._check_bloscpack_args, missing)
    extra = DEFAULT_BLOSCPACK_ARGS.copy()
    extra['wtf'] = 'wtf'
    nt.assert_raises(ValueError, bloscpack._check_bloscpack_args, extra)


def test_check_metadata_arguments():
    missing = DEFAULT_METADATA_ARGS.copy()
    missing.pop('magic_format')
    nt.assert_raises(ValueError, bloscpack._check_metadata_arguments, missing)
    extra = DEFAULT_METADATA_ARGS.copy()
    extra['wtf'] = 'wtf'
    nt.assert_raises(ValueError, bloscpack._check_metadata_arguments, extra)


def test_check_range():
    nt.assert_raises(TypeError,  check_range, 'test', 'a', 0, 1 )
    nt.assert_raises(ValueError, check_range, 'test', -1, 0, 1 )
    nt.assert_raises(ValueError, check_range, 'test', 2, 0, 1 )


def test_calculate_nchunks():
    # check for zero or negative chunk_size
    nt.assert_raises(ValueError, calculate_nchunks,
            23, chunk_size=0)
    nt.assert_raises(ValueError, calculate_nchunks,
            23, chunk_size=-1)

    nt.assert_equal((9, 1, 1), calculate_nchunks(9, chunk_size=1))
    nt.assert_equal((5, 2, 1), calculate_nchunks(9, chunk_size=2))
    nt.assert_equal((3, 3, 3), calculate_nchunks(9, chunk_size=3))
    nt.assert_equal((3, 4, 1), calculate_nchunks(9, chunk_size=4))
    nt.assert_equal((2, 5, 4), calculate_nchunks(9, chunk_size=5))
    nt.assert_equal((2, 6, 3), calculate_nchunks(9, chunk_size=6))
    nt.assert_equal((2, 7, 2), calculate_nchunks(9, chunk_size=7))
    nt.assert_equal((2, 8, 1), calculate_nchunks(9, chunk_size=8))
    nt.assert_equal((1, 9, 9), calculate_nchunks(9, chunk_size=9))

    # check downgrade
    nt.assert_equal((1, 23, 23), calculate_nchunks(23, chunk_size=24))

    # single byte file
    nt.assert_equal((1, 1,  1),
            calculate_nchunks(1, chunk_size=1))

    # check that a zero length file raises an error
    nt.assert_raises(ValueError, calculate_nchunks, 0)
    # in_file_size must be strictly positive
    nt.assert_raises(ValueError, calculate_nchunks, -1)

    # check overflow of nchunks due to chunk_size being too small
    # and thus stuff not fitting into the header
    nt.assert_raises(ChunkingException, calculate_nchunks,
            MAX_CHUNKS+1, chunk_size=1)

    # check that strings are converted correctly
    nt.assert_equal((6, 1048576, 209715),
            calculate_nchunks(reverse_pretty('5.2M')))
    nt.assert_equal((3, 2097152, 1258291),
            calculate_nchunks(reverse_pretty('5.2M'),
                chunk_size='2M'))

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


def test_check_options():
    # check for non-string
    nt.assert_raises(TypeError, bloscpack._check_options, 0)
    nt.assert_raises(TypeError, bloscpack._check_options, 1)
    # check for lengths too small and too large
    nt.assert_raises(ValueError, bloscpack._check_options, '0')
    nt.assert_raises(ValueError, bloscpack._check_options, '1')
    nt.assert_raises(ValueError, bloscpack._check_options, '0000000')
    nt.assert_raises(ValueError, bloscpack._check_options, '000000000')
    nt.assert_raises(ValueError, bloscpack._check_options, '1111111')
    nt.assert_raises(ValueError, bloscpack._check_options, '111111111')
    # check for non zeros and ones
    nt.assert_raises(ValueError, bloscpack._check_options, '0000000a')
    nt.assert_raises(ValueError, bloscpack._check_options, 'aaaaaaaa')


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
    nt.assert_raises(NoSuchChecksum, create_bloscpack_header, checksum='foo')
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

    # check value of max_app_chunks
    nt.assert_raises(ValueError, create_bloscpack_header, max_app_chunks=MAX_CHUNKS+1)
    nt.assert_raises(ValueError, create_bloscpack_header, max_app_chunks=-1)
    nt.assert_raises(TypeError, create_bloscpack_header, max_app_chunks='foo')

    # check sum
    nt.assert_raises(ValueError, create_bloscpack_header,
            nchunks=MAX_CHUNKS/2+1,
            max_app_chunks=MAX_CHUNKS/2+1)

    # check constrain on last_chunk
    nt.assert_raises(ValueError, create_bloscpack_header,
            chunk_size=1,
            last_chunk=2)


def test_create_bloscpack_header():
    # test with no arguments
    raw = MAGIC + struct.pack('<B', FORMAT_VERSION) + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff'+ \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'

    def mod_raw(offset, value):
        return raw[0:offset] + value + \
            raw[offset+len(value):]
    nt.assert_equal(raw, create_bloscpack_header())

    nt.assert_equal(mod_raw(4, struct.pack('<B', 23)),
            create_bloscpack_header(format_version=23))
    # test with options
    nt.assert_equal(mod_raw(5, '\x01'), create_bloscpack_header(offsets=True))
    nt.assert_equal(mod_raw(5, '\x02'), create_bloscpack_header(metadata=True))
    nt.assert_equal(mod_raw(5, '\x03'),
            create_bloscpack_header(offsets=True,metadata=True))
    # test with checksum
    nt.assert_equal(mod_raw(6, '\x01'),
            create_bloscpack_header(checksum='adler32'))
    nt.assert_equal(mod_raw(6, '\x08'),
            create_bloscpack_header(checksum='sha512'))
    # test with typesize
    nt.assert_equal(mod_raw(7, '\x01'), create_bloscpack_header(typesize=1))
    nt.assert_equal(mod_raw(7, '\x02'), create_bloscpack_header(typesize=2))
    nt.assert_equal(mod_raw(7, '\x04'), create_bloscpack_header(typesize=4))
    nt.assert_equal(mod_raw(7, '\x10'), create_bloscpack_header(typesize=16))
    nt.assert_equal(mod_raw(7, '\xff'), create_bloscpack_header(typesize=255))

    # test with chunksize
    nt.assert_equal(mod_raw(8, '\xff\xff\xff\xff'),
            create_bloscpack_header(chunk_size=-1))
    nt.assert_equal(mod_raw(8, '\x01\x00\x00\x00'),
            create_bloscpack_header(chunk_size=1))
    nt.assert_equal(mod_raw(8, '\x00\x00\x10\x00'),
            create_bloscpack_header(chunk_size=reverse_pretty('1M')))
    nt.assert_equal(mod_raw(8, '\xef\xff\xff\x7f'),
            create_bloscpack_header(chunk_size=blosc.BLOSC_MAX_BUFFERSIZE))

    # test with last_chunk
    nt.assert_equal(mod_raw(12, '\xff\xff\xff\xff'),
            create_bloscpack_header(last_chunk=-1))
    nt.assert_equal(mod_raw(12, '\x01\x00\x00\x00'),
            create_bloscpack_header(last_chunk=1))
    nt.assert_equal(mod_raw(12, '\x00\x00\x10\x00'),
            create_bloscpack_header(last_chunk=reverse_pretty('1M')))
    nt.assert_equal(mod_raw(12, '\xef\xff\xff\x7f'),
            create_bloscpack_header(last_chunk=blosc.BLOSC_MAX_BUFFERSIZE))

    # test nchunks
    nt.assert_equal(mod_raw(16, '\xff\xff\xff\xff\xff\xff\xff\xff'),
            create_bloscpack_header(nchunks=-1))
    nt.assert_equal(mod_raw(16, '\x00\x00\x00\x00\x00\x00\x00\x00'),
            create_bloscpack_header(nchunks=0))
    nt.assert_equal(mod_raw(16, '\x01\x00\x00\x00\x00\x00\x00\x00'),
            create_bloscpack_header(nchunks=1))
    nt.assert_equal(mod_raw(16, '\x7f\x00\x00\x00\x00\x00\x00\x00'),
            create_bloscpack_header(nchunks=127))
    nt.assert_equal(mod_raw(16, '\xff\xff\xff\xff\xff\xff\xff\x7f'),
            create_bloscpack_header(nchunks=MAX_CHUNKS))

    # test max_app_chunks
    nt.assert_equal(mod_raw(16, '\x01\x00\x00\x00\x00\x00\x00\x00'
        '\x00\x00\x00\x00\x00\x00\x00\x00'),
            create_bloscpack_header(nchunks=1, max_app_chunks=0))
    nt.assert_equal(mod_raw(16, '\x01\x00\x00\x00\x00\x00\x00\x00'
        '\x01\x00\x00\x00\x00\x00\x00\x00'),
            create_bloscpack_header(nchunks=1, max_app_chunks=1))
    nt.assert_equal(mod_raw(16, '\x01\x00\x00\x00\x00\x00\x00\x00'
        '\x7f\x00\x00\x00\x00\x00\x00\x00'),
            create_bloscpack_header(nchunks=1, max_app_chunks=127))
    nt.assert_equal(mod_raw(16, '\x01\x00\x00\x00\x00\x00\x00\x00'
        '\xfe\xff\xff\xff\xff\xff\xff\x7f'),
            create_bloscpack_header(nchunks=1, max_app_chunks=MAX_CHUNKS-1))

def test_decode_bloscpack_header():
    no_arg_return  = {
            'format_version': FORMAT_VERSION,
            'offsets':       False,
            'metadata':      False,
            'checksum':      'None',
            'typesize':      0,
            'chunk_size':    -1,
            'last_chunk':    -1,
            'nchunks':       -1,
            'max_app_chunks': 0,
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
    nt.assert_equal(copy_and_set_return('offsets', True),
            decode_bloscpack_header(copy_and_set_input(5, '\x01')))
    nt.assert_equal(copy_and_set_return('metadata', True),
            decode_bloscpack_header(copy_and_set_input(5, '\x02')))
    expected = copy_and_set_return('metadata', True)
    expected['offsets'] = True
    nt.assert_equal(expected,
            decode_bloscpack_header(copy_and_set_input(5, '\x03')))
    # check with checksum
    nt.assert_equal(copy_and_set_return('checksum', 'adler32'),
            decode_bloscpack_header(copy_and_set_input(6, '\x01')))
    nt.assert_equal(copy_and_set_return('checksum', 'sha384'),
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
    # check with max_app_chunks
    nt.assert_equal(copy_and_set_return('max_app_chunks', 1),
            decode_bloscpack_header(copy_and_set_input(24,
                '\x01\x00\x00\x00\x00\x00\x00\x00')))
    nt.assert_equal(copy_and_set_return('max_app_chunks',
        reverse_pretty('1M')),
            decode_bloscpack_header(copy_and_set_input(24,
                '\x00\x00\x10\x00\x00\x00\x00\x00')))
    nt.assert_equal(
            copy_and_set_return('max_app_chunks', MAX_CHUNKS),
            decode_bloscpack_header(copy_and_set_input(24,
                '\xff\xff\xff\xff\xff\xff\xff\x7f')))

def test_create_metadata_header():
    raw = '\x00\x00\x00\x00\x00\x00\x00\x00'\
          '\x00\x00\x00\x00\x00\x00\x00\x00'\
          '\x00\x00\x00\x00\x00\x00\x00\x00'\
          '\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(raw, create_metadata_header())

    def mod_raw(offset, value):
        return raw[0:offset] + value + \
            raw[offset+len(value):]

    nt.assert_equal(mod_raw(0, 'JSON'),
            create_metadata_header(magic_format='JSON'))

    nt.assert_equal(mod_raw(9, '\x01'),
            create_metadata_header(meta_checksum='adler32'))

    nt.assert_equal(mod_raw(10, '\x01'),
            create_metadata_header(meta_codec='zlib'))

    nt.assert_equal(mod_raw(11, '\x01'),
            create_metadata_header(meta_level=1))

    nt.assert_equal(mod_raw(12, '\x01'),
            create_metadata_header(meta_size=1))
    nt.assert_equal(mod_raw(12, '\xff\xff\xff\xff'),
            create_metadata_header(meta_size=MAX_META_SIZE))

    nt.assert_equal(mod_raw(16, '\x01'),
            create_metadata_header(max_meta_size=1))
    nt.assert_equal(mod_raw(16, '\xff\xff\xff\xff'),
            create_metadata_header(max_meta_size=MAX_META_SIZE))

    nt.assert_equal(mod_raw(20, '\x01'),
            create_metadata_header(meta_comp_size=1))
    nt.assert_equal(mod_raw(20, '\xff\xff\xff\xff'),
            create_metadata_header(meta_comp_size=MAX_META_SIZE))

    nt.assert_equal(mod_raw(24, 'sesame'),
            create_metadata_header(user_codec='sesame'))

def test_decode_metadata_header():
    no_arg_return  = {
            'magic_format':        '',
            'meta_options':        '00000000',
            'meta_checksum':       'None',
            'meta_codec':          'None',
            'meta_level':          0,
            'meta_size':           0,
            'max_meta_size':       0,
            'meta_comp_size':      0,
            'user_codec':          '',
            }
    no_arg_input = '\x00\x00\x00\x00\x00\x00\x00\x00'\
                   '\x00\x00\x00\x00\x00\x00\x00\x00'\
                   '\x00\x00\x00\x00\x00\x00\x00\x00'\
                   '\x00\x00\x00\x00\x00\x00\x00\x00'
    nt.assert_equal(no_arg_return, decode_metadata_header(no_arg_input))

    def copy_and_set_return(key, value):
        copy_ = no_arg_return.copy()
        copy_[key] = value
        return copy_

    def copy_and_set_input(offset, value):
        return no_arg_input[0:offset] + value + \
            no_arg_input[offset+len(value):]

    nt.assert_equal(copy_and_set_return('magic_format', 'JSON'),
            decode_metadata_header(copy_and_set_input(0, 'JSON')))

    nt.assert_equal(copy_and_set_return('meta_checksum', 'adler32'),
            decode_metadata_header(copy_and_set_input(9, '\x01')))

    nt.assert_equal(copy_and_set_return('meta_codec', 'zlib'),
            decode_metadata_header(copy_and_set_input(10, '\x01')))

    nt.assert_equal(copy_and_set_return('meta_level', 1),
            decode_metadata_header(copy_and_set_input(11, '\x01')))

    nt.assert_equal(copy_and_set_return('meta_size', 1),
            decode_metadata_header(copy_and_set_input(12, '\x01\x00\x00\x00')))

    nt.assert_equal(copy_and_set_return('meta_size', MAX_META_SIZE),
            decode_metadata_header(copy_and_set_input(12, '\xff\xff\xff\xff')))

    nt.assert_equal(copy_and_set_return('max_meta_size', 1),
            decode_metadata_header(copy_and_set_input(16, '\x01\x00\x00\x00')))

    nt.assert_equal(copy_and_set_return('max_meta_size', MAX_META_SIZE),
            decode_metadata_header(copy_and_set_input(16, '\xff\xff\xff\xff')))

    nt.assert_equal(copy_and_set_return('max_meta_size', 1),
            decode_metadata_header(copy_and_set_input(16, '\x01\x00\x00\x00')))

    nt.assert_equal(copy_and_set_return('max_meta_size', MAX_META_SIZE),
            decode_metadata_header(copy_and_set_input(16, '\xff\xff\xff\xff')))

    nt.assert_equal(copy_and_set_return('meta_comp_size', 1),
            decode_metadata_header(copy_and_set_input(20, '\x01\x00\x00\x00')))

    nt.assert_equal(copy_and_set_return('meta_comp_size', MAX_META_SIZE),
            decode_metadata_header(copy_and_set_input(20, '\xff\xff\xff\xff')))

    nt.assert_equal(copy_and_set_return('user_codec', 'sesame'),
            decode_metadata_header(copy_and_set_input(24, 'sesame')))

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
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        bloscpack.pack_file(in_file, out_file, chunk_size='2M')
        with open(out_file, 'r+b') as input_fp:
            bloscpack_header = bloscpack._read_bloscpack_header(input_fp)
            total_entries = bloscpack_header['nchunks'] + \
                    bloscpack_header['max_app_chunks']
            offsets = bloscpack._read_offsets(input_fp, bloscpack_header)
            # First chunks should start after header and offsets
            first = BLOSCPACK_HEADER_LENGTH + 8 * total_entries
            # We assume that the others are correct
            nt.assert_equal(offsets[0], first)
            nt.assert_equal([736, 418578, 736870, 1050327,
                1363364, 1660766, 1959218, 2257703],
                    offsets)
            # try to read the second header
            input_fp.seek(offsets[1], 0)
            blosc_header_raw = input_fp.read(BLOSC_HEADER_LENGTH)
            expected = {'versionlz': 1,
                        'blocksize': 131072,
                        'ctbytes':   318288,
                        'version':   2,
                        'flags':     1,
                        'nbytes':    2097152,
                        'typesize':  8}
            blosc_header = decode_blosc_header(blosc_header_raw)
            nt.assert_equal(expected, blosc_header)

    # now check the same thing again, but w/o any max_app_chunks
    input_fp, output_fp = StringIO(), StringIO()
    create_array_fp(1, input_fp)
    nchunks, chunk_size, last_chunk_size = \
            calculate_nchunks(input_fp.tell(), chunk_size='2M')
    input_fp.seek(0, 0)
    bloscpack_args = DEFAULT_BLOSCPACK_ARGS.copy()
    bloscpack_args['max_app_chunks'] = 0
    bloscpack._pack_fp(input_fp, output_fp,
            nchunks, chunk_size, last_chunk_size,
            bloscpack_args=bloscpack_args
            )
    output_fp.seek(0, 0)
    bloscpack_header = bloscpack._read_bloscpack_header(output_fp)
    nt.assert_equal(0, bloscpack_header['max_app_chunks'])
    offsets = bloscpack._read_offsets(output_fp, bloscpack_header)
    nt.assert_equal([96, 417938, 736230, 1049687,
        1362724, 1660126, 1958578, 2257063],
            offsets)

def test_metadata():
    test_metadata = {'dtype': 'float64',
                     'shape': [1024],
                     'others': [],
                     }
    received_metadata = pack_unpack_fp(1, metadata=test_metadata)
    nt.assert_equal(test_metadata, received_metadata)

def test_rewrite_metadata():
    test_metadata = {'dtype': 'float64',
                     'shape': [1024],
                     'others': [],
                     }
    # assemble the metadata args from the default
    metadata_args = DEFAULT_METADATA_ARGS.copy()
    # avoid checksum and codec
    metadata_args['meta_checksum'] = 'None'
    metadata_args['meta_codec'] = 'None'
    # preallocate a fixed size
    metadata_args['max_meta_size'] = 1000  # fixed preallocation
    target_fp = StringIO()
    # write the metadata section
    bloscpack._write_metadata(target_fp, test_metadata, metadata_args)
    # check that the length is correct
    nt.assert_equal(METADATA_HEADER_LENGTH + metadata_args['max_meta_size'],
            len(target_fp.getvalue()))

    # now add stuff to the metadata
    test_metadata['container'] = 'numpy'
    test_metadata['data_origin'] = 'LHC'
    # compute the new length
    new_metadata_length = len(SERIZLIALIZERS[0].dumps(test_metadata))
    # jam the new metadata into the cStringIO
    target_fp.seek(0, 0)
    bloscpack._rewrite_metadata_fp(target_fp, test_metadata,
            codec=None, level=None)
    # now seek back, read the metadata and make sure it has been updated
    # correctly
    target_fp.seek(0, 0)
    result_metadata, result_header = bloscpack._read_metadata(target_fp)
    nt.assert_equal(test_metadata, result_metadata)
    nt.assert_equal(new_metadata_length, result_header['meta_comp_size'])

    # make sure that NoChangeInMetadata is raised
    target_fp.seek(0, 0)
    nt.assert_raises(NoChangeInMetadata, bloscpack._rewrite_metadata_fp,
            target_fp, test_metadata, codec=None, level=None)

    # make sure that ChecksumLengthMismatch is raised, needs modified metadata
    target_fp.seek(0, 0)
    test_metadata['fluxcompensator'] = 'back to the future'
    nt.assert_raises(ChecksumLengthMismatch, bloscpack._rewrite_metadata_fp,
            target_fp, test_metadata,
            codec=None, level=None, checksum='sha512')
    # len of metadata when dumped to json should be around 1105
    for i in range(100):
        test_metadata[str(i)] = str(i)
    target_fp.seek(0, 0)
    nt.assert_raises(MetadataSectionTooSmall, bloscpack._rewrite_metadata_fp,
            target_fp, test_metadata, codec=None, level=None)


def test_metadata_opportunisitic_compression():
    # make up some metadata that can be compressed with benefit
    test_metadata = ("{'dtype': 'float64', 'shape': [1024], 'others': [],"
            "'original_container': 'carray'}")
    target_fp = StringIO()
    bloscpack._write_metadata(target_fp, test_metadata, DEFAULT_METADATA_ARGS)
    target_fp.seek(0, 0)
    metadata, header = bloscpack._read_metadata(target_fp)
    nt.assert_equal('zlib', header['meta_codec'])

    # now do the same thing, but use badly compressible metadata
    test_metadata = "abc"
    target_fp = StringIO()
    # default args say: do compression...
    bloscpack._write_metadata(target_fp, test_metadata, DEFAULT_METADATA_ARGS)
    target_fp.seek(0, 0)
    metadata, header = bloscpack._read_metadata(target_fp)
    # but it wasn't of any use
    nt.assert_equal('None', header['meta_codec'])


def test_disable_offsets():
    in_fp, out_fp, dcmp_fp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, in_fp)
    in_fp_size = in_fp.tell()
    in_fp.seek(0)
    bloscpack_args = DEFAULT_BLOSCPACK_ARGS.copy()
    bloscpack_args['offsets'] = False
    bloscpack._pack_fp(in_fp, out_fp,
            *calculate_nchunks(in_fp_size),
            bloscpack_args=bloscpack_args)
    out_fp.seek(0)
    bloscpack_header, metadata, metadata_header, offsets = \
            bloscpack._read_beginning(out_fp)
    nt.assert_true(len(offsets) == 0)


def test_invalid_format():
    # this will cause a bug if we ever reach 255 format versions
    bloscpack.FORMAT_VERSION = MAX_FORMAT_VERSION
    blosc_args = DEFAULT_BLOSC_ARGS
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        bloscpack.pack_file(in_file, out_file, blosc_args=blosc_args)
        nt.assert_raises(FormatVersionMismatch, unpack_file, out_file, dcmp_file)
    bloscpack.FORMAT_VERSION = FORMAT_VERSION

def test_file_corruption():
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        pack_file(in_file, out_file)
        # now go in and modify a byte in the file
        with open(out_file, 'r+b') as input_fp:
            # read offsets and header
            bloscpack._read_offsets(input_fp,
                    bloscpack._read_bloscpack_header(input_fp))
            # read the blosc header of the first chunk
            input_fp.read(BLOSC_HEADER_LENGTH)
            # read four bytes
            input_fp.read(4)
            # read the fifth byte
            fifth = input_fp.read(1)
            # figure out what to replace it by
            replace = '\x00' if fifth == '\xff' else '\xff'
            # seek one byte back relative to current position
            input_fp.seek(-1, 1)
            # write the flipped byte
            input_fp.write(replace)
        # now attempt to unpack it
        nt.assert_raises(ChecksumMismatch, unpack_file, out_file, dcmp_file)

def pack_unpack(repeats, chunk_size=None, progress=False):
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        if progress:
            print("Creating test array")
        create_array(repeats, in_file, progress=progress)
        if progress:
            print("Compressing")
        pack_file(in_file, out_file, chunk_size=chunk_size)
        if progress:
            print("Decompressing")
        unpack_file(out_file, dcmp_file)
        if progress:
            print("Verifying")
        cmp(in_file, dcmp_file)

def pack_unpack_fp(repeats, chunk_size=DEFAULT_CHUNK_SIZE,
        progress=False, metadata=None):
    in_fp, out_fp, dcmp_fp = StringIO(), StringIO(), StringIO()
    if progress:
        print("Creating test array")
    create_array_fp(repeats, in_fp, progress=progress)
    in_fp_size = in_fp.tell()
    if progress:
        print("Compressing")
    in_fp.seek(0)
    nchunks, chunk_size, last_chunk_size = \
            calculate_nchunks(in_fp_size, chunk_size)
    bloscpack._pack_fp(in_fp, out_fp,
            nchunks, chunk_size, last_chunk_size,
            metadata=metadata)
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
    pack_unpack(1, chunk_size=reverse_pretty('1M'))
    pack_unpack(1, chunk_size=reverse_pretty('2M'))
    pack_unpack(1, chunk_size=reverse_pretty('4M'))
    pack_unpack(1, chunk_size=reverse_pretty('8M'))

def test_pack_unpack_fp():
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

def prep_array_for_append(blosc_args=DEFAULT_BLOSC_ARGS,
        bloscpack_args=DEFAULT_BLOSCPACK_ARGS):
    orig, new, dcmp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, new)
    new_size = new.tell()
    new.reset()
    chunking = calculate_nchunks(new_size)
    bloscpack._pack_fp(new, orig, *chunking,
            blosc_args=blosc_args,
            bloscpack_args=bloscpack_args)
    orig.reset()
    new.reset()
    return orig, new, new_size, dcmp

def test_append_fp():
    orig, new, new_size, dcmp = prep_array_for_append()

    # check that the header and offsets are as we expected them to be
    orig_bloscpack_header, orig_metadata, orig_metadata_header, orig_offsets = \
            bloscpack._read_beginning(orig)
    orig.reset()
    expected_orig_bloscpack_header = {
            'chunk_size': 1048576,
            'nchunks': 16,
            'last_chunk': 271360,
            'max_app_chunks': 160,
            'format_version': 3,
            'offsets': True,
            'checksum': 'adler32',
            'typesize': 8,
            'metadata': False,
    }
    expected_orig_offsets = [1440, 221122, 419302, 576717, 737614,
                             894182, 1051091, 1208872, 1364148,
                             1512476, 1661570, 1811035, 1960042,
                             2109263, 2258547, 2407759]
    nt.assert_equal(expected_orig_bloscpack_header, orig_bloscpack_header)
    nt.assert_equal(expected_orig_offsets, orig_offsets)

    # perform the append
    bloscpack.append_fp(orig, new, new_size)
    orig.reset()

    # check that the header and offsets are as we expected them to be after
    # appending
    app_bloscpack_header, app_metadata, app_metadata_header, app_offsets = \
            bloscpack._read_beginning(orig)
    orig.reset()
    expected_app_bloscpack_header = {
            'chunk_size': 1048576,
            'nchunks': 31,
            'last_chunk': 542720,
            'max_app_chunks': 145,
            'format_version': 3,
            'offsets': True,
            'checksum': 'adler32',
            'typesize': 8,
            'metadata': False
    }
    expected_app_offsets = [1440, 221122, 419302, 576717, 737614,
                            894182, 1051091, 1208872, 1364148,
                            1512476, 1661570, 1811035, 1960042,
                            2109263, 2258547, 2407759, 2613561,
                            2815435, 2984307, 3141891, 3302879,
                            3459460, 3617126, 3775757, 3925209,
                            4073901, 4223131, 4372322, 4521936,
                            4671276, 4819767]
    nt.assert_equal(expected_app_bloscpack_header, app_bloscpack_header)
    nt.assert_equal(expected_app_offsets, app_offsets)

    # now check by unpacking
    bloscpack._unpack_fp(orig, dcmp)
    dcmp.reset()
    new.reset()
    new_str = new.read()
    dcmp_str = dcmp.read()
    nt.assert_equal(len(dcmp_str), len(new_str * 2))
    nt.assert_equal(dcmp_str, new_str * 2)

    ## TODO
    # * check additional aspects of file integrity
    #   * offsets OK
    #   * metadata OK


def test_append():
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        pack_file(in_file, out_file)
        append(out_file, in_file)
        unpack_file(out_file, dcmp_file)
        in_content = open(in_file, 'rb').read()
        dcmp_content = open(dcmp_file, 'rb').read()
        nt.assert_equal(len(dcmp_content), len(in_content) * 2)
        nt.assert_equal(dcmp_content, in_content * 2)


def test_append_single_chunk():
    orig, new, dcmp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, new)
    new_size = new.tell()
    new.reset()
    chunking = calculate_nchunks(new_size, chunk_size=new_size)
    bloscpack._pack_fp(new, orig, *chunking)
    orig.reset()
    new.reset()
    bloscpack.append_fp(orig, new, new_size)
    orig.reset()
    new.reset()
    bloscpack_header, metadata, metadata_header, offsets = \
            bloscpack._read_beginning(orig)
    orig.reset()
    nt.assert_equal(bloscpack_header['nchunks'], 2)

    bloscpack.append_fp(orig, new, new_size)
    orig.reset()
    new.reset()
    bloscpack_header, metadata, metadata_header, offsets = \
            bloscpack._read_beginning(orig)
    orig.reset()
    nt.assert_equal(bloscpack_header['nchunks'], 3)


def test_double_append():
    orig, new, new_size, dcmp = prep_array_for_append()
    bloscpack.append_fp(orig, new, new_size)
    orig.reset()
    new.reset()
    bloscpack.append_fp(orig, new, new_size)
    orig.reset()
    new.reset()
    new_str = new.read()
    bloscpack._unpack_fp(orig, dcmp)
    dcmp.reset()
    dcmp_str = dcmp.read()
    nt.assert_equal(len(dcmp_str), len(new_str) * 3)
    nt.assert_equal(dcmp_str, new_str * 3)


def test_append_fp_no_offsets():
    bloscpack_args = DEFAULT_BLOSCPACK_ARGS.copy()
    bloscpack_args['offsets'] = False
    orig, new, new_size, dcmp = prep_array_for_append(bloscpack_args=bloscpack_args)
    nt.assert_raises(RuntimeError, bloscpack.append_fp, orig, new, new_size)

def test_append_fp_not_enough_space():
    bloscpack_args = DEFAULT_BLOSCPACK_ARGS.copy()
    bloscpack_args['max_app_chunks'] = 0
    orig, new, new_size, dcmp = prep_array_for_append(bloscpack_args=bloscpack_args)
    nt.assert_raises(NotEnoughSpace, bloscpack.append_fp, orig, new, new_size)


def test_mixing_clevel():
    # the first set of chunks has max compression
    blosc_args = DEFAULT_BLOSC_ARGS.copy()
    blosc_args['clevel'] = 9
    orig, new, new_size, dcmp = prep_array_for_append()
    # get the original size
    orig.seek(0, 2)
    orig_size = orig.tell()
    orig.reset()
    # get a backup of the settings
    bloscpack_header, metadata, metadata_header, offsets = \
            bloscpack._read_beginning(orig)
    orig.reset()
    # compressed size of the last chunk, including checksum
    last_chunk_compressed_size = orig_size - offsets[-1]

    # do append
    blosc_args = DEFAULT_BLOSC_ARGS.copy()
    # use the typesize from the file
    blosc_args['typesize'] = None
    # make the second set of chunks have no compression
    blosc_args['clevel'] = 0
    nchunks = bloscpack.append_fp(orig, new, new_size, blosc_args=blosc_args)

    # get the final size
    orig.seek(0, 2)
    final_size = orig.tell()
    orig.reset()

    # the original file minus the compressed size of the last chunk
    discounted_orig_size = orig_size - last_chunk_compressed_size
    # size of the appended data
    #  * raw new size, since we have no compression
    #  * uncompressed size of the last chunk
    #  * nchunks + 1 times the blosc and checksum overhead
    appended_size = new_size + bloscpack_header['last_chunk'] + (nchunks+1) * (16 + 4)
    # final size should be original plus appended data
    nt.assert_equal(final_size, appended_size + discounted_orig_size)

    # check by unpacking
    bloscpack._unpack_fp(orig, dcmp)
    dcmp.reset()
    new.reset()
    new_str = new.read()
    dcmp_str = dcmp.read()
    nt.assert_equal(len(dcmp_str), len(new_str * 2))
    nt.assert_equal(dcmp_str, new_str * 2)


def test_append_mix_shuffle():
    orig, new, new_size, dcmp = prep_array_for_append()
    blosc_args = DEFAULT_BLOSC_ARGS.copy()
    # use the typesize from the file
    blosc_args['typesize'] = None
    # deactivate shuffle
    blosc_args['shuffle'] = False
    # crank up the clevel to ensure compression happens, otherwise the flags
    # will be screwed later on
    blosc_args['clevel'] = 9
    bloscpack.append_fp(orig, new, new_size, blosc_args=blosc_args)
    orig.reset()
    bloscpack._unpack_fp(orig, dcmp)
    dcmp.reset()
    new.reset()
    new_str = new.read()
    dcmp_str = dcmp.read()
    nt.assert_equal(len(dcmp_str), len(new_str * 2))
    nt.assert_equal(dcmp_str, new_str * 2)

    # now get the first and the last chunk and check that the shuffle doesn't
    # match
    orig.reset()
    bloscpack_header, metadata, metadata_header, offsets = \
            bloscpack._read_beginning(orig)
    orig.seek(offsets[0])
    checksum_impl = CHECKSUMS_LOOKUP[bloscpack_header['checksum']]
    compressed_zero, decompressed_zero, blosc_header_zero = \
        bloscpack._unpack_chunk_fp(orig, checksum_impl)
    orig.seek(offsets[-1])
    compressed_last, decompressed_last, blosc_header_last = \
        bloscpack._unpack_chunk_fp(orig, checksum_impl)
    # first chunk has shuffle active
    nt.assert_equal(blosc_header_zero['flags'], 1)
    # last chunk doesn't
    nt.assert_equal(blosc_header_last['flags'], 0)

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
