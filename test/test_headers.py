#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


import struct


import nose.tools as nt
from nose_parameterized import parameterized

import blosc

from bloscpack.constants import (MAGIC,
                                 FORMAT_VERSION,
                                 MAX_FORMAT_VERSION,
                                 MAX_CHUNKS,
                                 )
from bloscpack import reverse_pretty
from bloscpack import checksums
from bloscpack import exceptions
from bloscpack.headers import BloscPackHeader


@parameterized([
    (ValueError, -1),
    (ValueError, MAX_FORMAT_VERSION+1),
    (TypeError, 'foo')
])
def test_invalid_format_version(error_type, format_version):
    nt.assert_raises(error_type, BloscPackHeader,
                     format_version=format_version)


@parameterized([
    (ValueError, -1),
    (ValueError, len(checksums.CHECKSUMS)+1),
    (exceptions.NoSuchChecksum, 'foo')
])
def test_invalid_checksum(error_type, checksum):
    nt.assert_raises(error_type, BloscPackHeader,
                     checksum=checksum)


@parameterized([
    (ValueError, -1),
    (ValueError, blosc.BLOSC_MAX_TYPESIZE+1),
    (TypeError, 'foo'),
])
def test_invalid_type_size(error_type, typesize):
    nt.assert_raises(error_type, BloscPackHeader, typesize=typesize)


@parameterized([
    (ValueError, blosc.BLOSC_MAX_BUFFERSIZE+1),
    (ValueError, -2),
    (TypeError, 'foo'),
])
def test_invalid_chunk_size(error_type, chunk_size):
    nt.assert_raises(error_type, BloscPackHeader, chunk_size=chunk_size)


@parameterized([
    (ValueError, blosc.BLOSC_MAX_BUFFERSIZE+1),
    (ValueError, -2),
    (TypeError, 'foo'),
])
def test_invalid_last_chunk(error_type, last_chunk):
    nt.assert_raises(error_type, BloscPackHeader, last_chunk=last_chunk)


@parameterized([
    (ValueError, MAX_CHUNKS+1),
    (ValueError, -2),
    (TypeError, 'foo'),
])
def test_invalid_nchunks(error_type, nchunks):
    nt.assert_raises(error_type, BloscPackHeader, nchunks=nchunks)


@parameterized([
    (ValueError, MAX_CHUNKS+1),
    (ValueError, -1),
    (TypeError, 'foo'),
])
def test_invalid_max_app_chunks(error_type, max_app_chunks):
    nt.assert_raises(error_type, BloscPackHeader, max_app_chunks=max_app_chunks)


def test_BloscPackHeader_constructor_arguments():
    # check sum
    nt.assert_raises(ValueError, BloscPackHeader,
            nchunks=MAX_CHUNKS/2+1,
            max_app_chunks=MAX_CHUNKS/2+1)

    # check constrain on last_chunk
    nt.assert_raises(ValueError, BloscPackHeader,
            chunk_size=1,
            last_chunk=2)


def test_BloscPackHeader_encode():

    # test with no arguments
    raw = MAGIC + struct.pack('<B', FORMAT_VERSION) + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff' + \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'

    def mod_raw(offset, value):
        return raw[0:offset] + value + \
            raw[offset+len(value):]
    nt.assert_equal(raw, BloscPackHeader().encode())

    nt.assert_equal(mod_raw(4, struct.pack('<B', 23)),
            BloscPackHeader(format_version=23).encode())
    # test with options
    nt.assert_equal(mod_raw(5, '\x01'), BloscPackHeader(offsets=True).encode())
    nt.assert_equal(mod_raw(5, '\x02'), BloscPackHeader(metadata=True).encode())
    nt.assert_equal(mod_raw(5, '\x03'),
            BloscPackHeader(offsets=True, metadata=True).encode())
    # test with checksum
    nt.assert_equal(mod_raw(6, '\x01'),
            BloscPackHeader(checksum='adler32').encode())
    nt.assert_equal(mod_raw(6, '\x08'),
            BloscPackHeader(checksum='sha512').encode())
    # test with typesize
    nt.assert_equal(mod_raw(7, '\x01'), BloscPackHeader(typesize=1).encode())
    nt.assert_equal(mod_raw(7, '\x02'), BloscPackHeader(typesize=2).encode())
    nt.assert_equal(mod_raw(7, '\x04'), BloscPackHeader(typesize=4).encode())
    nt.assert_equal(mod_raw(7, '\x10'), BloscPackHeader(typesize=16).encode())
    nt.assert_equal(mod_raw(7, '\xff'), BloscPackHeader(typesize=255).encode())

    # test with chunksize
    nt.assert_equal(mod_raw(8, '\xff\xff\xff\xff'),
            BloscPackHeader(chunk_size=-1).encode())
    nt.assert_equal(mod_raw(8, '\x01\x00\x00\x00'),
            BloscPackHeader(chunk_size=1).encode())
    nt.assert_equal(mod_raw(8, '\x00\x00\x10\x00'),
            BloscPackHeader(chunk_size=reverse_pretty('1M')).encode())
    nt.assert_equal(mod_raw(8, '\xef\xff\xff\x7f'),
            BloscPackHeader(chunk_size=blosc.BLOSC_MAX_BUFFERSIZE).encode())

    # test with last_chunk
    nt.assert_equal(mod_raw(12, '\xff\xff\xff\xff'),
            BloscPackHeader(last_chunk=-1).encode())
    nt.assert_equal(mod_raw(12, '\x01\x00\x00\x00'),
            BloscPackHeader(last_chunk=1).encode())
    nt.assert_equal(mod_raw(12, '\x00\x00\x10\x00'),
            BloscPackHeader(last_chunk=reverse_pretty('1M')).encode())
    nt.assert_equal(mod_raw(12, '\xef\xff\xff\x7f'),
            BloscPackHeader(last_chunk=blosc.BLOSC_MAX_BUFFERSIZE).encode())

    # test nchunks
    nt.assert_equal(mod_raw(16, '\xff\xff\xff\xff\xff\xff\xff\xff'),
            BloscPackHeader(nchunks=-1).encode())
    nt.assert_equal(mod_raw(16, '\x00\x00\x00\x00\x00\x00\x00\x00'),
            BloscPackHeader(nchunks=0).encode())
    nt.assert_equal(mod_raw(16, '\x01\x00\x00\x00\x00\x00\x00\x00'),
            BloscPackHeader(nchunks=1).encode())
    nt.assert_equal(mod_raw(16, '\x7f\x00\x00\x00\x00\x00\x00\x00'),
            BloscPackHeader(nchunks=127).encode())
    nt.assert_equal(mod_raw(16, '\xff\xff\xff\xff\xff\xff\xff\x7f'),
            BloscPackHeader(nchunks=MAX_CHUNKS).encode())

    # test max_app_chunks
    nt.assert_equal(mod_raw(16, '\x01\x00\x00\x00\x00\x00\x00\x00'
        '\x00\x00\x00\x00\x00\x00\x00\x00'),
            BloscPackHeader(nchunks=1, max_app_chunks=0).encode())
    nt.assert_equal(mod_raw(16, '\x01\x00\x00\x00\x00\x00\x00\x00'
        '\x01\x00\x00\x00\x00\x00\x00\x00'),
            BloscPackHeader(nchunks=1, max_app_chunks=1).encode())
    nt.assert_equal(mod_raw(16, '\x01\x00\x00\x00\x00\x00\x00\x00'
        '\x7f\x00\x00\x00\x00\x00\x00\x00'),
            BloscPackHeader(nchunks=1, max_app_chunks=127).encode())
    nt.assert_equal(mod_raw(16, '\x01\x00\x00\x00\x00\x00\x00\x00'
        '\xfe\xff\xff\xff\xff\xff\xff\x7f'),
            BloscPackHeader(nchunks=1, max_app_chunks=MAX_CHUNKS-1).encode())


def test_decode_bloscpack_header():
    bloscpack_header = BloscPackHeader()

    def copy_and_set_return(key, value):
        copy_ = bloscpack_header.copy()
        setattr(copy_, key, value)
        return copy_

    format_version = struct.pack('<B', FORMAT_VERSION)
    no_arg_input = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff' + \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'

    def copy_and_set_input(offset, value):
        return no_arg_input[0:offset] + value + \
            no_arg_input[offset+len(value):]

    # check with no args
    nt.assert_equal(bloscpack_header, BloscPackHeader.decode(no_arg_input))
    # check with format_version
    format_version_set = copy_and_set_input(4, '\x17')
    format_version_set_return = copy_and_set_return('format_version', 23)
    nt.assert_equal(format_version_set_return,
            BloscPackHeader.decode(format_version_set))
    # check with options
    nt.assert_equal(copy_and_set_return('offsets', True),
            BloscPackHeader.decode(copy_and_set_input(5, '\x01')))
    nt.assert_equal(copy_and_set_return('metadata', True),
            BloscPackHeader.decode(copy_and_set_input(5, '\x02')))
    expected = copy_and_set_return('metadata', True)
    expected['offsets'] = True
    nt.assert_equal(expected,
            BloscPackHeader.decode(copy_and_set_input(5, '\x03')))
    # check with checksum
    nt.assert_equal(copy_and_set_return('checksum', 'adler32'),
            BloscPackHeader.decode(copy_and_set_input(6, '\x01')))
    nt.assert_equal(copy_and_set_return('checksum', 'sha384'),
            BloscPackHeader.decode(copy_and_set_input(6, '\x07')))
    # check with typesize
    nt.assert_equal(copy_and_set_return('typesize', 1),
            BloscPackHeader.decode(copy_and_set_input(7, '\x01')))
    nt.assert_equal(copy_and_set_return('typesize', 2),
            BloscPackHeader.decode(copy_and_set_input(7, '\x02')))
    nt.assert_equal(copy_and_set_return('typesize', 4),
            BloscPackHeader.decode(copy_and_set_input(7, '\x04')))
    nt.assert_equal(copy_and_set_return('typesize', 8),
            BloscPackHeader.decode(copy_and_set_input(7, '\x08')))
    nt.assert_equal(copy_and_set_return('typesize', blosc.BLOSC_MAX_TYPESIZE),
            BloscPackHeader.decode(copy_and_set_input(7, '\xff')))
    # check with chunk_size
    nt.assert_equal(copy_and_set_return('chunk_size', 1),
            BloscPackHeader.decode(copy_and_set_input(8, '\x01\x00\x00\x00')))
    nt.assert_equal(copy_and_set_return('chunk_size', reverse_pretty('1M')),
            BloscPackHeader.decode(copy_and_set_input(8, '\x00\x00\x10\x00')))
    nt.assert_equal(
            copy_and_set_return('chunk_size', blosc.BLOSC_MAX_BUFFERSIZE),
            BloscPackHeader.decode(copy_and_set_input(8, '\xef\xff\xff\x7f')))
    # check with last_chunk
    nt.assert_equal(copy_and_set_return('last_chunk', 1),
            BloscPackHeader.decode(copy_and_set_input(12, '\x01\x00\x00\x00')))
    nt.assert_equal(copy_and_set_return('last_chunk', reverse_pretty('1M')),
            BloscPackHeader.decode(copy_and_set_input(12, '\x00\x00\x10\x00')))
    nt.assert_equal(
            copy_and_set_return('last_chunk', blosc.BLOSC_MAX_BUFFERSIZE),
            BloscPackHeader.decode(copy_and_set_input(12, '\xef\xff\xff\x7f')))
    # check with nchunks
    nt.assert_equal(copy_and_set_return('nchunks', 1),
            BloscPackHeader.decode(copy_and_set_input(16,
                '\x01\x00\x00\x00\x00\x00\x00\x00')))
    nt.assert_equal(copy_and_set_return('nchunks', reverse_pretty('1M')),
            BloscPackHeader.decode(copy_and_set_input(16,
                '\x00\x00\x10\x00\x00\x00\x00\x00')))
    nt.assert_equal(
            copy_and_set_return('nchunks', MAX_CHUNKS),
            BloscPackHeader.decode(copy_and_set_input(16,
                '\xff\xff\xff\xff\xff\xff\xff\x7f')))

    # check with max_app_chunks
    # set nchunks to be 1 in header and raw
    bloscpack_header.nchunks = 1
    no_arg_input = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff' + \
        '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

    nt.assert_equal(copy_and_set_return('max_app_chunks', 1),
            BloscPackHeader.decode(copy_and_set_input(24,
                '\x01\x00\x00\x00\x00\x00\x00\x00')))
    nt.assert_equal(copy_and_set_return('max_app_chunks',
        reverse_pretty('1M')),
            BloscPackHeader.decode(copy_and_set_input(24,
                '\x00\x00\x10\x00\x00\x00\x00\x00')))
    # Maximum value is MAX_CHUNKS - 1 since nchunks is already 1
    nt.assert_equal(
            copy_and_set_return('max_app_chunks', MAX_CHUNKS-1),
            BloscPackHeader.decode(copy_and_set_input(24,
                '\xfe\xff\xff\xff\xff\xff\xff\x7f')))
