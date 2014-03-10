#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


import struct


import nose.tools as nt
import blosc
import numpy as np


from bloscpack.args import (DEFAULT_BLOSC_ARGS,
                            )
from bloscpack.constants import (MAGIC,
                                 FORMAT_VERSION,
                                 MAX_FORMAT_VERSION,
                                 MAX_META_SIZE,
                                 MAX_CHUNKS,
                                 )
from bloscpack.pretty import reverse_pretty
from bloscpack import checksums
from bloscpack import exceptions
from bloscpack.headers import (BloscPackHeader,
                               create_options,
                               decode_options,
                               check_options,
                               create_metadata_options,
                               decode_metadata_options,
                               create_metadata_header,
                               decode_metadata_header,
                               check_range,
                               decode_blosc_header,
                               )


def test_check_range():
    nt.assert_raises(TypeError,  check_range, 'test', 'a', 0, 1)
    nt.assert_raises(ValueError, check_range, 'test', -1, 0, 1)
    nt.assert_raises(ValueError, check_range, 'test', 2, 0, 1)


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


def test_check_options():
    # check for non-string
    nt.assert_raises(TypeError, check_options, 0)
    nt.assert_raises(TypeError, check_options, 1)
    # check for lengths too small and too large
    nt.assert_raises(ValueError, check_options, '0')
    nt.assert_raises(ValueError, check_options, '1')
    nt.assert_raises(ValueError, check_options, '0000000')
    nt.assert_raises(ValueError, check_options, '000000000')
    nt.assert_raises(ValueError, check_options, '1111111')
    nt.assert_raises(ValueError, check_options, '111111111')
    # check for non zeros and ones
    nt.assert_raises(ValueError, check_options, '0000000a')
    nt.assert_raises(ValueError, check_options, 'aaaaaaaa')


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


def test_decode_blosc_header():
    array_ = np.linspace(0, 100, 2e4).tostring()
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
                'flags': 0,  # no shuffle flag
                'nbytes': len(array_),
                'typesize': blosc_args['typesize']}
    nt.assert_equal(expected, header)
    # uncompressible data
    array_ = np.asarray(np.random.randn(23),
                        dtype=np.float32).tostring()
    blosc_args['shuffle'] = True
    compressed = blosc.compress(array_, **blosc_args)
    header = decode_blosc_header(compressed)
    expected = {'versionlz': 1,
                'blocksize': 88,
                'ctbytes': len(array_) + 16,  # original + 16 header bytes
                'version': 2,
                'flags': 3,  # 1 for shuffle 2 for non-compressed
                'nbytes': len(array_),
                'typesize': blosc_args['typesize']}
    nt.assert_equal(expected, header)


def test_BloscPackHeader_constructor_exceptions():
    # uses nose test generators

    def check(error_type, args_dict):
        nt.assert_raises(error_type, BloscPackHeader, **args_dict)

    for error_type, args_dict in [
            (ValueError, {'format_version': -1}),
            (ValueError, {'format_version': MAX_FORMAT_VERSION+1}),
            (TypeError,  {'format_version': 'foo'}),
            (ValueError, {'checksum': -1}),
            (ValueError, {'checksum': len(checksums.CHECKSUMS)+1}),
            (exceptions.NoSuchChecksum, {'checksum': 'foo'}),
            (ValueError, {'typesize': -1}),
            (ValueError, {'typesize': blosc.BLOSC_MAX_TYPESIZE+1}),
            (TypeError,  {'typesize': 'foo'}),
            (ValueError, {'chunk_size': blosc.BLOSC_MAX_BUFFERSIZE+1}),
            (ValueError, {'chunk_size': -2}),
            (TypeError,  {'chunk_size': 'foo'}),
            (ValueError, {'last_chunk': blosc.BLOSC_MAX_BUFFERSIZE+1}),
            (ValueError, {'last_chunk': -2}),
            (TypeError,  {'last_chunk': 'foo'}),
            (ValueError, {'nchunks': MAX_CHUNKS+1}),
            (ValueError, {'nchunks': -2}),
            (TypeError,  {'nchunks': 'foo'}),
            (ValueError, {'max_app_chunks': MAX_CHUNKS+1}),
            (ValueError, {'max_app_chunks': -1}),
            (TypeError,  {'max_app_chunks': 'foo'}),
            # sum of nchunks and max_app_chunks
            (ValueError, {'nchunks': MAX_CHUNKS/2+1,
                          'max_app_chunks': MAX_CHUNKS/2+1}),
            # check that max_app_chunks is zero
            (ValueError, {'nchunks': -1,
                          'max_app_chunks': 1}),
            # check constraint on last chunk, must be equal to or smaller than
            # chunk_size
            (ValueError, {'chunk_size': 1,
                          'last_chunk': 2}),
            ]:
        yield check, error_type, args_dict


def test_BloscPackHeader_total_prospective_entries():
    # uses nose test generators
    def check(expected, nchunks, max_app_chunks):
        header = BloscPackHeader(nchunks=nchunks,
                                 max_app_chunks=max_app_chunks)
        nt.assert_equal(expected, header.total_prospective_chunks)
    for expected, (nchunks, max_app_chunks) in [
            (0, (0, 0)),
            (1, (1, 0)),
            (1, (0, 1)),
            (None, (-1, 0)),
            (65, (42, 23)),
            (MAX_CHUNKS-1, (MAX_CHUNKS//2, MAX_CHUNKS//2)),
            (MAX_CHUNKS, (MAX_CHUNKS-1, 1)),
            (MAX_CHUNKS, (1, MAX_CHUNKS-1)),
            ]:
        yield check, expected, nchunks, max_app_chunks


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
    no_arg_return = {
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


