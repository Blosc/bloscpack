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

    for expected_options, kwargs in [
            ('00000001', {}),
            ('00000001', {'offsets': True}),
            ('00000000', {'offsets': False}),
            ('00000001', {'metadata': False}),
            ('00000011', {'metadata': True}),
            ('00000000', {'offsets': False, 'metadata': False}),
            ('00000010', {'offsets': False, 'metadata': True}),
            ('00000001', {'offsets': True, 'metadata': False}),
            ('00000011', {'offsets': True, 'metadata': True}),
            ]:
        yield nt.assert_equal, expected_options, create_options(**kwargs)


def test_decode_options():
    for expected, input in [
            ({'metadata': False, 'offsets': False}, '00000000'),
            ({'metadata': False, 'offsets': True}, '00000001'),
            ({'metadata': True, 'offsets': False}, '00000010'),
            ({'metadata': True, 'offsets': True}, '00000011'),
            ]:
        yield nt.assert_equal, expected, decode_options(input)


def test_decode_options_exceptions():

    for broken_input in [
            '0000000',
            '000000000',
            '0000000a',
            'abc',
            '00000100',
            '00001100',
            '11111100',
            ]:
        yield nt.assert_raises, ValueError, decode_options, broken_input


def test_check_options_exceptions():
    for broken_input in [
            # check for non-string
            0,
            1,
            ]:
        yield nt.assert_raises, TypeError, check_options, broken_input
    for broken_input in [
            # check for lengths too small and too large
            '0',
            '1',
            '0000000',
            '000000000',
            '1111111',
            '111111111',
            # check for non zeros and ones
            '0000000a',
            'aaaaaaaa',
            ]:
        yield nt.assert_raises, ValueError, check_options, broken_input


def test_create_metadata_options():
    nt.assert_equal('00000000', create_metadata_options())


def test_decode_metadata_options():
    nt.assert_equal({}, decode_metadata_options('00000000'))


def test_decode_metadata_options_exceptions():

    for broken_input in [
            '0000000',
            '000000000',
            '0000000a',
            'abc',
            '00000001',
            '00001111',
            '11111111',
            ]:
        yield nt.assert_raises, ValueError, decode_metadata_options, broken_input


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
        header = BloscPackHeader(nchunks=nchunks,
                                 max_app_chunks=max_app_chunks)
        yield nt.assert_equal, expected, header.total_prospective_chunks


def test_BloscPackHeader_encode():

    # the raw encoded header as produces w/o any kwargs
    format_version = struct.pack('<B', FORMAT_VERSION)
    raw = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff' + \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'

    # modify the raw encoded header with the value starting at offset
    def mod_raw(offset, replacement):
        return raw[0:offset] + replacement + \
            raw[offset+len(replacement):]

    # test with no arguments
    yield nt.assert_equal, raw, BloscPackHeader().encode()

    for offset, replacement, kwargs in [
            (4, struct.pack('<B', 23), {'format_version': 23}),
            # test with options
            (5, '\x01', {'offsets': True}),
            (5, '\x02', {'metadata': True}),
            (5, '\x03', {'offsets': True, 'metadata': True}),
            # test with checksum
            (6, '\x01', {'checksum': 'adler32'}),
            (6, '\x08', {'checksum': 'sha512'}),
            # test with typesize
            (7, '\x01', {'typesize': 1}),
            (7, '\x02', {'typesize': 2}),
            (7, '\x04', {'typesize': 4}),
            (7, '\x10', {'typesize': 16}),
            (7, '\xff', {'typesize': 255}),
            # test with chunksize
            (8, '\xff\xff\xff\xff', {'chunk_size': -1}),
            (8, '\x01\x00\x00\x00', {'chunk_size': 1}),
            (8, '\x00\x00\x10\x00', {'chunk_size': reverse_pretty('1M')}),
            (8, '\xef\xff\xff\x7f', {'chunk_size': blosc.BLOSC_MAX_BUFFERSIZE}),
            # test with last_chunk
            (12, '\xff\xff\xff\xff', {'last_chunk': -1}),
            (12, '\x01\x00\x00\x00', {'last_chunk': 1}),
            (12, '\x00\x00\x10\x00', {'last_chunk': reverse_pretty('1M')}),
            (12, '\xef\xff\xff\x7f', {'last_chunk': blosc.BLOSC_MAX_BUFFERSIZE}),
            # test nchunks
            (16, '\xff\xff\xff\xff\xff\xff\xff\xff', {'nchunks': -1}),
            (16, '\x00\x00\x00\x00\x00\x00\x00\x00', {'nchunks': 0}),
            (16, '\x01\x00\x00\x00\x00\x00\x00\x00', {'nchunks': 1}),
            (16, '\x7f\x00\x00\x00\x00\x00\x00\x00', {'nchunks': 127}),
            (16, '\xff\xff\xff\xff\xff\xff\xff\x7f', {'nchunks': MAX_CHUNKS}),
            # test max_app_chunks
            (16, '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
                {'nchunks': 1, 'max_app_chunks': 0}),
            (16, '\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00',
                {'nchunks': 1, 'max_app_chunks': 1}),
            (16, '\x01\x00\x00\x00\x00\x00\x00\x00\x7f\x00\x00\x00\x00\x00\x00\x00',
                {'nchunks': 1, 'max_app_chunks': 127}),
            # Maximum value is MAX_CHUNKS - 1 since nchunks is already 1
            (16, '\x01\x00\x00\x00\x00\x00\x00\x00\xfe\xff\xff\xff\xff\xff\xff\x7f',
                {'nchunks': 1, 'max_app_chunks': MAX_CHUNKS-1}),
            ]:
        yield nt.assert_equal, mod_raw(offset, replacement), \
            BloscPackHeader(**kwargs).encode()


def test_decode_bloscpack_header():

    format_version = struct.pack('<B', FORMAT_VERSION)
    raw = MAGIC + format_version + \
        '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff' + \
        '\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'

    def mod_raw(offset, replacement):
        return raw[0:offset] + replacement + \
            raw[offset+len(replacement):]

    # check with no args
    yield nt.assert_equal, BloscPackHeader(), BloscPackHeader.decode(raw)

    for kwargs, offset, replacement in [
            # check with format_version
            ({'format_version': 23}, 4, '\x17'),
            # check with options
            ({'offsets': True}, 5, '\x01'),
            ({'metadata': True}, 5, '\x02'),
            ({'metadata': True, 'offsets': True}, 5, '\x03'),
            # check with checksum
            ({'checksum': 'adler32'}, 6, '\x01'),
            ({'checksum': 'sha384'}, 6, '\x07'),
            # check with typesize
            ({'typesize': 1}, 7, '\x01'),
            ({'typesize': 2}, 7, '\x02'),
            ({'typesize': 4}, 7, '\x04'),
            ({'typesize': 8}, 7, '\x08'),
            ({'typesize': blosc.BLOSC_MAX_TYPESIZE}, 7, '\xff'),
            # check with chunk_size
            ({'chunk_size': 1}, 
                8, '\x01\x00\x00\x00'),
            ({'chunk_size': reverse_pretty('1M')},
                8, '\x00\x00\x10\x00'),
            ({'chunk_size': blosc.BLOSC_MAX_BUFFERSIZE},
                8, '\xef\xff\xff\x7f'),
            # check with last_chunk
            ({'last_chunk': 1},
                12, '\x01\x00\x00\x00'),
            ({'last_chunk': reverse_pretty('1M')},
                12, '\x00\x00\x10\x00'),
            ({'last_chunk': blosc.BLOSC_MAX_BUFFERSIZE},
                12, '\xef\xff\xff\x7f'),
            # check with nchunks
            ({'nchunks': 1},
                16, '\x01\x00\x00\x00\x00\x00\x00\x00'),
            ({'nchunks': reverse_pretty('1M')},
                16, '\x00\x00\x10\x00\x00\x00\x00\x00'),
            ({'nchunks': MAX_CHUNKS},
                16, '\xff\xff\xff\xff\xff\xff\xff\x7f'),
            # check with max_app_chunks
            ({'nchunks': 1, 'max_app_chunks': 0},
                16, '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'),
            ({'nchunks': 1, 'max_app_chunks': 1},
                16, '\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00'),
            ({'nchunks': 1, 'max_app_chunks': reverse_pretty('1M')},
                16, '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00'),
            # Maximum value is MAX_CHUNKS - 1 since nchunks is already 1
            ({'nchunks': 1, 'max_app_chunks': MAX_CHUNKS-1},
                16, '\x01\x00\x00\x00\x00\x00\x00\x00\xfe\xff\xff\xff\xff\xff\xff\x7f'),
            ]:
        yield (nt.assert_equal,
               BloscPackHeader(**kwargs),
               BloscPackHeader.decode(mod_raw(offset, replacement)))


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
