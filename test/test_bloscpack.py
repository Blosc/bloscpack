#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:

from __future__ import print_function

from cStringIO import StringIO


import numpy as np
import numpy.testing as npt
import nose.tools as nt
import blosc
from nose_parameterized import parameterized
from mock import patch


from bloscpack.api import (pack,
                           pack_file,
                           unpack_file,
                           pack_ndarray,
                           unpack_ndarray,
                           pack_ndarray_str,
                           unpack_ndarray_str,
                           unpack,
                           append,
                           append_fp,
                           )
from bloscpack.args import (DEFAULT_BLOSC_ARGS,
                            DEFAULT_BLOSCPACK_ARGS,
                            DEFAULT_METADATA_ARGS,
                            calculate_nchunks,
                            )
from bloscpack.checksums import (CHECKSUMS_LOOKUP,
                                 )
from bloscpack.constants import (MAX_FORMAT_VERSION,
                                 BLOSCPACK_HEADER_LENGTH,
                                 BLOSC_HEADER_LENGTH,
                                 METADATA_HEADER_LENGTH,
                                 )
from bloscpack.defaults import (DEFAULT_CHUNK_SIZE,
                                )
from bloscpack.exceptions import (NoSuchCodec,
                                  NoSuchSerializer,
                                  NotEnoughSpace,
                                  FormatVersionMismatch,
                                  ChecksumLengthMismatch,
                                  NoChangeInMetadata,
                                  MetadataSectionTooSmall,
                                  ChecksumMismatch,
                                  )
from bloscpack.fileio import (_read_bloscpack_header,
                              _read_offsets,
                              _read_beginning,
                              _read_compressed_chunk_fp,
                              _read_metadata,
                              _write_metadata,
                              _recreate_metadata,
                              _rewrite_metadata_fp,
                              )
from bloscpack.headers import (decode_blosc_header,
                               create_metadata_header,
                               decode_metadata_header,
                               BloscPackHeader,
                               )
from bloscpack.pretty import reverse_pretty
from bloscpack.serializers import SERIALIZERS
from bloscpack.sourcensink import (PlainFPSource,
                                   PlainFPSink,
                                   CompressedFPSource,
                                   CompressedFPSink,
                                   PlainMemorySource,
                                   CompressedMemorySource,
                                   PlainMemorySink,
                                   CompressedMemorySink,
                                   )
from bloscpack.testutil import (create_array,
                                create_array_fp,
                                create_tmp_files,
                                )
from bloscpack.util import (open_two_file,
                            )


def test_offsets():
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        pack_file(in_file, out_file, chunk_size='2M')
        with open(out_file, 'r+b') as input_fp:
            bloscpack_header = _read_bloscpack_header(input_fp)
            total_entries = bloscpack_header.nchunks + \
                            bloscpack_header.max_app_chunks
            offsets = _read_offsets(input_fp, bloscpack_header)
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
    source = PlainFPSource(input_fp)
    sink = CompressedFPSink(output_fp)
    pack(source, sink,
         nchunks, chunk_size, last_chunk_size,
         bloscpack_args=bloscpack_args
         )
    output_fp.seek(0, 0)
    bloscpack_header = _read_bloscpack_header(output_fp)
    nt.assert_equal(0, bloscpack_header.max_app_chunks)
    offsets = _read_offsets(output_fp, bloscpack_header)
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


def test_recreate_metadata():
    old_meta_header = create_metadata_header(magic_format='',
        options="00000000",
        meta_checksum='None',
        meta_codec='None',
        meta_level=0,
        meta_size=0,
        max_meta_size=0,
        meta_comp_size=0,
        user_codec='',
        )
    header_dict = decode_metadata_header(old_meta_header)
    nt.assert_raises(NoSuchSerializer,
            _recreate_metadata,
            header_dict,
            '',
            magic_format='NOSUCHSERIALIZER')
    nt.assert_raises(NoSuchCodec,
            _recreate_metadata,
            header_dict,
            '',
            codec='NOSUCHCODEC')
    nt.assert_raises(ChecksumLengthMismatch,
            _recreate_metadata,
            header_dict,
            '',
            checksum='adler32')


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
    _write_metadata(target_fp, test_metadata, metadata_args)
    # check that the length is correct
    nt.assert_equal(METADATA_HEADER_LENGTH + metadata_args['max_meta_size'],
            len(target_fp.getvalue()))

    # now add stuff to the metadata
    test_metadata['container'] = 'numpy'
    test_metadata['data_origin'] = 'LHC'
    # compute the new length
    new_metadata_length = len(SERIALIZERS[0].dumps(test_metadata))
    # jam the new metadata into the cStringIO
    target_fp.seek(0, 0)
    _rewrite_metadata_fp(target_fp, test_metadata,
            codec=None, level=None)
    # now seek back, read the metadata and make sure it has been updated
    # correctly
    target_fp.seek(0, 0)
    result_metadata, result_header = _read_metadata(target_fp)
    nt.assert_equal(test_metadata, result_metadata)
    nt.assert_equal(new_metadata_length, result_header['meta_comp_size'])

    # make sure that NoChangeInMetadata is raised
    target_fp.seek(0, 0)
    nt.assert_raises(NoChangeInMetadata, _rewrite_metadata_fp,
            target_fp, test_metadata, codec=None, level=None)

    # make sure that ChecksumLengthMismatch is raised, needs modified metadata
    target_fp.seek(0, 0)
    test_metadata['fluxcompensator'] = 'back to the future'
    nt.assert_raises(ChecksumLengthMismatch, _rewrite_metadata_fp,
            target_fp, test_metadata,
            codec=None, level=None, checksum='sha512')

    # make sure if level is not None, this works
    target_fp.seek(0, 0)
    test_metadata['hoverboard'] = 'back to the future 2'
    _rewrite_metadata_fp(target_fp, test_metadata,
            codec=None)

    # len of metadata when dumped to json should be around 1105
    for i in range(100):
        test_metadata[str(i)] = str(i)
    target_fp.seek(0, 0)
    nt.assert_raises(MetadataSectionTooSmall, _rewrite_metadata_fp,
            target_fp, test_metadata, codec=None, level=None)


def test_metadata_opportunisitic_compression():
    # make up some metadata that can be compressed with benefit
    test_metadata = ("{'dtype': 'float64', 'shape': [1024], 'others': [],"
            "'original_container': 'carray'}")
    target_fp = StringIO()
    _write_metadata(target_fp, test_metadata, DEFAULT_METADATA_ARGS)
    target_fp.seek(0, 0)
    metadata, header = _read_metadata(target_fp)
    nt.assert_equal('zlib', header['meta_codec'])

    # now do the same thing, but use badly compressible metadata
    test_metadata = "abc"
    target_fp = StringIO()
    # default args say: do compression...
    _write_metadata(target_fp, test_metadata, DEFAULT_METADATA_ARGS)
    target_fp.seek(0, 0)
    metadata, header = _read_metadata(target_fp)
    # but it wasn't of any use
    nt.assert_equal('None', header['meta_codec'])


@parameterized([
        ('blosclz', 0),
        ('lz4', 1),
        ('lz4hc', 1),
        ('snappy', 2),
        ('zlib', 3),
    ])
def test_alternate_cname(cname, int_id):
    blosc_args = DEFAULT_BLOSC_ARGS.copy()
    blosc_args['cname'] = cname
    array_ = np.linspace(0, 1, 2e6)
    sink = CompressedMemorySink()
    pack_ndarray(array_, sink, blosc_args=blosc_args)
    blosc_header = decode_blosc_header(sink.chunks[0])
    nt.assert_equal(blosc_header['flags'] >> 5, int_id)


def test_disable_offsets():
    in_fp, out_fp, dcmp_fp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, in_fp)
    in_fp_size = in_fp.tell()
    in_fp.seek(0)
    bloscpack_args = DEFAULT_BLOSCPACK_ARGS.copy()
    bloscpack_args['offsets'] = False
    source = PlainFPSource(in_fp)
    sink = CompressedFPSink(out_fp)
    pack(source, sink,
            *calculate_nchunks(in_fp_size),
            bloscpack_args=bloscpack_args)
    out_fp.seek(0)
    bloscpack_header, metadata, metadata_header, offsets = \
            _read_beginning(out_fp)
    nt.assert_true(len(offsets) == 0)


# this will cause a bug if we ever reach 255 format versions
@patch('bloscpack.fileio.FORMAT_VERSION', MAX_FORMAT_VERSION)
def test_invalid_format():
    blosc_args = DEFAULT_BLOSC_ARGS
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        pack_file(in_file, out_file, blosc_args=blosc_args)
        nt.assert_raises(FormatVersionMismatch, unpack_file, out_file, dcmp_file)

def test_file_corruption():
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        pack_file(in_file, out_file)
        # now go in and modify a byte in the file
        with open(out_file, 'r+b') as input_fp:
            # read offsets and header
            _read_offsets(input_fp,
                    _read_bloscpack_header(input_fp))
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


def test_roundtrip_numpy():
    # first try with the standard StringIO
    a = np.arange(50)
    sio = StringIO()
    sink = CompressedFPSink(sio)
    pack_ndarray(a, sink)
    sio.seek(0)
    source = CompressedFPSource(sio)
    b = unpack_ndarray(source)
    npt.assert_array_equal(a, b)

    # now use ths shiny CompressedMemorySink/Source combo
    a = np.arange(50)
    sink = CompressedMemorySink()
    pack_ndarray(a, sink)
    source = CompressedMemorySource(sink)
    b = unpack_ndarray(source)
    npt.assert_array_equal(a, b)

    # and lastly try the pack_*_str
    s = pack_ndarray_str(a)
    b = unpack_ndarray_str(s)
    npt.assert_array_equal(a, b)


def test_numpy_dtypes_shapes_order():
    for dt in np.sctypes['int'] + np.sctypes['uint'] + np.sctypes['float']:
        a = np.arange(64, dtype=dt)
        roundtrip_ndarray(a)
        a = a.copy().reshape(8, 8)
        roundtrip_ndarray(a)
        a = a.copy().reshape(4, 16)
        roundtrip_ndarray(a)
        a = a.copy().reshape(4, 4, 4)
        roundtrip_ndarray(a)
        a = np.asfortranarray(a)
        nt.assert_true(np.isfortran(a))
        roundtrip_ndarray(a)

    # Fixed with string arrays
    a = np.array(['abc', 'def', 'ghi'])
    roundtrip_ndarray(a)
    # This actually get's cast to a fixed width string array
    a = np.array([(1, 'abc'), (2, 'def'), (3, 'ghi')])
    roundtrip_ndarray(a)
    # object arrays
    a = np.array([(1, 'abc'), (2, 'def'), (3, 'ghi')], dtype='object')
    roundtrip_ndarray(a)

    # record array
    x = np.array([(1, 'O', 1)],
                 dtype=np.dtype([('step', 'int32'),
                                ('symbol', '|S1'),
                                ('index', 'int32')]))
    roundtrip_ndarray(x)

    # and a nested record array
    dt = [('year', '<i4'),
          ('countries', [('c1', [('iso', 'a3'), ('value', '<f4')]),
                         ('c2', [('iso', 'a3'), ('value', '<f4')])
                         ])
          ]
    x = np.array([(2009, (('USA', 10.),
                          ('CHN', 12.))),
                  (2010, (('BRA', 10.),
                          ('ARG', 12.)))],
                 dt)
    roundtrip_ndarray(x)

    # what about endianess
    x = np.arange(10, dtype='>i8')
    roundtrip_ndarray(x)


def test_larger_arrays():
    for dt in ('uint64', 'int64', 'float64'):
        a = np.arange(2e4, dtype=dt)
        roundtrip_ndarray(a)


def huge_arrays():
    for dt in ('uint64', 'int64', 'float64'):
        # needs plenty of memory
        a = np.arange(1e8, dtype=dt)
        roundtrip_ndarray(a)


def roundtrip_ndarray(ndarray):
    sink = CompressedMemorySink()
    pack_ndarray(ndarray, sink)
    source = CompressedMemorySource(sink)
    result = unpack_ndarray(source)
    npt.assert_array_equal(ndarray, result)


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
    source = PlainFPSource(in_fp)
    sink = CompressedFPSink(out_fp)
    pack(source, sink,
            nchunks, chunk_size, last_chunk_size,
            metadata=metadata)
    out_fp.seek(0)
    if progress:
        print("Decompressing")
    source = CompressedFPSource(out_fp)
    sink = PlainFPSink(dcmp_fp)
    metadata = unpack(source, sink)
    if progress:
        print("Verifying")
    cmp_fp(in_fp, dcmp_fp)
    if metadata:
        return metadata


def pack_unpack_mem(repeats, chunk_size=DEFAULT_CHUNK_SIZE,
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
    # let us play merry go round
    source = PlainFPSource(in_fp)
    sink = CompressedMemorySink()
    pack(source, sink, nchunks, chunk_size, last_chunk_size, metadata=metadata)
    source = CompressedMemorySource(sink)
    sink = PlainMemorySink()
    received_metadata = unpack(source, sink)
    nt.assert_equal(metadata, received_metadata)
    source = PlainMemorySource(sink.chunks)
    sink = CompressedFPSink(out_fp)
    pack(source, sink, nchunks, chunk_size, last_chunk_size, metadata=metadata)
    out_fp.seek(0)
    source = CompressedFPSource(out_fp)
    sink = PlainFPSink(dcmp_fp)
    received_metadata = unpack(source, sink)
    nt.assert_equal(metadata, received_metadata)
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


def test_pack_unpack_mem():
    pack_unpack_mem(1, chunk_size=reverse_pretty('1M'))
    pack_unpack_mem(1, chunk_size=reverse_pretty('2M'))
    pack_unpack_mem(1, chunk_size=reverse_pretty('4M'))
    pack_unpack_mem(1, chunk_size=reverse_pretty('8M'))

    metadata = {"dtype": "float64", "shape": [1024], "others": []}

    pack_unpack_mem(1, chunk_size=reverse_pretty('1M'), metadata=metadata)
    pack_unpack_mem(1, chunk_size=reverse_pretty('2M'), metadata=metadata)
    pack_unpack_mem(1, chunk_size=reverse_pretty('4M'), metadata=metadata)
    pack_unpack_mem(1, chunk_size=reverse_pretty('8M'), metadata=metadata)

def pack_unpack_hard():
    """ Test on somewhat larger arrays, but be nice to memory. """
    # Array is apprx. 1.5 GB large
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
    source = PlainFPSource(new)
    sink = CompressedFPSink(orig)
    pack(source, sink, *chunking,
         blosc_args=blosc_args,
         bloscpack_args=bloscpack_args)
    orig.reset()
    new.reset()
    return orig, new, new_size, dcmp


def reset_append_fp(original_fp, new_content_fp, new_size, blosc_args=None):
    """ like ``append_fp`` but with ``reset()`` on the file pointers. """
    nchunks = append_fp(original_fp, new_content_fp, new_size,
                        blosc_args=blosc_args)
    original_fp.reset()
    new_content_fp.reset()
    return nchunks


def reset_read_beginning(input_fp):
    """ like ``_read_beginning`` but with ``reset()`` on the file pointer. """
    ans = _read_beginning(input_fp)
    input_fp.reset()
    print(ans)
    return ans


def test_append_fp():
    orig, new, new_size, dcmp = prep_array_for_append()

    # check that the header and offsets are as we expected them to be
    orig_bloscpack_header, orig_offsets = reset_read_beginning(orig)[0:4:3]
    expected_orig_bloscpack_header = BloscPackHeader(
            format_version=3,
            offsets=True,
            metadata=False,
            checksum='adler32',
            typesize=8,
            chunk_size=1048576,
            last_chunk=271360,
            nchunks=16,
            max_app_chunks=160,
            )
    expected_orig_offsets = [1440, 221122, 419302, 576717, 737614,
                             894182, 1051091, 1208872, 1364148,
                             1512476, 1661570, 1811035, 1960042,
                             2109263, 2258547, 2407759]
    nt.assert_equal(expected_orig_bloscpack_header, orig_bloscpack_header)
    nt.assert_equal(expected_orig_offsets, orig_offsets)

    # perform the append
    reset_append_fp(orig, new, new_size)

    # check that the header and offsets are as we expected them to be after
    # appending
    app_bloscpack_header, app_offsets = reset_read_beginning(orig)[0:4:3]
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
    source = CompressedFPSource(orig)
    sink = PlainFPSink(dcmp)
    unpack(source, sink)
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


def test_append_into_last_chunk():
    # first create an array with a single chunk
    orig, new, dcmp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, new)
    new_size = new.tell()
    new.reset()
    chunking = calculate_nchunks(new_size, chunk_size=new_size)
    source = PlainFPSource(new)
    sink = CompressedFPSink(orig)
    pack(source, sink, *chunking)
    orig.reset()
    new.reset()
    # append a few bytes, creating a new, smaller, last_chunk
    new_content = new.read()
    new.reset()
    nchunks = reset_append_fp(orig, StringIO(new_content[:1023]), 1023)
    bloscpack_header = reset_read_beginning(orig)[0]
    nt.assert_equal(nchunks, 1)
    nt.assert_equal(bloscpack_header['last_chunk'], 1023)
    # now append into that last chunk
    nchunks = reset_append_fp(orig, StringIO(new_content[:1023]), 1023)
    bloscpack_header = reset_read_beginning(orig)[0]
    nt.assert_equal(nchunks, 0)
    nt.assert_equal(bloscpack_header['last_chunk'], 2046)

    # now check by unpacking
    source = CompressedFPSource(orig)
    sink = PlainFPSink(dcmp)
    unpack(source, sink)
    dcmp.reset()
    new.reset()
    new_str = new.read()
    dcmp_str = dcmp.read()
    nt.assert_equal(len(dcmp_str), len(new_str) + 2046)
    nt.assert_equal(dcmp_str, new_str + new_str[:1023] * 2)


def test_append_single_chunk():
    orig, new, dcmp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, new)
    new_size = new.tell()
    new.reset()
    chunking = calculate_nchunks(new_size, chunk_size=new_size)
    source = PlainFPSource(new)
    sink = CompressedFPSink(orig)
    pack(source, sink, *chunking)
    orig.reset()
    new.reset()

    # append a single chunk
    reset_append_fp(orig, new, new_size)
    bloscpack_header = reset_read_beginning(orig)[0]
    nt.assert_equal(bloscpack_header['nchunks'], 2)

    # append a large content, that amounts to two chunks
    new_content = new.read()
    new.reset()
    reset_append_fp(orig, StringIO(new_content * 2), new_size * 2)
    bloscpack_header = reset_read_beginning(orig)[0]
    nt.assert_equal(bloscpack_header['nchunks'], 4)

    # append half a chunk
    reset_append_fp(orig, StringIO(new_content[:len(new_content)]), new_size/2)
    bloscpack_header = reset_read_beginning(orig)[0]
    nt.assert_equal(bloscpack_header['nchunks'], 5)

    # append a few bytes
    reset_append_fp(orig, StringIO(new_content[:1023]), 1024)
    # make sure it is squashed into the lat chunk
    bloscpack_header = reset_read_beginning(orig)[0]
    nt.assert_equal(bloscpack_header['nchunks'], 5)


def test_double_append():
    orig, new, new_size, dcmp = prep_array_for_append()
    reset_append_fp(orig, new, new_size)
    reset_append_fp(orig, new, new_size)
    new_str = new.read()
    source = CompressedFPSource(orig)
    sink = PlainFPSink(dcmp)
    unpack(source, sink)
    dcmp.reset()
    dcmp_str = dcmp.read()
    nt.assert_equal(len(dcmp_str), len(new_str) * 3)
    nt.assert_equal(dcmp_str, new_str * 3)


def test_append_metadata():
    orig, new, dcmp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, new)
    new_size = new.tell()
    new.reset()

    metadata = {"dtype": "float64", "shape": [1024], "others": []}
    chunking = calculate_nchunks(new_size, chunk_size=new_size)
    source = PlainFPSource(new)
    sink = CompressedFPSink(orig)
    pack(source, sink, *chunking, metadata=metadata)
    orig.reset()
    new.reset()
    reset_append_fp(orig, new, new_size)
    source = CompressedFPSource(orig)
    sink = PlainFPSink(dcmp)
    ans = unpack(source, sink)
    print(ans)
    dcmp.reset()
    new.reset()
    new_str = new.read()
    dcmp_str = dcmp.read()
    nt.assert_equal(len(dcmp_str), len(new_str) * 2)
    nt.assert_equal(dcmp_str, new_str * 2)


def test_append_fp_no_offsets():
    bloscpack_args = DEFAULT_BLOSCPACK_ARGS.copy()
    bloscpack_args['offsets'] = False
    orig, new, new_size, dcmp = prep_array_for_append(bloscpack_args=bloscpack_args)
    nt.assert_raises(RuntimeError, append_fp, orig, new, new_size)


def test_append_fp_not_enough_space():
    bloscpack_args = DEFAULT_BLOSCPACK_ARGS.copy()
    bloscpack_args['max_app_chunks'] = 0
    orig, new, new_size, dcmp = prep_array_for_append(bloscpack_args=bloscpack_args)
    nt.assert_raises(NotEnoughSpace, append_fp, orig, new, new_size)


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
            reset_read_beginning(orig)
    # compressed size of the last chunk, including checksum
    last_chunk_compressed_size = orig_size - offsets[-1]

    # do append
    blosc_args = DEFAULT_BLOSC_ARGS.copy()
    # use the typesize from the file
    blosc_args['typesize'] = None
    # make the second set of chunks have no compression
    blosc_args['clevel'] = 0
    nchunks = append_fp(orig, new, new_size, blosc_args=blosc_args)

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
    source = CompressedFPSource(orig)
    sink = PlainFPSink(dcmp)
    unpack(source, sink)
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
    reset_append_fp(orig, new, new_size, blosc_args=blosc_args)
    source = CompressedFPSource(orig)
    sink = PlainFPSink(dcmp)
    unpack(source, sink)
    orig.reset()
    dcmp.reset()
    new.reset()
    new_str = new.read()
    dcmp_str = dcmp.read()
    nt.assert_equal(len(dcmp_str), len(new_str * 2))
    nt.assert_equal(dcmp_str, new_str * 2)

    # now get the first and the last chunk and check that the shuffle doesn't
    # match
    bloscpack_header, offsets = reset_read_beginning(orig)[0:4:3]
    orig.seek(offsets[0])
    checksum_impl = CHECKSUMS_LOOKUP[bloscpack_header['checksum']]
    compressed_zero,  blosc_header_zero = \
        _read_compressed_chunk_fp(orig, checksum_impl)
    decompressed_zero = blosc.decompress(compressed_zero)
    orig.seek(offsets[-1])
    compressed_last,  blosc_header_last = \
        _read_compressed_chunk_fp(orig, checksum_impl)
    decompressed_last = blosc.decompress(compressed_last)
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
