#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


from __future__ import print_function


import blosc
import nose.tools as nt
from mock import patch
import numpy as np

from bloscpack.args import (BloscArgs,
                            BloscpackArgs,
                            MetadataArgs,
                            calculate_nchunks,
                            )
from bloscpack.compat_util import StringIO
from bloscpack.constants import (MAX_FORMAT_VERSION,
                                 BLOSCPACK_HEADER_LENGTH,
                                 BLOSC_HEADER_LENGTH,
                                 )
from bloscpack.defaults import (DEFAULT_CHUNK_SIZE,
                                )
from bloscpack.exceptions import (FormatVersionMismatch,
                                  ChecksumMismatch,
                                  )
from bloscpack.file_io import (PlainFPSource,
                               PlainFPSink,
                               CompressedFPSource,
                               CompressedFPSink,
                               pack_file,
                               unpack_file,
                               pack_bytes_file,
                               unpack_bytes_file,
                               _read_bloscpack_header,
                               _read_offsets,
                               _read_beginning,
                               _read_metadata,
                               _write_metadata,
                               )
from bloscpack.headers import (decode_blosc_header,
                               )
from bloscpack.pretty import reverse_pretty
from bloscpack.abstract_io import (pack, unpack)
from bloscpack.testutil import (create_array,
                                create_array_fp,
                                create_tmp_files,
                                cmp_fp,
                                cmp_file,
                                simple_progress,
                                )


def test_offsets():
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        pack_file(in_file, out_file, chunk_size='2M')
        with open(out_file, 'r+b') as input_fp:
            bloscpack_header = _read_bloscpack_header(input_fp)
            total_entries = bloscpack_header.total_prospective_chunks
            offsets = _read_offsets(input_fp, bloscpack_header)
            # First chunks should start after header and offsets
            first = BLOSCPACK_HEADER_LENGTH + 8 * total_entries
            # We assume that the others are correct
            nt.assert_equal(offsets[0], first)
            nt.assert_equal(736, offsets[0])
            # try to read the second header
            input_fp.seek(offsets[1], 0)
            blosc_header_raw = input_fp.read(BLOSC_HEADER_LENGTH)
            expected = {'versionlz': 1,
                        'version':   2,
                        'flags':     1,
                        'nbytes':    2097152,
                        'typesize':  8}
            blosc_header = decode_blosc_header(blosc_header_raw)
            blosc_header_slice = dict((k, blosc_header[k]) for k in expected.keys())
            nt.assert_equal(expected, blosc_header_slice)

    # now check the same thing again, but w/o any max_app_chunks
    input_fp, output_fp = StringIO(), StringIO()
    create_array_fp(1, input_fp)
    nchunks, chunk_size, last_chunk_size = \
            calculate_nchunks(input_fp.tell(), chunk_size='2M')
    input_fp.seek(0, 0)
    bloscpack_args = BloscpackArgs(max_app_chunks=0)
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
    nt.assert_equal(96, offsets[0])


def test_metadata():
    test_metadata = {'dtype': 'float64',
                     'shape': [1024],
                     'others': [],
                     }
    received_metadata = pack_unpack_fp(1, metadata=test_metadata)
    nt.assert_equal(test_metadata, received_metadata)


def test_metadata_opportunisitic_compression():
    # make up some metadata that can be compressed with benefit
    test_metadata = ("{'dtype': 'float64', 'shape': [1024], 'others': [],"
                     "'original_container': 'carray'}")
    target_fp = StringIO()
    _write_metadata(target_fp, test_metadata, MetadataArgs())
    target_fp.seek(0, 0)
    metadata, header = _read_metadata(target_fp)
    nt.assert_equal('zlib', header['meta_codec'])

    # now do the same thing, but use badly compressible metadata
    test_metadata = "abc"
    target_fp = StringIO()
    # default args say: do compression...
    _write_metadata(target_fp, test_metadata, MetadataArgs())
    target_fp.seek(0, 0)
    metadata, header = _read_metadata(target_fp)
    # but it wasn't of any use
    nt.assert_equal('None', header['meta_codec'])


def test_disable_offsets():
    in_fp, out_fp, dcmp_fp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, in_fp)
    in_fp_size = in_fp.tell()
    in_fp.seek(0)
    bloscpack_args = BloscpackArgs(offsets=False)
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
@patch('bloscpack.file_io.FORMAT_VERSION', MAX_FORMAT_VERSION)
def test_invalid_format():
    blosc_args = BloscArgs()
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        pack_file(in_file, out_file, blosc_args=blosc_args)
        nt.assert_raises(FormatVersionMismatch,
                         unpack_file, out_file, dcmp_file)


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
            replace = b'\x00' if fifth == b'\xff' else b'\xff'
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
        cmp_file(in_file, dcmp_file)


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
    unpack(source, sink)
    if progress:
        print("Verifying")
    cmp_fp(in_fp, dcmp_fp)
    return source.metadata


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


def test_pack_unpack_bytes_file():
    array_ = np.linspace(0, 1e5)
    input_bytes = array_.tostring()
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        pack_bytes_file(input_bytes, out_file)
        output_bytes = unpack_bytes_file(out_file)
    nt.assert_equal(input_bytes, output_bytes)


def pack_unpack_hard():
    """ Test on somewhat larger arrays, but be nice to memory. """
    # Array is apprx. 1.5 GB large
    # should make apprx 1536 chunks
    pack_unpack(100, chunk_size=reverse_pretty('1M'), progress=simple_progress)


def pack_unpack_extreme():
    """ Test on somewhat larer arrays, uses loads of memory. """
    # this will create a huge array, and then use the
    # blosc.BLOSC_MAX_BUFFERSIZE as chunk-szie
    pack_unpack(300, chunk_size=blosc.BLOSC_MAX_BUFFERSIZE,
                progress=simple_progress)
