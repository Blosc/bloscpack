#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


from __future__ import print_function


from cStringIO import StringIO


import blosc
import nose.tools as nt
from mock import patch


from bloscpack.args import (DEFAULT_BLOSC_ARGS,
                            DEFAULT_BLOSCPACK_ARGS,
                            DEFAULT_METADATA_ARGS,
                            calculate_nchunks,
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
                                  FormatVersionMismatch,
                                  ChecksumLengthMismatch,
                                  NoChangeInMetadata,
                                  MetadataSectionTooSmall,
                                  ChecksumMismatch,
                                  )
from bloscpack.file_io import (PlainFPSource,
                               PlainFPSink,
                               CompressedFPSource,
                               CompressedFPSink,
                               pack_file,
                               unpack_file,
                               _read_bloscpack_header,
                               _read_offsets,
                               _read_beginning,
                               _read_metadata,
                               _write_metadata,
                               _recreate_metadata,
                               _rewrite_metadata_fp,
                               )
from bloscpack.headers import (decode_blosc_header,
                               create_metadata_header,
                               decode_metadata_header,
                               )
from bloscpack.memory_io import (PlainMemorySource,
                                 CompressedMemorySource,
                                 PlainMemorySink,
                                 CompressedMemorySink
                                 )
from bloscpack.pretty import reverse_pretty
from bloscpack.serializers import SERIALIZERS
from bloscpack.abstract_io import (pack, unpack)
from bloscpack.testutil import (create_array,
                                create_array_fp,
                                create_tmp_files,
                                cmp_fp,
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
@patch('bloscpack.file_io.FORMAT_VERSION', MAX_FORMAT_VERSION)
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
    unpack(source, sink)
    if progress:
        print("Verifying")
    cmp_fp(in_fp, dcmp_fp)
    return source.metadata


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
    unpack(source, sink)
    nt.assert_equal(metadata, source.metadata)
    source = PlainMemorySource(sink.chunks)
    sink = CompressedFPSink(out_fp)
    pack(source, sink, nchunks, chunk_size, last_chunk_size, metadata=metadata)
    out_fp.seek(0)
    source = CompressedFPSource(out_fp)
    sink = PlainFPSink(dcmp_fp)
    unpack(source, sink)
    nt.assert_equal(metadata, source.metadata)
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
