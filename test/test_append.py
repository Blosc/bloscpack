#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


from cStringIO import StringIO


import blosc
import nose.tools as nt
from bloscpack.abstract_io import pack, unpack

from bloscpack.append import (append,
                              append_fp,
                              )
from bloscpack.args import (DEFAULT_BLOSC_ARGS,
                            DEFAULT_BLOSCPACK_ARGS,
                            calculate_nchunks,
                            )
from bloscpack.checksums import (CHECKSUMS_LOOKUP,
                                 )
from bloscpack.exceptions import (NotEnoughSpace,
                                  )
from bloscpack.file_io import (PlainFPSource,
                               PlainFPSink,
                               CompressedFPSource,
                               CompressedFPSink,
                               pack_file,
                               unpack_file,
                               _read_beginning,
                               _read_compressed_chunk_fp,
                               )
from bloscpack.headers import (BloscPackHeader,
                               )
from bloscpack.testutil import (create_array,
                                create_array_fp,
                                create_tmp_files,
                                )


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
