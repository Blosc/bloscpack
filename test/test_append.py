# -*- coding: utf-8 -*-
# vim :set ft=py:


import blosc
import pytest
import numpy as np


from bloscpack.abstract_io import (pack,
                                   unpack,
                                   )
from bloscpack.append import (append,
                              append_fp,
                              _recreate_metadata,
                              _rewrite_metadata_fp,
                              )
from bloscpack.args import (BloscArgs,
                            BloscpackArgs,
                            calculate_nchunks,
                            MetadataArgs,
                            )
from bloscpack.checksums import (CHECKSUMS_LOOKUP,
                                 )
from bloscpack.compat_util import StringIO
from bloscpack.constants import (METADATA_HEADER_LENGTH,
                                 )
from bloscpack.exceptions import (NotEnoughSpace,
                                  NoSuchSerializer,
                                  NoSuchCodec,
                                  ChecksumLengthMismatch,
                                  NoChangeInMetadata,
                                  MetadataSectionTooSmall,
                                  )
from bloscpack.file_io import (PlainFPSource,
                               PlainFPSink,
                               CompressedFPSource,
                               CompressedFPSink,
                               pack_file_to_file,
                               unpack_file_from_file,
                               _read_beginning,
                               _read_compressed_chunk_fp,
                               _write_metadata,
                               _read_metadata,
                               )
from bloscpack.headers import (BloscpackHeader,
                               MetadataHeader,
                               )
from bloscpack.serializers import (SERIALIZERS,
                                   )
from bloscpack.testutil import (create_array,
                                create_array_fp,
                                create_tmp_files,
                                )


def prep_array_for_append(blosc_args=BloscArgs(),
                          bloscpack_args=BloscpackArgs()):
    orig, new, dcmp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, new)
    new_size = new.tell()
    new.seek(0)
    chunking = calculate_nchunks(new_size)
    source = PlainFPSource(new)
    sink = CompressedFPSink(orig)
    pack(source, sink, *chunking,
         blosc_args=blosc_args,
         bloscpack_args=bloscpack_args)
    orig.seek(0)
    new.seek(0)
    return orig, new, new_size, dcmp


def reset_append_fp(original_fp, new_content_fp, new_size, blosc_args=None):
    """ like ``append_fp`` but with ``seek(0)`` on the file pointers. """
    nchunks = append_fp(original_fp, new_content_fp, new_size,
                        blosc_args=blosc_args)
    original_fp.seek(0)
    new_content_fp.seek(0)
    return nchunks


def reset_read_beginning(input_fp):
    """ like ``_read_beginning`` but with ``seek(0)`` on the file pointer. """
    ans = _read_beginning(input_fp)
    input_fp.seek(0)
    return ans


def test_append_fp():
    orig, new, new_size, dcmp = prep_array_for_append()

    # check that the header and offsets are as we expected them to be
    orig_bloscpack_header, orig_offsets = reset_read_beginning(orig)[0:4:3]
    expected_orig_bloscpack_header = BloscpackHeader(
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
    expected_orig_offsets = [1440]
    assert expected_orig_bloscpack_header == orig_bloscpack_header
    assert expected_orig_offsets[0] == orig_offsets[0]

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
    expected_app_offsets = [1440]
    assert expected_app_bloscpack_header == app_bloscpack_header
    assert expected_app_offsets[0] == app_offsets[0]

    # now check by unpacking
    source = CompressedFPSource(orig)
    sink = PlainFPSink(dcmp)
    unpack(source, sink)
    dcmp.seek(0)
    new.seek(0)
    new_str = new.read()
    dcmp_str = dcmp.read()
    assert len(dcmp_str) == len(new_str * 2)
    assert dcmp_str == new_str * 2

    ## TODO
    # * check additional aspects of file integrity
    #   * offsets OK
    #   * metadata OK


def test_append():
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        create_array(1, in_file)
        pack_file_to_file(in_file, out_file)
        append(out_file, in_file)
        unpack_file_from_file(out_file, dcmp_file)
        in_content = open(in_file, 'rb').read()
        dcmp_content = open(dcmp_file, 'rb').read()
        assert len(dcmp_content) == len(in_content) * 2
        assert dcmp_content == in_content * 2


def test_append_into_last_chunk():
    # first create an array with a single chunk
    orig, new, dcmp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, new)
    new_size = new.tell()
    new.seek(0)
    chunking = calculate_nchunks(new_size, chunk_size=new_size)
    source = PlainFPSource(new)
    sink = CompressedFPSink(orig)
    pack(source, sink, *chunking)
    orig.seek(0)
    new.seek(0)
    # append a few bytes, creating a new, smaller, last_chunk
    new_content = new.read()
    new.seek(0)
    nchunks = reset_append_fp(orig, StringIO(new_content[:1023]), 1023)
    bloscpack_header = reset_read_beginning(orig)[0]
    assert nchunks == 1
    assert bloscpack_header['last_chunk'] == 1023
    # now append into that last chunk
    nchunks = reset_append_fp(orig, StringIO(new_content[:1023]), 1023)
    bloscpack_header = reset_read_beginning(orig)[0]
    assert nchunks == 0
    assert bloscpack_header['last_chunk'] == 2046

    # now check by unpacking
    source = CompressedFPSource(orig)
    sink = PlainFPSink(dcmp)
    unpack(source, sink)
    dcmp.seek(0)
    new.seek(0)
    new_str = new.read()
    dcmp_str = dcmp.read()
    assert len(dcmp_str) == len(new_str) + 2046
    assert dcmp_str == new_str + new_str[:1023] * 2


def test_append_single_chunk():
    orig, new, dcmp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, new)
    new_size = new.tell()
    new.seek(0)
    chunking = calculate_nchunks(new_size, chunk_size=new_size)
    source = PlainFPSource(new)
    sink = CompressedFPSink(orig)
    pack(source, sink, *chunking)
    orig.seek(0)
    new.seek(0)

    # append a single chunk
    reset_append_fp(orig, new, new_size)
    bloscpack_header = reset_read_beginning(orig)[0]
    assert bloscpack_header['nchunks'] == 2

    # append a large content, that amounts to two chunks
    new_content = new.read()
    new.seek(0)
    reset_append_fp(orig, StringIO(new_content * 2), new_size * 2)
    bloscpack_header = reset_read_beginning(orig)[0]
    assert bloscpack_header['nchunks'] == 4

    # append half a chunk
    reset_append_fp(orig, StringIO(new_content[:len(new_content)]), new_size//2)
    bloscpack_header = reset_read_beginning(orig)[0]
    assert bloscpack_header['nchunks'] == 5

    # append a few bytes
    reset_append_fp(orig, StringIO(new_content[:1023]), 1024)
    # make sure it is squashed into the lat chunk
    bloscpack_header = reset_read_beginning(orig)[0]
    assert bloscpack_header['nchunks'] == 5


def test_double_append():
    orig, new, new_size, dcmp = prep_array_for_append()
    reset_append_fp(orig, new, new_size)
    reset_append_fp(orig, new, new_size)
    new_str = new.read()
    source = CompressedFPSource(orig)
    sink = PlainFPSink(dcmp)
    unpack(source, sink)
    dcmp.seek(0)
    dcmp_str = dcmp.read()
    assert len(dcmp_str) == len(new_str) * 3
    assert dcmp_str == new_str * 3


def test_append_metadata():
    orig, new, dcmp = StringIO(), StringIO(), StringIO()
    create_array_fp(1, new)
    new_size = new.tell()
    new.seek(0)

    metadata = {"dtype": "float64", "shape": [1024], "others": []}
    chunking = calculate_nchunks(new_size, chunk_size=new_size)
    source = PlainFPSource(new)
    sink = CompressedFPSink(orig)
    pack(source, sink, *chunking, metadata=metadata)
    orig.seek(0)
    new.seek(0)
    reset_append_fp(orig, new, new_size)
    source = CompressedFPSource(orig)
    sink = PlainFPSink(dcmp)
    ans = unpack(source, sink)
    print(ans)
    dcmp.seek(0)
    new.seek(0)
    new_str = new.read()
    dcmp_str = dcmp.read()
    assert len(dcmp_str) == len(new_str) * 2
    assert dcmp_str == new_str * 2


def test_append_fp_no_offsets():
    bloscpack_args = BloscpackArgs(offsets=False)
    orig, new, new_size, dcmp = prep_array_for_append(bloscpack_args=bloscpack_args)
    with pytest.raises(RuntimeError):
        append_fp(orig, new, new_size)


def test_append_fp_not_enough_space():
    bloscpack_args = BloscpackArgs(max_app_chunks=0)
    orig, new, new_size, dcmp = prep_array_for_append(bloscpack_args=bloscpack_args)
    with pytest.raises(NotEnoughSpace):
        append_fp(orig, new, new_size)


def test_mixing_clevel():
    # the first set of chunks has max compression
    blosc_args = BloscArgs(clevel=9)
    orig, new, new_size, dcmp = prep_array_for_append()
    # get the original size
    orig.seek(0, 2)
    orig_size = orig.tell()
    orig.seek(0)
    # get a backup of the settings
    bloscpack_header, metadata, metadata_header, offsets = \
        reset_read_beginning(orig)
    # compressed size of the last chunk, including checksum
    last_chunk_compressed_size = orig_size - offsets[-1]

    # do append
    # use the typesize from the file and
    # make the second set of chunks have no compression
    blosc_args = BloscArgs(typesize=None, clevel=0)
    nchunks = append_fp(orig, new, new_size, blosc_args=blosc_args)

    # get the final size
    orig.seek(0, 2)
    final_size = orig.tell()
    orig.seek(0)

    # the original file minus the compressed size of the last chunk
    discounted_orig_size = orig_size - last_chunk_compressed_size
    # size of the appended data
    #  * raw new size, since we have no compression
    #  * uncompressed size of the last chunk
    #  * nchunks + 1 times the blosc and checksum overhead
    appended_size = new_size + bloscpack_header['last_chunk'] + (nchunks+1) * (16 + 4)
    # final size should be original plus appended data
    assert final_size == appended_size + discounted_orig_size

    # check by unpacking
    source = CompressedFPSource(orig)
    sink = PlainFPSink(dcmp)
    unpack(source, sink)
    dcmp.seek(0)
    new.seek(0)
    new_str = new.read()
    dcmp_str = dcmp.read()
    assert len(dcmp_str) == len(new_str * 2)
    assert dcmp_str == new_str * 2


def test_append_mix_shuffle():
    orig, new, new_size, dcmp = prep_array_for_append()
    # use the typesize from the file
    # deactivate shuffle
    # crank up the clevel to ensure compression happens, otherwise the flags
    # will be screwed later on
    blosc_args = BloscArgs(typesize=None, shuffle=False, clevel=9)

    # need to create something that will be compressible even without shuffle,
    # the linspace used in 'new' doesn't work anymore as of python-blosc 1.6.1
    to_append = np.zeros(int(2e6))
    to_append_fp = StringIO()
    to_append_fp.write(to_append.tostring())
    to_append_fp_size = to_append_fp.tell()
    to_append_fp.seek(0)

    # now do the append
    reset_append_fp(orig, to_append_fp, to_append_fp_size, blosc_args=blosc_args)

    # decompress 'orig' so that we can examine it
    source = CompressedFPSource(orig)
    sink = PlainFPSink(dcmp)
    unpack(source, sink)
    orig.seek(0)
    dcmp.seek(0)
    new.seek(0)
    new_str = new.read()
    dcmp_str = dcmp.read()

    # now sanity check the length and content of the decompressed
    assert len(dcmp_str) == len(new_str) + to_append_fp_size
    assert dcmp_str == new_str + to_append.tostring()

    # now get the first and the last chunk and check that the shuffle doesn't
    # match
    bloscpack_header, offsets = reset_read_beginning(orig)[0:4:3]
    orig.seek(offsets[0])
    checksum_impl = CHECKSUMS_LOOKUP[bloscpack_header['checksum']]
    compressed_zero,  blosc_header_zero, digest = \
        _read_compressed_chunk_fp(orig, checksum_impl)
    decompressed_zero = blosc.decompress(compressed_zero)
    orig.seek(offsets[-1])
    compressed_last,  blosc_header_last, digest = \
        _read_compressed_chunk_fp(orig, checksum_impl)
    decompressed_last = blosc.decompress(compressed_last)
    # first chunk has shuffle active
    assert blosc_header_zero['flags'] == 1
    # last chunk doesn't
    assert blosc_header_last['flags'] == 0


def test_recreate_metadata():
    old_meta_header = MetadataHeader(magic_format=b'',
                                     meta_options="00000000",
                                     meta_checksum='None',
                                     meta_codec='None',
                                     meta_level=0,
                                     meta_size=0,
                                     max_meta_size=0,
                                     meta_comp_size=0,
                                     user_codec=b'',
                                     )
    header_dict = old_meta_header
    with pytest.raises(NoSuchSerializer):
        _recreate_metadata(header_dict, '', magic_format='NOSUCHSERIALIZER')
    with pytest.raises(NoSuchCodec):
        _recreate_metadata(header_dict, '', codec='NOSUCHCODEC')
    with pytest.raises(ChecksumLengthMismatch):
        _recreate_metadata(header_dict, '', checksum='adler32')


def test_rewrite_metadata():
    test_metadata = {'dtype': 'float64',
                     'shape': [1024],
                     'others': [],
                     }
    # assemble the metadata args from the default
    metadata_args = MetadataArgs()
    # avoid checksum and codec
    metadata_args.meta_checksum = 'None'
    metadata_args.meta_codec = 'None'
    # preallocate a fixed size
    metadata_args.max_meta_size = 1000  # fixed preallocation
    target_fp = StringIO()
    # write the metadata section
    _write_metadata(target_fp, test_metadata, metadata_args)
    # check that the length is correct
    assert METADATA_HEADER_LENGTH + metadata_args.max_meta_size == \
        len(target_fp.getvalue())

    # now add stuff to the metadata
    test_metadata['container'] = 'numpy'
    test_metadata['data_origin'] = 'LHC'
    # compute the new length
    new_metadata_length = len(SERIALIZERS[0].dumps(test_metadata))
    # jam the new metadata into the StringIO
    target_fp.seek(0, 0)
    _rewrite_metadata_fp(target_fp, test_metadata,
                         codec=None, level=None)
    # now seek back, read the metadata and make sure it has been updated
    # correctly
    target_fp.seek(0, 0)
    result_metadata, result_header = _read_metadata(target_fp)
    assert test_metadata == result_metadata
    assert new_metadata_length == result_header.meta_comp_size

    # make sure that NoChangeInMetadata is raised
    target_fp.seek(0, 0)
    with pytest.raises(NoChangeInMetadata):
        _rewrite_metadata_fp(target_fp, test_metadata, codec=None, level=None)

    # make sure that ChecksumLengthMismatch is raised, needs modified metadata
    target_fp.seek(0, 0)
    test_metadata['fluxcompensator'] = 'back to the future'
    with pytest.raises(ChecksumLengthMismatch):
        _rewrite_metadata_fp(target_fp, test_metadata, codec=None, level=None,
                             checksum='sha512')

    # make sure if level is not None, this works
    target_fp.seek(0, 0)
    test_metadata['hoverboard'] = 'back to the future 2'
    _rewrite_metadata_fp(target_fp, test_metadata,
                         codec=None)

    # len of metadata when dumped to json should be around 1105
    for i in range(100):
        test_metadata[str(i)] = str(i)
    target_fp.seek(0, 0)
    with pytest.raises(MetadataSectionTooSmall):
        _rewrite_metadata_fp(target_fp, test_metadata, codec=None, level=None)
