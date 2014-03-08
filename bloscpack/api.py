#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


from __future__ import division


import os.path as path
import itertools


import blosc


from .args import (BLOSC_ARGS,
                   calculate_nchunks,
                   _check_blosc_args,
                   )
from .constants import (BLOSCPACK_HEADER_LENGTH,
                        METADATA_HEADER_LENGTH,
                        )
from .checksums import (CHECKSUMS_LOOKUP,
                        )
from .defaults import (DEFAULT_CLEVEL,
                       DEFAULT_SHUFFLE,
                       DEFAULT_CNAME,
                       )
from .exceptions import (NotEnoughSpace,
                         NonUniformTypesize,
                         )
from .file_io import (PlainFPSource,
                      CompressedFPSink,
                      _read_beginning,
                      _read_compressed_chunk_fp,
                      _write_offsets,
                      _write_compressed_chunk,
                      )
from .pretty import (double_pretty_size,
                     )
import log
from .util import (open_two_file,
                   )
from .sourcensink import (_compress_chunk_str,
                          )


def append_fp(original_fp, new_content_fp, new_size, blosc_args=None):
    """ Append from a file pointer to a file pointer.

    Parameters
    ----------
    original : file_like
        the original file_pointer
    new_content : str
        the file pointer for the data to be appended
    new_size : int
        the size of the new_content
    blosc_args : dict
        the blosc_args

    Returns
    -------
    nchunks_written : int
        the total number of new chunks written to the file

    Notes
    -----
    The blosc_args argument can be supplied if different blosc arguments are
    desired.

    """
    bloscpack_header, metadata, metadata_header, offsets = \
        _read_beginning(original_fp)
    checksum_impl = bloscpack_header.checksum_impl
    if not offsets:
        raise RuntimeError(
                'Appending to a file without offsets is not yet supported')
    if blosc_args is None:
        blosc_args = dict(zip(BLOSC_ARGS, [None] * len(BLOSC_ARGS)))
    # handle blosc_args
    if blosc_args['typesize'] is None:
        if bloscpack_header.typesize == -1:
            raise NonUniformTypesize(
                    "Non uniform type size, can not append to file.")
        else:
            # use the typesize from the bloscpack header
            blosc_args['typesize'] = bloscpack_header.typesize
    if blosc_args['clevel'] is None:
        # use the default
        blosc_args['clevel'] = DEFAULT_CLEVEL
    if blosc_args['shuffle'] is None:
        blosc_args['shuffle'] = DEFAULT_SHUFFLE
    if blosc_args['cname'] is None:
        blosc_args['cname'] = DEFAULT_CNAME
    _check_blosc_args(blosc_args)
    offsets_pos = (BLOSCPACK_HEADER_LENGTH +
                  (METADATA_HEADER_LENGTH + metadata_header['max_meta_size'] +
                      CHECKSUMS_LOOKUP[metadata_header['meta_checksum']].size
                   if metadata is not None else 0))
    # seek to the final offset
    original_fp.seek(offsets[-1], 0)
    # decompress the last chunk
    compressed, blosc_header = _read_compressed_chunk_fp(original_fp, checksum_impl)
    decompressed = blosc.decompress(compressed)
    # figure out how many bytes we need to read to rebuild the last chunk
    ultimo_length = len(decompressed)
    bytes_to_read = bloscpack_header.chunk_size - ultimo_length
    if new_size <= bytes_to_read:
        # special case
        # must squeeze data into last chunk
        fill_up = new_content_fp.read(new_size)
        # seek back to the position of the original last chunk
        original_fp.seek(offsets[-1], 0)
        # write the chunk that has been filled up
        compressed = _compress_chunk_str(decompressed + fill_up, blosc_args)
        digest = checksum_impl(compressed)
        _write_compressed_chunk(original_fp, compressed, digest)
        # return 0 to indicate that no new chunks have been written
        # build the new header
        bloscpack_header.last_chunk += new_size
        # create the new header
        raw_bloscpack_header = bloscpack_header.encode()
        original_fp.seek(0)
        original_fp.write(raw_bloscpack_header)
        return 0

    # figure out what is left over
    new_new_size = new_size - bytes_to_read
    # read those bytes
    fill_up = new_content_fp.read(bytes_to_read)
    # figure out how many chunks we will need
    nchunks, chunk_size, last_chunk_size = \
            calculate_nchunks(new_new_size,
                chunk_size=bloscpack_header.chunk_size)
    # make sure that we actually have that kind of space
    if nchunks > bloscpack_header.max_app_chunks:
        raise NotEnoughSpace('not enough space')
    # seek back to the position of the original last chunk
    original_fp.seek(offsets[-1], 0)
    # write the chunk that has been filled up
    compressed = _compress_chunk_str(decompressed + fill_up, blosc_args)
    digest = checksum_impl(compressed)
    _write_compressed_chunk(original_fp, compressed, digest)
    # append to the original file, again original_fp should be adequately
    # positioned
    sink = CompressedFPSink(original_fp)
    sink.configure(blosc_args, bloscpack_header)
    # allocate new offsets
    sink.offset_storage = list(itertools.repeat(-1, nchunks))
    # read from the new input file, new_content_fp should be adequately
    # positioned
    source = PlainFPSource(new_content_fp)
    source.configure(chunk_size, last_chunk_size, nchunks)
    # read, compress, write loop
    for i, chunk in enumerate(source()):
        log.debug("Handle chunk '%d' %s" % (i,'(last)' if i == nchunks -1
            else ''))

        compressed = _compress_chunk_str(chunk, blosc_args)
        sink.put(i, compressed)

    # build the new header
    bloscpack_header.last_chunk = last_chunk_size
    bloscpack_header.nchunks += nchunks
    bloscpack_header.max_app_chunks -= nchunks
    # create the new header
    raw_bloscpack_header = bloscpack_header.encode()
    original_fp.seek(0)
    original_fp.write(raw_bloscpack_header)
    # write the new offsets, but only those that changed
    original_fp.seek(offsets_pos)
    # FIXME: write only those that changed
    _write_offsets(sink.output_fp, offsets + sink.offset_storage)
    return nchunks


def append(orig_file, new_file, blosc_args=None):
    """ Append from a file pointer to a file pointer.

    Parameters
    ----------
    orig_file : str
        the name of the file to append to
    new_file : str
        the name of the file to append from
    blosc_args : dict
        the blosc_args

    Notes
    -----
    The blosc_args argument can be supplied if different blosc arguments are
    desired.
    """
    orig_size_before = path.getsize(orig_file)
    new_size = path.getsize(new_file)
    log.verbose('orig file size before append: %s' %
            double_pretty_size(orig_size_before))
    log.verbose('new file size: %s' % double_pretty_size(new_size))

    with open_two_file(open(orig_file, 'r+b'), open(new_file, 'rb')) as \
            (orig_fp, new_fp):
        append_fp(orig_fp, new_fp, new_size, blosc_args)
    orig_size_after = path.getsize(orig_file)
    log.verbose('orig file size after append: %s' %
            double_pretty_size(orig_size_after))
    log.verbose('Approximate compression ratio of appended data: %f' %
            ((orig_size_after-orig_size_before)/new_size))
