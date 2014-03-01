#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


from __future__ import division


import cStringIO
import os.path as path
import itertools


import blosc


from .args import (DEFAULT_BLOSCPACK_ARGS,
                   DEFAULT_BLOSC_ARGS,
                   DEFAULT_METADATA_ARGS,
                   BLOSC_ARGS,
                   calculate_nchunks,
                   _check_blosc_args,
                   _check_bloscpack_args,
                   _handle_max_apps,
                   )
from .constants import (BLOSCPACK_HEADER_LENGTH,
                        METADATA_HEADER_LENGTH,
                        )
from .checksums import (CHECKSUMS_LOOKUP,
                        )
from .defaults import (DEFAULT_CLEVEL,
                       DEFAULT_SHUFFLE,
                       DEFAULT_CNAME,
                       DEFAULT_CHUNK_SIZE,
                       )
from .exceptions import (NotEnoughSpace,
                         )
from .fileio import (_read_beginning,
                     _read_compressed_chunk_fp,
                     _write_offsets,
                     )
from .headers import (BloscPackHeader,
                      )
from .pretty import (double_pretty_size,
                     pretty_size,
                     )
import log
from .util import (open_two_file,
                   )
from .sourcensink import (PlainFPSource,
                          PlainFPSink,
                          CompressedFPSource,
                          CompressedFPSink,
                          PlainNumpySource,
                          PlainNumpySink,
                          _compress_chunk_str,
                          _write_compressed_chunk,
                          )


def pack_file(in_file, out_file, chunk_size=DEFAULT_CHUNK_SIZE, metadata=None,
              blosc_args=DEFAULT_BLOSC_ARGS,
              bloscpack_args=DEFAULT_BLOSCPACK_ARGS,
              metadata_args=DEFAULT_METADATA_ARGS):
    """ Main function for compressing a file.

    Parameters
    ----------
    in_file : str
        the name of the input file
    out_file : str
        the name of the output file
    chunk_size : int
        the desired chunk size in bytes
    metadata : dict
        the metadata dict
    blosc_args : dict
        blosc keyword args
    bloscpack_args : dict
        bloscpack keyword args
    metadata_args : dict
        metadata keyword args

    Raises
    ------

    ChunkingException
        if there was a problem caculating the chunks

    # TODO document which arguments are silently ignored

    """
    in_file_size = path.getsize(in_file)
    log.verbose('input file size: %s' % double_pretty_size(in_file_size))
    # calculate chunk sizes
    nchunks, chunk_size, last_chunk_size = \
            calculate_nchunks(in_file_size, chunk_size)
    with open_two_file(open(in_file, 'rb'), open(out_file, 'wb')) as \
            (input_fp, output_fp):
        source = PlainFPSource(input_fp)
        sink = CompressedFPSink(output_fp)
        pack(source, sink,
                nchunks, chunk_size, last_chunk_size,
                metadata=metadata,
                blosc_args=blosc_args,
                bloscpack_args=bloscpack_args,
                metadata_args=metadata_args)
    out_file_size = path.getsize(out_file)
    log.verbose('output file size: %s' % double_pretty_size(out_file_size))
    log.verbose('compression ratio: %f' % (out_file_size/in_file_size))


def pack(source, sink,
        nchunks, chunk_size, last_chunk,
        metadata=None,
        blosc_args=DEFAULT_BLOSC_ARGS,
        bloscpack_args=DEFAULT_BLOSCPACK_ARGS,
        metadata_args=DEFAULT_METADATA_ARGS):
    """ Core packing function.  """
    _check_blosc_args(blosc_args)
    log.debug('blosc args are:')
    for arg, value in blosc_args.iteritems():
        log.debug('\t%s: %s' % (arg, value))
    _check_bloscpack_args(bloscpack_args)
    log.debug('bloscpack args are:')
    for arg, value in bloscpack_args.iteritems():
        log.debug('\t%s: %s' % (arg, value))
    max_app_chunks = _handle_max_apps(bloscpack_args['offsets'],
            nchunks,
            bloscpack_args['max_app_chunks'])
    # create the bloscpack header
    bloscpack_header = BloscPackHeader(
            offsets=bloscpack_args['offsets'],
            metadata=metadata is not None,
            checksum=bloscpack_args['checksum'],
            typesize=blosc_args['typesize'],
            chunk_size=chunk_size,
            last_chunk=last_chunk,
            nchunks=nchunks,
            max_app_chunks=max_app_chunks
            )
    source.configure(chunk_size, last_chunk, nchunks)
    sink.configure(blosc_args, bloscpack_header)
    sink.write_bloscpack_header()
    # deal with metadata
    if metadata is not None:
        sink.write_metadata(metadata, metadata_args)
    elif metadata_args is not None:
        log.debug('metadata_args will be silently ignored')
    sink.init_offsets()

    compress_func = source.compress_func
    # read-compress-write loop
    for i, chunk in enumerate(source()):
        log.debug("Handle chunk '%d' %s" %
                    (i,'(last)' if i == nchunks -1 else ''))
        compressed = compress_func(chunk, blosc_args)
        sink.put(i, compressed)

    sink.finalize()


def pack_ndarray(ndarray, sink,
        chunk_size=DEFAULT_CHUNK_SIZE,
        blosc_args=DEFAULT_BLOSC_ARGS,
        bloscpack_args=DEFAULT_BLOSCPACK_ARGS,
        metadata_args=DEFAULT_METADATA_ARGS):
    """ Serialialize a Numpy array.

    Parameters
    ----------
    ndarray : ndarray
        the numpy array to serialize
    sink : CompressedSink
        the sink to serialize to
    blosc_args : dict
        the args for blosc
    bloscpack_args : dict
        the args for bloscpack
    metadata_args : dict
        the args for the metadata

    Notes
    -----

    The 'typesize' value of 'blosc_args' will be silently ignored and replaced
    with the itemsize of the Numpy array's dtype.
    """

    blosc_args = blosc_args.copy()
    blosc_args['typesize'] = ndarray.dtype.itemsize
    source = PlainNumpySource(ndarray)
    nchunks, chunk_size, last_chunk_size = \
            calculate_nchunks(source.size, chunk_size)
    pack(source, sink,
            nchunks, chunk_size, last_chunk_size,
            metadata=source.metadata,
            blosc_args=blosc_args,
            bloscpack_args=bloscpack_args,
            metadata_args=metadata_args)
    #out_file_size = path.getsize(file_pointer)
    #log.verbose('output file size: %s' % double_pretty_size(out_file_size))
    #log.verbose('compression ratio: %f' % (out_file_size/source.size))


def pack_ndarray_file(ndarray, filename,
                      chunk_size=DEFAULT_CHUNK_SIZE,
                      blosc_args=DEFAULT_BLOSC_ARGS,
                      bloscpack_args=DEFAULT_BLOSCPACK_ARGS,
                      metadata_args=DEFAULT_METADATA_ARGS):
    with open(filename, 'wb') as fp:
        sink = CompressedFPSink(fp)
        pack_ndarray(ndarray, sink,
                    chunk_size=chunk_size,
                    blosc_args=blosc_args,
                    bloscpack_args=bloscpack_args,
                    metadata_args=metadata_args)


def pack_ndarray_str(ndarray,
                      chunk_size=DEFAULT_CHUNK_SIZE,
                      blosc_args=DEFAULT_BLOSC_ARGS,
                      bloscpack_args=DEFAULT_BLOSCPACK_ARGS,
                      metadata_args=DEFAULT_METADATA_ARGS):
    sio = cStringIO.StringIO()
    sink = CompressedFPSink(sio)
    pack_ndarray(ndarray, sink,
                    chunk_size=chunk_size,
                    blosc_args=blosc_args,
                    bloscpack_args=bloscpack_args,
                    metadata_args=metadata_args)
    return sio.getvalue()


def unpack(source, sink):
    # read, decompress, write loop
    for compressed in iter(source):
        sink.put(compressed)
    return source.metadata


def unpack_file(in_file, out_file):
    """ Main function for decompressing a file.

    Parameters
    ----------
    in_file : str
        the name of the input file
    out_file : str
        the name of the output file

    Returns
    -------
    metadata : str
        the metadata contained in the file if present

    Raises
    ------

    FormatVersionMismatch
        if the file has an unmatching format version number
    ChecksumMismatch
        if any of the chunks fail to produce the correct checksum
    """
    in_file_size = path.getsize(in_file)
    log.verbose('input file size: %s' % pretty_size(in_file_size))
    with open_two_file(open(in_file, 'rb'), open(out_file, 'wb')) as \
            (input_fp, output_fp):
        source = CompressedFPSource(input_fp)
        sink = PlainFPSink(output_fp, source.nchunks)
        metadata = unpack(source, sink)
    out_file_size = path.getsize(out_file)
    log.verbose('output file size: %s' % pretty_size(out_file_size))
    log.verbose('decompression ratio: %f' % (out_file_size / in_file_size))
    return metadata


def unpack_ndarray(source):
    """ Deserialize a Numpy array.

    Parameters
    ----------
    source : CompressedSource
        the source containing the serialized Numpy array

    Returns
    -------
    ndarray : ndarray
        the Numpy array

    Raises
    ------
    NotANumpyArray
        if the source doesn't seem to contain a Numpy array
    """

    sink = PlainNumpySink(source.metadata)
    for compressed in iter(source):
        sink.put(compressed)
    return sink.ndarray


def unpack_ndarray_file(filename):
    source = CompressedFPSource(open(filename, 'rb'))
    return unpack_ndarray(source)


def unpack_ndarray_str(str_):
    sio = cStringIO.StringIO(str_)

    source = CompressedFPSource(sio)
    sink = PlainNumpySink(source.metadata)
    for compressed in iter(source):
        sink.put(compressed)
    return sink.ndarray


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
