#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


from __future__ import division

import itertools
import os.path as path

import blosc

from .args import (DEFAULT_BLOSCPACK_ARGS,
                   DEFAULT_BLOSC_ARGS,
                   DEFAULT_METADATA_ARGS,
                   calculate_nchunks,
                   _check_metadata_arguments,
                   )
from .abstract_io import (pack,
                          unpack,
                          )
from .metacodecs import (CODECS_AVAIL,
                         CODECS_LOOKUP,
)
from .constants import (METADATA_HEADER_LENGTH,
                        BLOSCPACK_HEADER_LENGTH,
                        BLOSC_HEADER_LENGTH,
                        FORMAT_VERSION,
)
from .checksums import (CHECKSUMS_AVAIL,
                        CHECKSUMS_LOOKUP,
)
from .defaults import (DEFAULT_CHUNK_SIZE,
                       )
from .exceptions import (MetadataSectionTooSmall,
                         FormatVersionMismatch,
                         ChecksumMismatch,
)
from .headers import (create_metadata_header,
                      decode_metadata_header,
                      decode_blosc_header,
                      BloscPackHeader,
                      decode_int64,
                      encode_int64,
)
from .pretty import (double_pretty_size,
                     pretty_size,
                     )
from .serializers import (SERIALIZERS_LOOKUP,
)
from .util import (open_two_file,
                   )
from .abstract_io import (PlainSource,
                          PlainSink,
                          CompressedSource,
                          CompressedSink,
                          )
import log


def _write_metadata(output_fp, metadata, metadata_args):
    """ Write the metadata to a file pointer.

    Parameters
    ----------
    output_fp : file like
        the file pointer to write to
    metadata : dict
        the metadata to write
    metadata_args : dict
        the metadata args

    Returns
    -------
    metadata_total : int
        the total number of bytes occupied by metadata header, metadata plus
        preallocation and checksum

    Notes
    -----
    The 'output_fp' should point to the position in the file where the metadata
    should be written.

    """
    _check_metadata_arguments(metadata_args)
    metadata_total = 0
    log.debug('metadata args are:')
    for arg, value in metadata_args.iteritems():
        log.debug('\t%s: %s' % (arg, value))
    metadata_total += METADATA_HEADER_LENGTH
    serializer_impl = SERIALIZERS_LOOKUP[metadata_args['magic_format']]
    metadata = serializer_impl.dumps(metadata)
    codec = 'None'
    if metadata_args['meta_codec'] != CODECS_AVAIL[0]:
        codec_impl = CODECS_LOOKUP[metadata_args['meta_codec']]
        metadata_compressed = codec_impl.compress(metadata,
                metadata_args['meta_level'])
        meta_size = len(metadata)
        meta_comp_size = len(metadata_compressed)
        # be opportunistic, avoid compression if not beneficial
        if meta_size < meta_comp_size:
            log.debug('metadata compression requested, but it was not '
                      'beneficial, deactivating '
                      "(raw: '%s' vs. compressed: '%s') " %
                      (meta_size, meta_comp_size))
            meta_comp_size = meta_size
        else:
            codec = codec_impl.name
            metadata = metadata_compressed
    else:
        meta_size = len(metadata)
        meta_comp_size = meta_size
    log.debug("Raw %s metadata of size '%s': %s" %
              ('compressed' if metadata_args['meta_codec'] != 'None' else
               'uncompressed', meta_comp_size, repr(metadata)))
    if hasattr(metadata_args['max_meta_size'], '__call__'):
        max_meta_size = metadata_args['max_meta_size'](meta_size)
    elif isinstance(metadata_args['max_meta_size'], int):
        max_meta_size = metadata_args['max_meta_size']
    log.debug('max meta size is deemed to be: %d' %
              max_meta_size)
    if meta_comp_size > max_meta_size:
        raise MetadataSectionTooSmall(
                'metadata section is too small to contain the metadata '
                'required: %d allocated: %d' %
                (meta_comp_size, max_meta_size))
    metadata_total += meta_comp_size
    # create metadata header
    raw_metadata_header = create_metadata_header(
            magic_format=metadata_args['magic_format'],
            meta_checksum=metadata_args['meta_checksum'],
            meta_codec=codec,
            meta_level=metadata_args['meta_level'],
            meta_size=meta_size,
            max_meta_size=max_meta_size,
            meta_comp_size=meta_comp_size)
    log.debug('raw_metadata_header: %s' % repr(raw_metadata_header))
    output_fp.write(raw_metadata_header)
    output_fp.write(metadata)
    prealloc = max_meta_size - meta_comp_size
    for i in xrange(prealloc):
        output_fp.write('\x00')
    metadata_total += prealloc
    log.debug("metadata has %d preallocated empty bytes" % prealloc)
    if metadata_args['meta_checksum'] != CHECKSUMS_AVAIL[0]:
        metadata_checksum_impl = CHECKSUMS_LOOKUP[metadata_args['meta_checksum']]
        metadata_digest = metadata_checksum_impl(metadata)
        metadata_total += metadata_checksum_impl.size
        output_fp.write(metadata_digest)
        log.debug("metadata checksum (%s): %s" %
                (metadata_args['meta_checksum'], repr(metadata_digest)))
    log.debug("metadata section occupies a total of %s" %
            double_pretty_size(metadata_total))
    return metadata_total


def _read_bloscpack_header(input_fp):
    """ Read the bloscpack header.

    Parameters
    ----------
    input_fp : file like
        a file pointer to read from

    Returns
    -------
    bloscpack_header : BloscPackHeader
        the decoded bloscpack header

    Raises
    ------
    FormatVersionMismatch
        if the received format version is not equal to the one this module can
        decode

    """
    log.debug('reading bloscpack header')
    bloscpack_header_raw = input_fp.read(BLOSCPACK_HEADER_LENGTH)
    log.debug('bloscpack_header_raw: %s' %
              repr(bloscpack_header_raw))
    bloscpack_header = BloscPackHeader.decode(bloscpack_header_raw)
    log.debug("bloscpack header: %s" % repr(bloscpack_header))
    if FORMAT_VERSION != bloscpack_header.format_version:
        raise FormatVersionMismatch(
                "format version of file was not '%s' as expected, but '%d'" %
                (FORMAT_VERSION, bloscpack_header.format_version))
    return bloscpack_header


def _read_metadata(input_fp):
    """ Read the metadata and header from a file pointer.

    Parameters
    ----------
    input_fp : file like
        a file pointer to read from

    Returns
    -------
    metadata : dict
        the metadata
    metadata_header : dict
        the metadata contents as dict

    Notes
    -----
    The 'input_fp' should point to the position where the metadata starts. The
    number of bytes to read will be determined from the metadata header.

    """
    raw_metadata_header = input_fp.read(METADATA_HEADER_LENGTH)
    log.debug("raw metadata header: '%s'" % repr(raw_metadata_header))
    metadata_header = decode_metadata_header(raw_metadata_header)
    log.debug("metadata header: ")
    for arg, value in metadata_header.iteritems():
        log.debug('\t%s: %s' % (arg, value))
    metadata = input_fp.read(metadata_header['meta_comp_size'])
    prealloc = metadata_header['max_meta_size'] - metadata_header['meta_comp_size']
    input_fp.seek(prealloc, 1)
    if metadata_header['meta_checksum'] != 'None':
        metadata_checksum_impl = CHECKSUMS_LOOKUP[metadata_header['meta_checksum']]
        metadata_expected_digest = input_fp.read(metadata_checksum_impl.size)
        metadata_received_digest = metadata_checksum_impl(metadata)
        if metadata_received_digest != metadata_expected_digest:
            raise ChecksumMismatch(
                    "Checksum mismatch detected in metadata "
                    "expected: '%s', received: '%s'" %
                    (repr(metadata_expected_digest),
                        repr(metadata_received_digest)))
        else:
            log.debug('metadata checksum OK (%s): %s ' %
                    (metadata_checksum_impl.name,
                        repr(metadata_received_digest)))
    if metadata_header['meta_codec'] != 'None':
        metadata_codec_impl = CODECS_LOOKUP[metadata_header['meta_codec']]
        metadata = metadata_codec_impl.decompress(metadata)
    log.verbose("read %s metadata of size: '%s'" %
            # FIXME meta_codec?
            ('compressed' if metadata_header['meta_codec'] != 'None' else
                'uncompressed', metadata_header['meta_comp_size']))
    serializer_impl = SERIALIZERS_LOOKUP[metadata_header['magic_format']]
    metadata = serializer_impl.loads(metadata)
    return metadata, metadata_header


def _read_offsets(input_fp, bloscpack_header):
    """ Read the offsets from a file pointer.

    Parameters
    ----------
    input_fp : file like
        a file pointer to read from

    Returns
    -------
    offsets : list of int
        the offsets

    Notes
    -----
    The 'input_fp' should point to the position where the offsets start. Any
    unused offsets will not be returned.

    """
    if bloscpack_header.offsets:
        total_entries = bloscpack_header.total_prospective_chunks
        offsets_raw = input_fp.read(8 * total_entries)
        log.debug('Read raw offsets: %s' % repr(offsets_raw))
        offsets = [decode_int64(offsets_raw[j - 8:j]) for j in
                   xrange(8, bloscpack_header.nchunks * 8 + 1, 8)]
        log.debug('Offsets: %s' % offsets)
        return offsets
    else:
        return []


def _read_beginning(input_fp):
    """ Read the bloscpack_header, metadata, metadata_header and offsets.

    Parameters
    ----------
    input_fp : file like
        input file pointer

    Returns
    -------
    bloscpack_header : dict
    metadata : object
    metadata_header : dict
    offsets : list of ints

    """
    bloscpack_header = _read_bloscpack_header(input_fp)
    metadata, metadata_header = _read_metadata(input_fp) \
            if bloscpack_header.metadata\
            else (None, None)
    offsets = _read_offsets(input_fp, bloscpack_header)
    return bloscpack_header, metadata, metadata_header, offsets


def _write_offsets(output_fp, offsets):
    log.debug("Writing '%d' offsets: '%s'" %
              (len(offsets), repr(offsets)))
    # write the offsets encoded into the reserved space in the file
    encoded_offsets = "".join([encode_int64(i) for i in offsets])
    log.debug("Raw offsets: %s" % repr(encoded_offsets))
    output_fp.write(encoded_offsets)


def _read_compressed_chunk_fp(input_fp, checksum_impl):
    """ Read a compressed chunk from a file pointer.

    Parameters
    ----------
    input_fp : file like
        the file pointer to read the chunk from
    checksum_impl : Checksum
        the checksum that has been used

    Returns
    -------
    compressed : str
        the compressed data
    blosc_header : dict
        the blosc header from the chunk
    """
    # read blosc header
    blosc_header_raw = input_fp.read(BLOSC_HEADER_LENGTH)
    blosc_header = decode_blosc_header(blosc_header_raw)
    if log.LEVEL == log.DEBUG:
        log.debug('blosc_header: %s' % repr(blosc_header))
    ctbytes = blosc_header['ctbytes']
    # Seek back BLOSC_HEADER_LENGTH bytes in file relative to current
    # position. Blosc needs the header too and presumably this is
    # better than to read the whole buffer and then concatenate it...
    input_fp.seek(-BLOSC_HEADER_LENGTH, 1)
    # read chunk
    compressed = input_fp.read(ctbytes)
    if checksum_impl.size > 0:
        # do checksum
        expected_digest = input_fp.read(checksum_impl.size)
        received_digest = checksum_impl(compressed)
        if received_digest != expected_digest:
            raise ChecksumMismatch(
                    "Checksum mismatch detected in chunk, "
                    "expected: '%s', received: '%s'" %
                    (repr(expected_digest), repr(received_digest)))
        else:
            log.debug('checksum OK (%s): %s' %
                      (checksum_impl.name, repr(received_digest)))
    return compressed, blosc_header


def _write_compressed_chunk(output_fp, compressed, digest):
    output_fp.write(compressed)
    if len(digest) > 0:
        output_fp.write(digest)


class PlainFPSource(PlainSource):

    def __init__(self, input_fp):
        self.input_fp = input_fp

    def __call__(self):
        # if nchunks == 1 the last_chunk_size is the size of the single chunk
        for num_bytes in ([self.chunk_size] *
                          (self.nchunks - 1) +
                          [self.last_chunk]):
            yield self.input_fp.read(num_bytes)


class CompressedFPSource(CompressedSource):

    def __init__(self, input_fp):

        self.input_fp = input_fp
        self.bloscpack_header, self.metadata, self.metadata_header, \
                self.offsets = _read_beginning(input_fp)
        self.checksum_impl = self.bloscpack_header.checksum_impl
        self.nchunks = self.bloscpack_header.nchunks

    def __call__(self):
        for i in xrange(self.nchunks):
            compressed, header = _read_compressed_chunk_fp(self.input_fp, self.checksum_impl)
            yield compressed


class PlainFPSink(PlainSink):

    def __init__(self, output_fp, nchunks=None):
        self.output_fp = output_fp
        self.nchunks = nchunks
        self.i = 0

    def put(self, compressed):
        log.debug("decompressing chunk '%d'%s" %
                  (self.i, ' (last)' if self.nchunks is not None
                   and self.i == self.nchunks - 1 else ''))
        decompressed = blosc.decompress(compressed)
        log.debug("chunk handled, in: %s out: %s" %
                  (pretty_size(len(compressed)),
                   pretty_size(len(decompressed))))
        self.output_fp.write(decompressed)
        self.i += 1


class CompressedFPSink(CompressedSink):

    def __init__(self, output_fp):
        self.output_fp = output_fp
        self.meta_total = 0

    def write_bloscpack_header(self):
        raw_bloscpack_header = self.bloscpack_header.encode()
        self.output_fp.write(raw_bloscpack_header)

    def write_metadata(self, metadata, metadata_args):
        self.meta_total += _write_metadata(self.output_fp,
                                           metadata,
                                           metadata_args)

    def init_offsets(self):
        if self.offsets:
            total_entries = self.bloscpack_header.total_prospective_chunks
            self.offset_storage = list(itertools.repeat(-1,
                                       self.bloscpack_header.nchunks))
            self.output_fp.write(encode_int64(-1) * total_entries)

    def finalize(self):
        if self.offsets:
            self.output_fp.seek(BLOSCPACK_HEADER_LENGTH + self.meta_total, 0)
            _write_offsets(self.output_fp, self.offset_storage)

    def put(self, i, compressed):
        offset = self.output_fp.tell()
        digest = self.do_checksum(compressed)
        _write_compressed_chunk(self.output_fp, compressed, digest)
        if self.offsets:
            self.offset_storage[i] = offset
        return offset, compressed, digest


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
        unpack(source, sink)
    out_file_size = path.getsize(out_file)
    log.verbose('output file size: %s' % pretty_size(out_file_size))
    log.verbose('decompression ratio: %f' % (out_file_size / in_file_size))
    return source.metadata
