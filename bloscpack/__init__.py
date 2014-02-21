#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

""" Command line interface to Blosc via python-blosc """

from __future__ import division

import abc
import contextlib
import itertools
import sys


import blosc
import numpy as np

from .args import (DEFAULT_BLOSCPACK_ARGS,
                   DEFAULT_BLOSC_ARGS,
                   DEFAULT_METADATA_ARGS,
                   DEFAULT_META_CODEC,
                   DEFAULT_META_LEVEL,
                   BLOSC_ARGS,
                   METADATA_ARGS,
                   calculate_nchunks,
                   _check_blosc_args,
                   _check_bloscpack_args,
                   _check_metadata_arguments,
                   _handle_max_apps,
                   )
from .checksums import (check_valid_checksum,
                        CHECKSUMS_LOOKUP,
                        CHECKSUMS_AVAIL,
                        )
from .constants import (FORMAT_VERSION,
                        BLOSC_HEADER_LENGTH,
                        BLOSCPACK_HEADER_LENGTH,
                        METADATA_HEADER_LENGTH,
                        MAX_CLEVEL,
                        )
from .defaults import (DEFAULT_CLEVEL,
                       DEFAULT_SHUFFLE,
                       DEFAULT_CNAME,
                       DEFAULT_CHUNK_SIZE,
                       )
from .headers import (BloscPackHeader,
                      check_range,
                      encode_int64,
                      decode_int64,
                      decode_blosc_header,
                      create_metadata_header,
                      decode_metadata_header,
                      )
from .metacodecs import (CODECS_AVAIL,
                         CODECS_LOOKUP,
                         check_valid_codec,
                         )
from .pretty import (pretty_size,
                     double_pretty_size,
                     )
from .serializers import(SERIALIZERS_LOOKUP,
                         check_valid_serializer,
                         )
import log
from .version import __version__  # pragma: no cover


class FormatVersionMismatch(RuntimeError):
    pass


class ChecksumMismatch(RuntimeError):
    pass


class ChecksumLengthMismatch(RuntimeError):
    pass


class NoMetadataFound(RuntimeError):
    pass


class NoChangeInMetadata(RuntimeError):
    pass


class MetadataSectionTooSmall(RuntimeError):
    pass


class NonUniformTypesize(RuntimeError):
    pass


class NotEnoughSpace(RuntimeError):
    pass


class NotANumpyArray(RuntimeError):
    pass


@contextlib.contextmanager
def open_two_file(input_fp, output_fp):
    """ Hack for making with statement work on two files with 2.6. """
    yield input_fp, output_fp
    input_fp.close()
    output_fp.close()

PYTHON_VERSION = sys.version_info[0:3]
if sys.version_info < (2, 7, 5):  # pragma: no cover
    memoryview = lambda x: x


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


def _compress_chunk_str(chunk, blosc_args):
    return blosc.compress(chunk, **blosc_args)


def _compress_chunk_ptr(chunk, blosc_args):
    ptr, size = chunk
    return blosc.compress_ptr(ptr, size, **blosc_args)


def _write_compressed_chunk(output_fp, compressed, digest):
    output_fp.write(compressed)
    if len(digest) > 0:
        output_fp.write(digest)


class PlainSource(object):

    _metaclass__ = abc.ABCMeta

    def configure(self, chunk_size, last_chunk, nchunks):
        self.chunk_size = chunk_size
        self.last_chunk = last_chunk
        self.nchunks = nchunks

    @property
    def compress_func(self):
        return _compress_chunk_str

    def __iter__(self):
        return self()

    @abc.abstractmethod
    def __call__(self):
        pass


class CompressedSource(object):

    _metaclass__ = abc.ABCMeta

    def __iter__(self):
        return self()

    @abc.abstractmethod
    def __call__(self):
        pass

class PlainFPSource(PlainSource):

    def __init__(self, input_fp):
        self.input_fp = input_fp

    def __call__(self):
        # if nchunks == 1 the last_chunk_size is the size of the single chunk
        for num_bytes in ([self.chunk_size] * (self.nchunks - 1) +
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


class PlainMemorySource(PlainSource):

    def __init__(self, chunks):
        self.chunks = chunks

    def __call__(self):
        for c in self.chunks:
            yield c


class CompressedMemorySource(CompressedSource):

    @property
    def metadata(self):
        return self.compressed_memory_sink.metadata

    def __init__(self, compressed_memory_sink):
        self.compressed_memory_sink = compressed_memory_sink
        self.checksum_impl = compressed_memory_sink.checksum_impl
        self.checksum = compressed_memory_sink.checksum
        self.nchunks = compressed_memory_sink.nchunks

        self.chunks = compressed_memory_sink.chunks
        if self.checksum:
            self.checksums = compressed_memory_sink.checksums

    def __call__(self):
        for i in xrange(self.nchunks):
            compressed = self.chunks[i]
            if self.checksum:
                expected_digest = self.checksums[i]
                received_digest = self.checksum_impl(compressed)
                if received_digest != expected_digest:
                    raise ChecksumMismatch(
                            "Checksum mismatch detected in chunk, "
                            "expected: '%s', received: '%s'" %
                            (repr(expected_digest), repr(received_digest)))
            yield compressed


class PlainNumpySource(PlainSource):

    def __init__(self, ndarray):

        # Reagrding the dtype, quote from numpy/lib/format.py:dtype_to_descr
        #
        # The .descr attribute of a dtype object cannot be round-tripped
        # through the dtype() constructor. Simple types, like dtype('float32'),
        # have a descr which looks like a record array with one field with ''
        # as a name. The dtype() constructor interprets this as a request to
        # give a default name.  Instead, we construct descriptor that can be
        # passed to dtype().
        self.metadata = {'dtype': ndarray.dtype.descr
                         if ndarray.dtype.names is not None
                         else ndarray.dtype.str,
                         'shape': ndarray.shape,
                         'order': 'F' if np.isfortran(ndarray) else 'C',
                         'container': 'numpy',
                         }
        self.size = ndarray.size * ndarray.itemsize
        self.ndarray = np.ascontiguousarray(ndarray)
        self.ptr = ndarray.__array_interface__['data'][0]

    @property
    def compress_func(self):
        return _compress_chunk_ptr

    def __call__(self):
        self.nitems = int(self.chunk_size / self.ndarray.itemsize)
        offset = self.ptr
        for i in xrange(self.nchunks - 1):
            yield offset, self.nitems
            offset += self.chunk_size
        yield offset, int(self.last_chunk / self.ndarray.itemsize)


class PlainSink(object):

    _metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def put(self, chunk):
        pass


class CompressedSink(object):

    _metaclass__ = abc.ABCMeta

    def configure(self, blosc_args, bloscpack_header):
        self.blosc_args = blosc_args
        self.bloscpack_header = bloscpack_header
        self.checksum_impl = bloscpack_header.checksum_impl
        self.offsets = bloscpack_header.offsets

    @abc.abstractmethod
    def write_bloscpack_header(self):
        pass

    @abc.abstractmethod
    def write_metadata(self, metadata, metadata_args):
        pass

    @abc.abstractmethod
    def init_offsets(self):
        pass

    @abc.abstractmethod
    def finalize(self):
        pass

    @abc.abstractmethod
    def put(self, i, compressed):
        pass

    def do_checksum(self, compressed):
        if self.checksum_impl.size > 0:
            # compute the checksum on the compressed data
            digest = self.checksum_impl(compressed)
            log.debug('checksum (%s): %s ' %
                    (self.checksum_impl.name, repr(digest)))
        else:
            digest = ''
            log.debug('no checksum')
        return digest


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
                metadata, metadata_args)

    def init_offsets(self):
        if self.offsets:
            total_entries = self.bloscpack_header.nchunks + \
                    self.bloscpack_header.max_app_chunks
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


class PlainMemorySink(PlainSink):

    def __init__(self, nchunks=None):
        if nchunks is not None:
            self.have_chunks = True
            self.chunks = [None] * nchunks
            self.i = 0
        else:
            self.have_chunks = False
            self.chunks = []

    def put(self, chunk):
        if self.have_chunks:
            self.chunks[self.i] = chunk
            self.i += 1
        else:
            self.chunks.append(chunk)


class CompressedMemorySink(CompressedSink):

    def configure(self, blosc_args, bloscpack_header):
        self.blosc_args = blosc_args
        self.bloscpack_header = bloscpack_header
        self.checksum_impl = bloscpack_header.checksum_impl
        self.checksum = bloscpack_header.checksum
        self.nchunks = bloscpack_header.nchunks

        self.chunks = [None] * self.bloscpack_header.nchunks
        if self.checksum:
            self.checksums = [None] * self.bloscpack_header.nchunks

        self.metadata = None
        self.metadata_args = None

    def write_bloscpack_header(self):
        # no op
        pass

    def write_metadata(self, metadata, metadata_args):
        self.metadata = metadata
        self.metadata_args = metadata_args

    def init_offsets(self):
        # no op
        pass

    def put(self, i, compressed):
        self.chunks[i] = compressed
        if self.checksum:
            self.checksums[i] = self.do_checksum(compressed)


class PlainNumpySink(PlainSink):

    def __init__(self, metadata):
        self.metadata = metadata
        if metadata is None or metadata['container'] != 'numpy':
            raise NotANumpyArray
        self.ndarray = np.empty(metadata['shape'],
                dtype=np.dtype(metadata['dtype']),
                order=metadata['order'])
        self.ptr = self.ndarray.__array_interface__['data'][0]

    def put(self, compressed):
        bwritten = blosc.decompress_ptr(compressed, self.ptr)
        self.ptr += bwritten


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
        total_entries = bloscpack_header.nchunks + \
                bloscpack_header.max_app_chunks
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
            log.debug('checksum OK (%s): %s ' %
                    (checksum_impl.name, repr(received_digest)))
    return compressed, blosc_header


def _seek_to_metadata(target_fp):
    """ Given a target file pointer, seek to the metadata section.

    Parameters
    ----------

    target_fp : file like
        the target file pointer

    Returns
    -------
    metadata_position : int

    Raises
    ------
    NoMetadataFound
        if there is no metadata section in this file

    """
    bloscpack_header = _read_bloscpack_header(target_fp)
    if not bloscpack_header.metadata:
        raise NoMetadataFound("unable to seek to metadata if it does not exist")
    else:
        return target_fp.tell()


def _rewrite_metadata_fp(target_fp, new_metadata,
            magic_format=None, checksum=None,
            codec=DEFAULT_META_CODEC, level=DEFAULT_META_LEVEL):
    """ Rewrite the metadata section in a file pointer.

    Parameters
    ----------
    target_fp : file like
        the target file pointer to rewrite in
    new_metadata: dict
        the new metadata to save

    See the notes in ``_recreate_metadata`` for a description of the keyword
    arguments.

    """
    # cache the current position
    current_pos = target_fp.tell()
    # read the metadata section
    old_metadata, old_metadata_header = _read_metadata(target_fp)
    if old_metadata == new_metadata:
        raise NoChangeInMetadata(
                'you requested to update metadata, but this has not changed')
    new_metadata_args = _recreate_metadata(old_metadata_header, new_metadata,
            magic_format=magic_format, checksum=checksum,
            codec=codec, level=level)
    # seek back to where the metadata begins...
    target_fp.seek(current_pos, 0)
    # and re-write it
    _write_metadata(target_fp, new_metadata, new_metadata_args)


def _recreate_metadata(old_metadata_header, new_metadata,
            magic_format=None, checksum=None,
            codec=DEFAULT_META_CODEC, level=DEFAULT_META_LEVEL):
    """ Update the metadata section.

    Parameters
    ----------
    old_metadata_header: dict
        the header of the old metadata
    new_metadata: dict
        the new metadata to save

    See the notes below for a description of the keyword arguments.

    Returns
    -------
    new_metadata_args: dict
        the new arguments for ``_write_metadata``

    Raises
    ------
    ChecksumLengthMismatch
        if the new checksum has a different length than the old one
    NoChangeInMetadata
        if the metadata has not changed

    Notes
    -----
    This create new ``metadata_args`` based on an old metadata_header, Since
    the space has already been allocated, only certain metadata arguments can
    be overridden. The keyword arguments specify which ones these are. If a
    keyword argument value is 'None' the existing argument which is obtained
    from the header is used.  Otherwise the value from the keyword argument
    takes precedence. Due to a policy of opportunistic compression, the 'codec'
    and 'level' arguments are not 'None' by default, to ensure that previously
    uncompressed metadata, which might be favourably compressible as a result
    of the enlargement process, will actually be compressed. As for the
    'checksum' only a checksum with the same digest size can be used.

    The ``metadata_args`` returned by this function are suitable to be passed
    on to ``_write_metadata``.

    """
    # get the settings from the metadata header
    metadata_args = dict((k, old_metadata_header[k]) for k in METADATA_ARGS)
    # handle and check validity of overrides
    if magic_format is not None:
        check_valid_serializer(magic_format)
        metadata_args['magic_format'] = magic_format
    if checksum is not None:
        check_valid_checksum(checksum)
        old_impl = CHECKSUMS_LOOKUP[old_metadata_header['meta_checksum']]
        new_impl = CHECKSUMS_LOOKUP[checksum]
        if old_impl.size != new_impl.size:
            raise ChecksumLengthMismatch(
                    'checksums have a size mismatch')
        metadata_args['meta_checksum'] = checksum
    if codec is not None:
        check_valid_codec(codec)
        metadata_args['meta_codec'] = codec
    if level is not None:
        check_range('meta_level', level, 0, MAX_CLEVEL)
        metadata_args['meta_level'] = level
    return metadata_args
