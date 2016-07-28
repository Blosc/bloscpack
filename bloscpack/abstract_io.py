#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import abc


import blosc


from .args import (BloscArgs,
                   BloscpackArgs,
                   MetadataArgs,
                   _handle_max_apps
                   )
from .headers import (BloscpackHeader,
                      )
from .exceptions import (ChecksumMismatch,
                         ChunkSizeTypeSizeMismatch,
                         )
from .pretty import (double_pretty_size,
                     )
from . import log


def _compress_chunk_str(chunk, blosc_args):
    return blosc.compress(chunk, **blosc_args)


class PlainSource(object):

    _metaclass__ = abc.ABCMeta

    def configure(self, chunk_size, last_chunk, nchunks):
        self.chunk_size = chunk_size
        self.last_chunk = last_chunk
        self.nchunks = nchunks

    @property
    def compress_func(self):
        return _compress_chunk_str

    @abc.abstractmethod
    def __iter__(self):
        pass


class CompressedSource(object):

    _metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __iter__(self):
        pass


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
            log.debug('checksum (%s): %s' %
                     (self.checksum_impl.name, repr(digest)))
        else:
            digest = ''
            log.debug('no checksum')
        return digest


def pack(source, sink,
         nchunks, chunk_size, last_chunk,
         metadata=None,
         blosc_args=None,
         bloscpack_args=None,
         metadata_args=None):
    """ Core packing function.  """

    if not isinstance(source, PlainSource):
        raise TypeError
    if not isinstance(sink, CompressedSink):
        raise TypeError

    blosc_args = blosc_args or BloscArgs()
    log.debug(blosc_args.pformat())
    if chunk_size % blosc_args.typesize != 0:
        # Make the chunk_size divisible by typesize
        chunksize = chunk_size - (chunk_size % blosc_args.typesize)
        if chunksize == 0:
            raise ChunkSizeTypeSizeMismatch(
                "chunk_size: '%s' is less than typesize: '%i'" %
                (double_pretty_size(chunk_size), blosc_args.typesize)
            )
        chunk_size = chunksize
    bloscpack_args = bloscpack_args or BloscpackArgs()
    log.debug(bloscpack_args.pformat())
    if metadata is not None:
        metadata_args = metadata_args or MetadataArgs()
        log.debug(metadata_args.pformat())
    elif metadata_args is not None:
        log.debug('metadata_args will be silently ignored')

    max_app_chunks = _handle_max_apps(bloscpack_args.offsets,
            nchunks,
            bloscpack_args.max_app_chunks)
    # create the bloscpack header
    bloscpack_header = BloscpackHeader(
            offsets=bloscpack_args.offsets,
            metadata=metadata is not None,
            checksum=bloscpack_args.checksum,
            typesize=blosc_args.typesize,
            chunk_size=chunk_size,
            last_chunk=last_chunk,
            nchunks=nchunks,
            max_app_chunks=max_app_chunks
            )
    log.debug(bloscpack_header.pformat())

    source.configure(chunk_size, last_chunk, nchunks)
    sink.configure(blosc_args, bloscpack_header)
    sink.write_bloscpack_header()
    if metadata is not None:
        sink.write_metadata(metadata, metadata_args)
    sink.init_offsets()

    compress_func = source.compress_func
    # read-compress-write loop
    for i, chunk in enumerate(source):
        if log.LEVEL == log.DEBUG:
            log.debug("Handle chunk '%d'%s" %
                    (i, ' (last)' if i == nchunks - 1 else ''))
        compressed = compress_func(chunk, blosc_args)
        sink.put(i, compressed)
        if log.LEVEL == log.DEBUG:
            log.debug("chunk handled, in: %s out: %s" %
                    (double_pretty_size(len(chunk)),
                    double_pretty_size(len(compressed))))
    sink.finalize()


def unpack(source, sink):
    if not isinstance(source, CompressedSource):
        raise TypeError
    if not isinstance(sink, PlainSink):
        raise TypeError
    # read, decompress, write loop
    for i, (compressed, digest) in enumerate(source):
        if log.LEVEL == log.DEBUG:
            log.debug("decompressing chunk '%d'%s" %
                    (i, ' (last)' if source.nchunks is not None
                    and i == source.nchunks - 1 else ''))
        if digest:
            computed_digest = source.checksum_impl(compressed)
            if digest != computed_digest:
                raise ChecksumMismatch(
                        "Checksum mismatch detected in chunk, "
                        "expected: '%s', received: '%s'" %
                        (repr(digest), repr(computed_digest)))
            else:
                if log.LEVEL == log.DEBUG:
                    log.debug('checksum OK (%s): %s' %
                            (source.checksum_impl.name, repr(digest)))

        len_decompressed = sink.put(compressed)
        if log.LEVEL == log.DEBUG:
            log.debug("chunk handled, in: %s out: %s" %
                    (double_pretty_size(len(compressed)),
                    double_pretty_size(len_decompressed)))
