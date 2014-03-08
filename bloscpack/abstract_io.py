#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import abc


import blosc


from .args import (DEFAULT_BLOSC_ARGS,
                   DEFAULT_BLOSCPACK_ARGS,
                   DEFAULT_METADATA_ARGS,
                   _check_blosc_args,
                   _check_bloscpack_args,
                   _handle_max_apps
                   )
from .headers import (BloscPackHeader
                      )
from .exceptions import (ChecksumMismatch,
                         )
import log


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
                  (i, '(last)' if i == nchunks - 1 else ''))
        compressed = compress_func(chunk, blosc_args)
        sink.put(i, compressed)

    sink.finalize()


def unpack(source, sink):
    # read, decompress, write loop
    for compressed in iter(source):
        sink.put(compressed)
    return source.metadata
