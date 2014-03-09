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
from .headers import (BloscPackHeader,
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
