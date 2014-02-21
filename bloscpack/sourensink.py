#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import abc
import itertools


import blosc
import numpy as np

from .constans import (BLOSCPACK_HEADER_LENGTH,
                       )
from .header import (encode_int64,
                     )
from .exceptions import (ChecksumMismatch,
                         NotANumpyArray,
                         )
from .pretty import (pretty_size,
                     )
import log


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
                                           metadata,
                                           metadata_args)

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
