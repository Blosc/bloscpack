#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import cStringIO


import blosc
import numpy


from .file_io import (pack,
                      CompressedFPSource,
                      CompressedFPSink,
                      )
from .args import (DEFAULT_BLOSCPACK_ARGS,
                   DEFAULT_BLOSC_ARGS,
                   DEFAULT_METADATA_ARGS,
                   calculate_nchunks,
                   )
from .defaults import (DEFAULT_CHUNK_SIZE,
                       )
from .sourcensink import (PlainSource,
                          PlainSink,
                          )
from .exceptions import (NotANumpyArray,
                         )


def _compress_chunk_ptr(chunk, blosc_args):
    ptr, size = chunk
    return blosc.compress_ptr(ptr, size, **blosc_args)


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
                         'order': 'F' if numpy.isfortran(ndarray) else 'C',
                         'container': 'numpy',
                         }
        self.size = ndarray.size * ndarray.itemsize
        self.ndarray = numpy.ascontiguousarray(ndarray)
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


class PlainNumpySink(PlainSink):

    def __init__(self, metadata):
        self.metadata = metadata
        if metadata is None or metadata['container'] != 'numpy':
            raise NotANumpyArray
        self.ndarray = numpy.empty(metadata['shape'],
                                   dtype=numpy.dtype(metadata['dtype']),
                                   order=metadata['order'])
        self.ptr = self.ndarray.__array_interface__['data'][0]

    def put(self, compressed):
        bwritten = blosc.decompress_ptr(compressed, self.ptr)
        self.ptr += bwritten


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
