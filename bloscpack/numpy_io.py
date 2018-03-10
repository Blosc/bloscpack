#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import ast

import blosc
import numpy
import six
from six.moves import xrange


from .abstract_io import (pack,
                          unpack,
                          )
from .compat_util import StringIO
from .file_io import (CompressedFPSource,
                      CompressedFPSink,
                      )
from .args import (BloscArgs,
                   calculate_nchunks,
                   )
from .defaults import (DEFAULT_CHUNK_SIZE,
                       )
from .abstract_io import (PlainSource,
                          PlainSink,
                          )
from .exceptions import (NotANumpyArray,
                         ObjectNumpyArrayRejection,
                         )
from . import log


def _compress_chunk_ptr(chunk, blosc_args):
    ptr, size = chunk
    return blosc.compress_ptr(ptr, size, **blosc_args)


def _ndarray_meta(ndarray):
    # Reagrding the dtype, quote from numpy/lib/format.py:dtype_to_descr
    #
    # The .descr attribute of a dtype object cannot be round-tripped
    # through the dtype() constructor. Simple types, like dtype('float32'),
    # have a descr which looks like a record array with one field with ''
    # as a name. The dtype() constructor interprets this as a request to
    # give a default name.  Instead, we construct descriptor that can be
    # passed to dtype().
    return {'dtype': repr(ndarray.dtype.descr)
            if ndarray.dtype.names is not None
            else repr(ndarray.dtype.str),
            'shape': ndarray.shape,
            'order': 'F' if numpy.isfortran(ndarray) else 'C',
            'container': 'numpy',
            }


class PlainNumpySource(PlainSource):

    def __init__(self, ndarray):

        self.metadata = _ndarray_meta(ndarray)
        self.size = ndarray.size * ndarray.itemsize
        # The following is guesswork
        # non contiguous fortran array (if ever such a thing exists)
        if numpy.isfortran(ndarray) and not ndarray.flags['F_CONTIGUOUS']:
            self.ndarray = numpy.asfortranarray(ndarray)
        # non contiguous C array
        elif not numpy.isfortran(ndarray) and not ndarray.flags['C_CONTIGUOUS']:
            self.ndarray = numpy.ascontiguousarray(ndarray)
        # contiguous fortran or C array, do nothing
        else:
            self.ndarray = ndarray
        self.ptr = self.ndarray.__array_interface__['data'][0]

    @property
    def compress_func(self):
        return _compress_chunk_ptr

    def __iter__(self):
        self.nitems = int(self.chunk_size / self.ndarray.itemsize)
        offset = self.ptr
        for i in xrange(self.nchunks - 1):
            yield offset, self.nitems
            offset += self.chunk_size
        yield offset, int(self.last_chunk / self.ndarray.itemsize)


def _conv(descr):
    """ Converts nested list of lists into list of tuples.

    Needed for backwards compatability, see below.

    Examples
    --------
     [[u'a', u'f8']] -> [('a', 'f8')]
     [[u'a', u'f8', 2]] -> [('a', 'f8', 2)]
     [[u'a', [[u'b', 'f8']]]] -> [('a', [('b', 'f8')])]

    """
    if isinstance(descr, list):
        if isinstance(descr[0], list):
            descr = [_conv(d) for d in descr]
        else:
            descr = tuple([_conv(d) for d in descr])
    elif six.PY2 and isinstance(descr, unicode):  # pragma: no cover
        descr = str(descr)
    return descr


class PlainNumpySink(PlainSink):

    def __init__(self, metadata):
        self.metadata = metadata
        if metadata is None or metadata['container'] != 'numpy':
            raise NotANumpyArray
        # The try except is a backwards compatability hack for the old way of
        # serializing ndarray dtype which was used prior to 0.7.2. For basic
        # dtyepes, the dtype 'descr' was serialized directly to json and not
        # via 'repr'.  As such, it does not need to be evaluated, but instead
        # is already a string that can be passed to the constructor. It will
        # raise a SyntaxError in this case. For nested dtypes we have the
        # problem, that it did compress the files but was unable to decompress
        # them. In this case, it will raise a TypeError and the _conv function
        # above is used to convert the dtype accordingly.
        try:
            dtype_ = ast.literal_eval(metadata['dtype'])
        except (ValueError, SyntaxError):
            dtype_ = _conv(metadata['dtype'])
        self.ndarray = numpy.empty(metadata['shape'],
                                   dtype=numpy.dtype(dtype_),
                                   order=metadata['order'])
        self.ptr = self.ndarray.__array_interface__['data'][0]

    def put(self, compressed):
        bwritten = blosc.decompress_ptr(compressed, self.ptr)
        self.ptr += bwritten
        return bwritten


def pack_ndarray(ndarray, sink,
                 chunk_size=DEFAULT_CHUNK_SIZE,
                 blosc_args=None,
                 bloscpack_args=None,
                 metadata_args=None):
    """ Serialialize a Numpy array.

    Parameters
    ----------
    ndarray : ndarray
        the numpy array to serialize
    sink : CompressedSink
        the sink to serialize to
    blosc_args : BloscArgs
        blosc args
    bloscpack_args : BloscpackArgs
        bloscpack args
    metadata_args : MetadataArgs
        the args for the metadata

    Notes
    -----

    The 'typesize' value of 'blosc_args' will be silently ignored and replaced
    with the itemsize of the Numpy array's dtype.
    """
    if ndarray.dtype.hasobject:
        raise ObjectNumpyArrayRejection
    if blosc_args is None:
        blosc_args = BloscArgs(typesize=ndarray.dtype.itemsize)
    else:
        log.debug("Ignoring 'typesize' in blosc_args")
        blosc_args.typesize = ndarray.dtype.itemsize
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
                      blosc_args=None,
                      bloscpack_args=None,
                      metadata_args=None):
    with open(filename, 'wb') as fp:
        sink = CompressedFPSink(fp)
        pack_ndarray(ndarray, sink,
                     chunk_size=chunk_size,
                     blosc_args=blosc_args,
                     bloscpack_args=bloscpack_args,
                     metadata_args=metadata_args)


def pack_ndarray_str(ndarray,
                     chunk_size=DEFAULT_CHUNK_SIZE,
                     blosc_args=None,
                     bloscpack_args=None,
                     metadata_args=None):
    sio = StringIO()
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
    unpack(source, sink)
    return sink.ndarray


def unpack_ndarray_file(filename):
    source = CompressedFPSource(open(filename, 'rb'))
    return unpack_ndarray(source)


def unpack_ndarray_str(str_):
    sio = StringIO(str_)
    source = CompressedFPSource(sio)
    return unpack_ndarray(source)
