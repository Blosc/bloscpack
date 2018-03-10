#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


import numpy as np
import numpy.testing as npt
import nose.tools as nt
import mock


from bloscpack.abstract_io import (pack,
                                   )
from bloscpack.args import (BloscArgs,
                            calculate_nchunks,
                            )
from bloscpack.compat_util import StringIO
from bloscpack.exceptions import (NotANumpyArray,
                                  ChunkSizeTypeSizeMismatch,
                                  ObjectNumpyArrayRejection,
                                  )
from bloscpack.file_io import (PlainFPSource,
                               CompressedFPSource,
                               CompressedFPSink,
                               )
from bloscpack.headers import (decode_blosc_header,
                               )
from bloscpack.memory_io import CompressedMemorySource, CompressedMemorySink
from bloscpack.numpy_io import (pack_ndarray,
                                unpack_ndarray,
                                pack_ndarray_str,
                                unpack_ndarray_str,
                                pack_ndarray_file,
                                unpack_ndarray_file,
                                _conv,
                                )
from bloscpack.testutil import (create_tmp_files,
                                )


def roundtrip_numpy_memory(ndarray):
    sink = CompressedMemorySink()
    pack_ndarray(ndarray, sink)
    source = CompressedMemorySource(sink)
    b = unpack_ndarray(source)
    return npt.assert_array_equal, ndarray, b


def roundtrip_numpy_str(ndarray):
    s = pack_ndarray_str(ndarray)
    b = unpack_ndarray_str(s)
    return npt.assert_array_equal, ndarray, b


def roundtrip_numpy_file_pointers(ndarray):
    sio = StringIO()
    sink = CompressedFPSink(sio)
    pack_ndarray(ndarray, sink)
    sio.seek(0)
    source = CompressedFPSource(sio)
    b = unpack_ndarray(source)
    return npt.assert_array_equal, ndarray, b


def roundtrip_numpy_file(ndarray):
    with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
        pack_ndarray_file(ndarray, out_file)
        b = unpack_ndarray_file(out_file)
        return npt.assert_array_equal, ndarray, b


def test_conv():
    test_data = (
        ([[u'a', u'f8']], [('a', 'f8')]),
        ([[u'a', u'f8', 2]], [('a', 'f8', 2)]),
        ([[u'a', [[u'b', 'f8']]]], [('a', [('b', 'f8')])]),
    )
    for input_, expected in test_data:
        received = _conv(input_)
        yield nt.assert_equal, expected, received


def test_unpack_exception():
    a = np.arange(50)
    sio = StringIO()
    a_str = a.tostring()
    source = PlainFPSource(StringIO(a_str))
    sink = CompressedFPSink(sio)
    pack(source, sink, *calculate_nchunks(len(a_str)))
    nt.assert_raises(NotANumpyArray, unpack_ndarray_str, sio.getvalue())


def roundtrip_ndarray(ndarray):
    yield roundtrip_numpy_memory(ndarray)
    yield roundtrip_numpy_str(ndarray)
    yield roundtrip_numpy_file_pointers(ndarray)
    yield roundtrip_numpy_file(ndarray)


def test_numpy_dtypes_shapes_order():

    # happy trail
    a = np.arange(50)
    for case in roundtrip_ndarray(a):
        yield case

    for dt in np.sctypes['int'] + np.sctypes['uint'] + np.sctypes['float']:
        a = np.arange(64, dtype=dt)
        for case in roundtrip_ndarray(a):
            yield case
        a = a.copy().reshape(8, 8)
        for case in roundtrip_ndarray(a):
            yield case
        a = a.copy().reshape(4, 16)
        for case in roundtrip_ndarray(a):
            yield case
        a = a.copy().reshape(4, 4, 4)
        for case in roundtrip_ndarray(a):
            yield case
        a = np.asfortranarray(a)
        nt.assert_true(np.isfortran(a))
        for case in roundtrip_ndarray(a):
            yield case

    # Fixed width string arrays
    a = np.array(['abc', 'def', 'ghi'])
    for case in roundtrip_ndarray(a):
        yield case

    # This actually get's cast to a fixed width string array
    a = np.array([(1, 'abc'), (2, 'def'), (3, 'ghi')])
    for case in roundtrip_ndarray(a):
        yield case

    ## object arrays
    #a = np.array([(1, 'abc'), (2, 'def'), (3, 'ghi')], dtype='object')
    #for case in roundtrip_ndarray(a):
    #    yield case

    # structured array
    a = np.array([('a', 1), ('b', 2)], dtype=[('a', 'S1'), ('b', 'f8')])
    for case in roundtrip_ndarray(a):
        yield case

    # record array
    a = np.array([(1, 'O', 1)],
                 dtype=np.dtype([('step', 'int32'),
                                ('symbol', '|S1'),
                                ('index', 'int32')]))
    for case in roundtrip_ndarray(a):
        yield case

    # and a nested record array
    dt = [('year', '<i4'),
          ('countries', [('c1', [('iso', 'a3'), ('value', '<f4')]),
                         ('c2', [('iso', 'a3'), ('value', '<f4')])
                         ])
          ]
    a = np.array([(2009, (('USA', 10.),
                          ('CHN', 12.))),
                  (2010, (('BRA', 10.),
                          ('ARG', 12.)))],
                 dt)
    for case in roundtrip_ndarray(a):
        yield case

    # what about endianess
    a = np.arange(10, dtype='>i8')
    for case in roundtrip_ndarray(a):
        yield case

    # empty array
    a = np.array([], dtype='f8')
    for case in roundtrip_ndarray(a):
        yield case


def test_reject_object_array():
    a = np.array([(1, 'abc'), (2, 'def'), (3, 'ghi')], dtype='object')
    nt.assert_raises(ObjectNumpyArrayRejection, roundtrip_numpy_memory, a)


def test_reject_nested_object_array():
    a = np.array([(1, 'abc'), (2, 'def'), (3, 'ghi')],
                  dtype=[('a', int), ('b', 'object')])
    nt.assert_raises(ObjectNumpyArrayRejection, roundtrip_numpy_memory, a)


def test_backwards_compat():

    def old_ndarray_meta(ndarray):
        # This DOESN'T use 'repr', see also:
        # bloscpack.numpy_io._ndarray_meta
        return {'dtype': ndarray.dtype.descr
                if ndarray.dtype.names is not None
                else ndarray.dtype.str,
                'shape': ndarray.shape,
                'order': 'F' if np.isfortran(ndarray) else 'C',
                'container': 'numpy',
                }
    test_data = [np.arange(10),
                 np.array([('a', 1), ('b', 2)],
                          dtype=[('a', 'S1'), ('b', 'f8')]),
                 ]

    with mock.patch('bloscpack.numpy_io._ndarray_meta', old_ndarray_meta):
        for a in test_data:
            # uses old version of _ndarray_meta
            c = pack_ndarray_str(a)
            # should not raise a SyntaxError
            d = unpack_ndarray_str(c)
            yield npt.assert_array_equal, a, d


def test_itemsize_chunk_size_mismatch():
    a = np.arange(10)
    nt.assert_raises(ChunkSizeTypeSizeMismatch,
                     pack_ndarray_str, a, chunk_size=7)


def test_larger_arrays():
    for dt in ('uint64', 'int64', 'float64'):
        a = np.arange(2e4, dtype=dt)
        for case in roundtrip_ndarray(a):
            yield case


def huge_arrays():
    for dt in ('uint64', 'int64', 'float64'):
        # needs plenty of memory
        a = np.arange(1e8, dtype=dt)
        for case in roundtrip_ndarray(a):
            yield case


def test_alternate_cname():
    for cname, int_id in [
            ('blosclz', 0),
            ('lz4', 1),
            ('lz4hc', 1),
            ('snappy', 2),
            ('zlib', 3),
            ('zstd', 4),
            ]:
        blosc_args = BloscArgs(cname=cname)
        array_ = np.linspace(0, 1, 2e6)
        sink = CompressedMemorySink()
        pack_ndarray(array_, sink, blosc_args=blosc_args)
        blosc_header = decode_blosc_header(sink.chunks[0])
        yield nt.assert_equal, blosc_header['flags'] >> 5, int_id


def test_typesize_is_set_correctly_with_default_blosc_args():
    a = np.array([1, 2, 3], dtype='uint8')
    sink = CompressedMemorySink()
    pack_ndarray(a, sink)
    expected_args = BloscArgs(typesize=1)
    nt.assert_equal(expected_args, sink.blosc_args)


def test_typesize_is_set_correctly_with_custom_blosc_args():
    a = np.array([1, 2, 3], dtype='uint8')
    sink = CompressedMemorySink()
    input_args = BloscArgs(clevel=9)
    pack_ndarray(a, sink, blosc_args=input_args)
    expected_args = BloscArgs(clevel=9, typesize=1)
    nt.assert_equal(expected_args, sink.blosc_args)


def test_roundtrip_slice():
    a = np.arange(100).reshape((10, 10))
    s = a[3:5, 3:5]
    for case in roundtrip_ndarray(s):
        yield case
