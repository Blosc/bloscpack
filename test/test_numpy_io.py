#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


from cStringIO import StringIO
from unittest import TestCase


import numpy as np
import numpy.testing as npt
import nose.tools as nt


from bloscpack.abstract_io import (pack,
                                   )
from bloscpack.args import (BloscArgs,
                            calculate_nchunks,
                            )
from bloscpack.exceptions import (NotANumpyArray,
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
                                )
from bloscpack.testutil import (create_tmp_files,
                                )


class RoundTripNumpy(TestCase):

    def setUp(self):
        self.a = np.arange(50)

    def test_roundtrip_numpy_file_pointers(self):
        sio = StringIO()
        sink = CompressedFPSink(sio)
        pack_ndarray(self.a, sink)
        sio.seek(0)
        source = CompressedFPSource(sio)
        b = unpack_ndarray(source)
        npt.assert_array_equal(self.a, b)

    def test_roundtrip_numpy_memory(self):
        sink = CompressedMemorySink()
        pack_ndarray(self.a, sink)
        source = CompressedMemorySource(sink)
        b = unpack_ndarray(source)
        npt.assert_array_equal(self.a, b)

    def test_roundtrip_numpy_str(self):
        s = pack_ndarray_str(self.a)
        b = unpack_ndarray_str(s)
        npt.assert_array_equal(self.a, b)

    def test_roundtrip_numpy_file(self):
        with create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
            pack_ndarray_file(self.a, out_file)
            b = unpack_ndarray_file(out_file)
            npt.assert_array_equal(self.a, b)


def test_unpack_exception():
    a = np.arange(50)
    sio = StringIO()
    a_str = a.tostring()
    source = PlainFPSource(StringIO(a_str))
    sink = CompressedFPSink(sio)
    pack(source, sink, *calculate_nchunks(len(a_str)))
    nt.assert_raises(NotANumpyArray, unpack_ndarray_str, sio.getvalue())


def roundtrip_ndarray(ndarray):
    sink = CompressedMemorySink()
    pack_ndarray(ndarray, sink)
    source = CompressedMemorySource(sink)
    result = unpack_ndarray(source)
    npt.assert_array_equal(ndarray, result)


def test_numpy_dtypes_shapes_order():
    for dt in np.sctypes['int'] + np.sctypes['uint'] + np.sctypes['float']:
        a = np.arange(64, dtype=dt)
        roundtrip_ndarray(a)
        a = a.copy().reshape(8, 8)
        roundtrip_ndarray(a)
        a = a.copy().reshape(4, 16)
        roundtrip_ndarray(a)
        a = a.copy().reshape(4, 4, 4)
        roundtrip_ndarray(a)
        a = np.asfortranarray(a)
        nt.assert_true(np.isfortran(a))
        roundtrip_ndarray(a)

    # Fixed with string arrays
    a = np.array(['abc', 'def', 'ghi'])
    roundtrip_ndarray(a)
    # This actually get's cast to a fixed width string array
    a = np.array([(1, 'abc'), (2, 'def'), (3, 'ghi')])
    roundtrip_ndarray(a)
    # object arrays
    a = np.array([(1, 'abc'), (2, 'def'), (3, 'ghi')], dtype='object')
    roundtrip_ndarray(a)

    # record array
    x = np.array([(1, 'O', 1)],
                 dtype=np.dtype([('step', 'int32'),
                                ('symbol', '|S1'),
                                ('index', 'int32')]))
    roundtrip_ndarray(x)

    # and a nested record array
    dt = [('year', '<i4'),
          ('countries', [('c1', [('iso', 'a3'), ('value', '<f4')]),
                         ('c2', [('iso', 'a3'), ('value', '<f4')])
                         ])
          ]
    x = np.array([(2009, (('USA', 10.),
                          ('CHN', 12.))),
                  (2010, (('BRA', 10.),
                          ('ARG', 12.)))],
                 dt)
    roundtrip_ndarray(x)

    # what about endianess
    x = np.arange(10, dtype='>i8')
    roundtrip_ndarray(x)


def test_larger_arrays():
    for dt in ('uint64', 'int64', 'float64'):
        a = np.arange(2e4, dtype=dt)
        roundtrip_ndarray(a)


def huge_arrays():
    for dt in ('uint64', 'int64', 'float64'):
        # needs plenty of memory
        a = np.arange(1e8, dtype=dt)
        roundtrip_ndarray(a)


def test_alternate_cname():
    for cname, int_id in [
            ('blosclz', 0),
            ('lz4', 1),
            ('lz4hc', 1),
            ('snappy', 2),
            ('zlib', 3),
            ]:
        blosc_args = BloscArgs(cname=cname)
        array_ = np.linspace(0, 1, 2e6)
        sink = CompressedMemorySink()
        pack_ndarray(array_, sink, blosc_args=blosc_args)
        blosc_header = decode_blosc_header(sink.chunks[0])
        yield nt.assert_equal, blosc_header['flags'] >> 5, int_id
