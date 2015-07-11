#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


from nose import tools as nt


from bloscpack.abstract_io import (pack,
                                   unpack,
                                   )
from bloscpack.args import (calculate_nchunks,
                            )
from bloscpack.compat_util import StringIO
from bloscpack.defaults import (DEFAULT_CHUNK_SIZE,
                                )
from bloscpack.file_io import (PlainFPSource,
                               CompressedFPSink,
                               CompressedFPSource,
                               PlainFPSink,
                               )
from bloscpack.memory_io import (CompressedMemorySink,
                                 CompressedMemorySource,
                                 PlainMemorySink,
                                 PlainMemorySource,
                                 )
from bloscpack.pretty import (reverse_pretty,
                              )
from bloscpack.testutil import (create_array_fp,
                                cmp_fp,
                                )


def pack_unpack_mem(repeats, chunk_size=DEFAULT_CHUNK_SIZE,
                    progress=False, metadata=None):
    in_fp, out_fp, dcmp_fp = StringIO(), StringIO(), StringIO()
    if progress:
        print("Creating test array")
    create_array_fp(repeats, in_fp, progress=progress)
    in_fp_size = in_fp.tell()
    if progress:
        print("Compressing")
    in_fp.seek(0)
    nchunks, chunk_size, last_chunk_size = \
            calculate_nchunks(in_fp_size, chunk_size)
    # let us play merry go round
    source = PlainFPSource(in_fp)
    sink = CompressedMemorySink()
    pack(source, sink, nchunks, chunk_size, last_chunk_size, metadata=metadata)
    source = CompressedMemorySource(sink)
    sink = PlainMemorySink()
    unpack(source, sink)
    nt.assert_equal(metadata, source.metadata)
    source = PlainMemorySource(sink.chunks)
    sink = CompressedFPSink(out_fp)
    pack(source, sink, nchunks, chunk_size, last_chunk_size, metadata=metadata)
    out_fp.seek(0)
    source = CompressedFPSource(out_fp)
    sink = PlainFPSink(dcmp_fp)
    unpack(source, sink)
    nt.assert_equal(metadata, source.metadata)
    in_fp.seek(0)
    dcmp_fp.seek(0)
    cmp_fp(in_fp, dcmp_fp)
    return source.metadata


def test_pack_unpack_mem():
    pack_unpack_mem(1, chunk_size=reverse_pretty('1M'))
    pack_unpack_mem(1, chunk_size=reverse_pretty('2M'))
    pack_unpack_mem(1, chunk_size=reverse_pretty('4M'))
    pack_unpack_mem(1, chunk_size=reverse_pretty('8M'))

    metadata = {"dtype": "float64", "shape": [1024], "others": []}

    pack_unpack_mem(1, chunk_size=reverse_pretty('1M'), metadata=metadata)
    pack_unpack_mem(1, chunk_size=reverse_pretty('2M'), metadata=metadata)
    pack_unpack_mem(1, chunk_size=reverse_pretty('4M'), metadata=metadata)
    pack_unpack_mem(1, chunk_size=reverse_pretty('8M'), metadata=metadata)
