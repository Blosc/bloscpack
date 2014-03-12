#!/usr/bin/env python
# -*- coding: utf-8 -*-


from __future__ import division


import os.path as path
import time


import numpy


import bloscpack.testutil as bpt
from bloscpack.args import DEFAULT_BLOSC_ARGS
from bloscpack import pack_file, unpack_file

blosc_args = DEFAULT_BLOSC_ARGS

with bpt.create_tmp_files() as (tdir, in_file, out_file, dcmp_file):
    bpt.create_array(100, in_file)
    repeats = 3
    print ("%s\t\t%s\t\t%s\t\t%s" %
           ("chunk_size", "comp-time", "decomp-time", "ratio"))
    for chunk_size in (int(2**i) for i in numpy.arange(19, 23.5, 0.5)):
        cmp_times, dcmp_times = [], []
        for _ in range(repeats):
            tic = time.time()
            pack_file(in_file, out_file, chunk_size=chunk_size,
                      blosc_args=blosc_args)
            toc = time.time()
            cmp_times.append(toc-tic)
            tic = time.time()
            unpack_file(out_file, dcmp_file)
            toc = time.time()
            dcmp_times.append(toc-tic)
        ratio = path.getsize(out_file)/path.getsize(in_file)
        print ("%d\t\t%f\t\t%f\t\t%f" %
               (chunk_size,
                sum(cmp_times)/repeats,
                sum(dcmp_times)/repeats,
                ratio,
                )
               )
