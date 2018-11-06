#!/usr/bin/env python
# -*- coding: utf-8 -*-


from __future__ import division
from __future__ import print_function


import os.path as path
import time


import numpy


import bloscpack.testutil as bpt
from bloscpack.sysutil import drop_caches, sync
from bloscpack import pack_file_to_file, unpack_file_from_file
from bloscpack.pretty import pretty_size


with bpt.create_tmp_files() as (tdir, in_file, out_file, dcmp_file):

    print('create the test data', end='')
    bpt.create_array(100, in_file, progress=bpt.simple_progress)
    repeats = 3
    print("%s\t%s\t\t%s\t\t%s" %
          ("chunk_size", "comp-time", "decomp-time", "ratio"))
    for chunk_size in (int(2**i) for i in numpy.arange(19, 23.5, 0.5)):
        cmp_times, dcmp_times = [], []
        for _ in range(repeats):
            drop_caches()
            tic = time.time()
            pack_file_to_file(in_file, out_file, chunk_size=chunk_size)
            sync()
            toc = time.time()
            cmp_times.append(toc-tic)
            drop_caches()
            tic = time.time()
            unpack_file_from_file(out_file, dcmp_file)
            sync()
            toc = time.time()
            dcmp_times.append(toc-tic)
        ratio = path.getsize(in_file)/path.getsize(out_file)
        print("%s\t\t%f\t\t%f\t\t%f" %
              (pretty_size(chunk_size),
               sum(cmp_times)/repeats,
               sum(dcmp_times)/repeats,
               ratio,
               )
              )
