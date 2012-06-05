#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path as path
import tempfile
import time
import numpy
import bloscpack

tdir = tempfile.mkdtemp()
blosc_args = {'typesize': 4,
                'clevel' : 7,
                'shuffle' : True}
in_file = path.join(tdir, 'file')
out_file = path.join(tdir, 'file.blp')
array_ = numpy.linspace(0, 100, 2e8)
with open(in_file, 'wb') as in_fp:
    in_fp.write(array_.tostring())
del array_

repeats = 3
print "%s\t\t\t%s" % ("chunk_size", "comp-time")
for chunk_size in (int(2**i) for i in numpy.arange(12, 32, 0.5)):
    times = []
    for _ in range(repeats):
        tic = time.time()
        bloscpack.pack_file(in_file, out_file, blosc_args, chunk_size=chunk_size)
        toc = time.time()
        times.append(toc-tic)
    print "%d\t\t\t%f" % (chunk_size, sum(times)/repeats)
