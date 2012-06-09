#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function

import os.path as path
import sys
import tempfile
import time
import numpy
import bloscpack

tdir = tempfile.mkdtemp()
in_file = path.join(tdir, 'file')
out_file = path.join(tdir, 'file.blp')
dcmp_file = path.join(tdir, 'file.dcmp')
blosc_args = {'typesize': 4,
                'clevel' : 7,
                'shuffle' : True}

def get_fs(file_name):
    return bloscpack.pretty_size(path.getsize(file_name))

def get_ratio(file1, file2):
    return str(path.getsize(file1)/path.getsize(file2))

print('create the test data', end='')
array_ = numpy.linspace(0, 100, 2e7)
with open(in_file, 'w') as in_fp:
    for _ in range(10):
        in_fp.write(array_.tostring())
        print('.', end='')
        sys.stdout.flush()
print('')
del array_
print("Input file size: %s" % get_fs(in_file))
tic = time.time()
bloscpack.pack_file(in_file, out_file, blosc_args,
        chunk_size=bloscpack.reverse_pretty(bloscpack.DEFAULT_CHUNK_SIZE))
toc = time.time()
print("Time: %.2f seconds" % (toc - tic))
print("Output file size: %s" % get_fs(out_file))
print("Ratio: %s" % get_ratio(out_file, in_file))
