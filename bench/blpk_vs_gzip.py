#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function

import os.path as path
import sys
import tempfile
import time
import subprocess
import numpy
import bloscpack
import test_bloscpack as tb

tdir = tempfile.mkdtemp()
in_file = path.join(tdir, 'file')
out_file = path.join(tdir, 'file.blp')
gz_out_file = path.join(tdir, 'file.gz')
dcmp_file = path.join(tdir, 'file.dcmp')
blosc_args = {'typesize': 4,
                'clevel' : 7,
                'shuffle' : True}

def get_fs(file_name):
    return bloscpack.pretty_size(path.getsize(file_name))

def get_ratio(file1, file2):
    return path.getsize(file1)/path.getsize(file2)

print('create the test data', end='')
def progress(i):
    if i % 10 == 0:
        print('.', end='')
    sys.stdout.flush()
tb.create_array(100, in_file, progress=progress)
print('')

print("Input file size: %s" % get_fs(in_file))

print("Will now run bloscpack... ")
tic = time.time()
bloscpack.pack_file(in_file, out_file, blosc_args,
        chunk_size=bloscpack.reverse_pretty(bloscpack.DEFAULT_CHUNK_SIZE))
toc = time.time()
print("Time: %.2f seconds" % (toc - tic))
print("Output file size: %s" % get_fs(out_file))
print("Ratio: %.2f" % get_ratio(out_file, in_file))

print("Will now run gzip... ")
tic = time.time()
subprocess.call('gzip -c %s > %s' % (in_file, gz_out_file), shell=True)
toc = time.time()
print("Time: %.2f seconds" % (toc - tic))
print("Output file size: %s" % get_fs(gz_out_file))
print("Ratio: %.2f" % get_ratio(gz_out_file, in_file))
