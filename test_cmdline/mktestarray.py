#!/usr/bin/env python

""" Utility for generating test data from cmdline interface tests.

Example usage from cram file:

  $ PYTHONPATH=$TESTDIR/../ ./$TESTDIR/mktestarray.py
  $ ls
  data.dat
  meta.json
"""


import json
import os.path as path

from bloscpack.numpy_io import _ndarray_meta
import numpy

DATA_FILE = 'data.dat'
META_FILE = 'meta.json'


def exists(filename):
    return path.isfile(filename)

if not exists(DATA_FILE) and not exists(META_FILE):
    a = numpy.linspace(0, 100, int(2e7))
    with open(DATA_FILE, 'wb') as f:
        f.write(a.tobytes())
    with open(META_FILE, 'w') as m:
        meta = dict(sorted(_ndarray_meta(a).items()))
        m.write(json.dumps(meta))
