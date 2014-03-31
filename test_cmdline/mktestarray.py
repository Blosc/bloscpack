#!/usr/bin/env python

""" Utility for generating test data from cmdline interface tests.

Example usage from cram file:

  $ PYTHONPATH=$TESTDIR/../ ./$TESTDIR/mktestarray.py
  $ ls
  data.dat
  meta.json
"""


import json

from bloscpack.numpy_io import _ndarray_meta
import numpy

a = numpy.linspace(0, 100, 2e7)
with open('data.dat', 'w') as f:
    f.write(a.tostring())
with open('meta.json', 'w') as m:
    m.write(json.dumps(_ndarray_meta(a)))
