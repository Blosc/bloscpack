#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


from __future__ import print_function


import atexit
import contextlib
import os.path as path
import shutil
import sys
import tempfile


import numpy as np


from .defaults import (DEFAULT_CHUNK_SIZE,
                       )
from .pretty import (reverse_pretty
                     )


def simple_progress(i):
    if i % 10 == 0:
        print('.', end='')
    sys.stdout.flush()


def create_array(repeats, in_file, progress=False):
    with open(in_file, 'wb') as in_fp:
        create_array_fp(repeats, in_fp, progress=progress)


def create_array_fp(repeats, in_fp, progress=False):
    for i in range(repeats):
        array_ = np.linspace(i, i+1, 2e6)
        in_fp.write(array_.tostring())
        if progress:
            progress(i)
    in_fp.flush()
    if progress:
        print('done')


def atexit_tmpremover(dirname):
    try:
        shutil.rmtree(dirname)
        print("Removed temporary directory on abort: %s" % dirname)
    except OSError:
        # if the temp dir was removed already, by the context manager
        pass


@contextlib.contextmanager
def create_tmp_files():
    tdir = tempfile.mkdtemp(prefix='bloscpack-')
    in_file = path.join(tdir, 'file')
    out_file = path.join(tdir, 'file.blp')
    dcmp_file = path.join(tdir, 'file.dcmp')
    # register the temp dir remover, safeguard against abort
    atexit.register(atexit_tmpremover, tdir)
    yield tdir, in_file, out_file, dcmp_file
    # context manager remover
    shutil.rmtree(tdir)


def cmp_file(file1, file2):
    """ File comparison utility with a small chunksize """
    with open(file1, 'rb') as fp1, open(file2, 'rb') as fp2:
        cmp_fp(fp1, fp2)


def cmp_fp(fp1, fp2):
    import nose.tools as nt  # nose is a testing dependency
    chunk_size = reverse_pretty(DEFAULT_CHUNK_SIZE)
    while True:
        a = fp1.read(chunk_size)
        b = fp2.read(chunk_size)
        if a == b'' and b == b'':
            return True
        else:
            nt.assert_equal(a, b)
