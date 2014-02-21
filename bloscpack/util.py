#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import contextlib
import sys


@contextlib.contextmanager
def open_two_file(input_fp, output_fp):
    """ Hack for making with statement work on two files with 2.6. """
    yield input_fp, output_fp
    input_fp.close()
    output_fp.close()

PYTHON_VERSION = sys.version_info[0:3]
if sys.version_info < (2, 7, 5):  # pragma: no cover
    memoryview = lambda x: x
else:
    memoryview = memoryview
