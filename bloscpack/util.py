#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import contextlib
import sys


PYTHON_VERSION = sys.version_info[0:3]
if sys.version_info < (2, 7, 5):  # pragma: no cover
    memoryview = lambda x: x
else:
    memoryview = memoryview
