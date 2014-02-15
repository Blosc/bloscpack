#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

try:
    from collections import OrderedDict
except ImportError:  # pragma: no cover
    from ordereddict import OrderedDict

# miscellaneous
FORMAT_VERSION = 3
MAGIC = 'blpk'
EXTENSION = '.blp'

# header lengths
BLOSC_HEADER_LENGTH = 16
BLOSCPACK_HEADER_LENGTH = 32
METADATA_HEADER_LENGTH = 32

# maximum values
MAX_FORMAT_VERSION = 255
MAX_CHUNKS = (2**63)-1
MAX_META_SIZE = (2**32-1)  # uint32 max val

# lookup table for human readable sizes
SUFFIXES = OrderedDict((
             ("B", 2**0 ),
             ("K", 2**10),
             ("M", 2**20),
             ("G", 2**30),
             ("T", 2**40)))
