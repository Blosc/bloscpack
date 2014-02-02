#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

# miscellaneous
FORMAT_VERSION = 3
MAGIC = 'blpk'
EXTENSION = '.blp'
PREFIX = "bloscpack.py"

# header lengths
BLOSC_HEADER_LENGTH = 16
BLOSCPACK_HEADER_LENGTH = 32
METADATA_HEADER_LENGTH = 32

# maximum values
MAX_FORMAT_VERSION = 255
MAX_CHUNKS = (2**63)-1
MAX_META_SIZE = (2**32-1)  # uint32 max val
