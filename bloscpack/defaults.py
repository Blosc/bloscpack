#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

# blosc args
DEFAULT_TYPESIZE = 8
DEFAULT_CLEVEL = 7
DEFAULT_SHUFFLE = True
DEFAULT_CNAME = 'blosclz'

# bloscpack args
DEFAULT_OFFSETS = True
DEFAULT_CHECKSUM = 'adler32'
DEFAULT_MAX_APP_CHUNKS = lambda x: 10 * x

DEFAULT_CHUNK_SIZE = '1M'

# metadata args
DEFAULT_MAGIC_FORMAT = b'JSON'
DEFAULT_META_CHECKSUM = 'adler32'
DEFAULT_META_CODEC = 'zlib'
DEFAULT_META_LEVEL = 6
DEFAULT_MAX_META_SIZE = lambda x: 10 * x
