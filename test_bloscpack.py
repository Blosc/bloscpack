#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

import nose
import nose.tools as nt
from bloscpack import *

def test_print_verbose():
    nt.assert_raises(TypeError, print_verbose, 'message', 'MAXIMUM')

def test_nchunks():
    nt.assert_equal((2, 3, 4), calculate_nchunks(7, nchunks=2))
    nt.assert_raises(ChunkingException,
            calculate_nchunks, blosc.BLOSC_MAX_BUFFERSIZE*2+1, nchunks=2)

def test_create_bloscpack_header():
    nt.assert_equal('%s\x00\x00\x00\x00' % MAGIC, create_bloscpack_header(0))
    nt.assert_equal('%s\x01\x00\x00\x00' % MAGIC, create_bloscpack_header(1))
    nt.assert_equal('%s\xff\xff\xff\xff' % MAGIC,
            create_bloscpack_header(4294967295))
    nt.assert_raises(Exception, create_bloscpack_header, 4294967296)

def test_decode_bloscpack_header():
    nt.assert_equal(0, decode_bloscpack_header('%s\x00\x00\x00\x00' % MAGIC))
    nt.assert_equal(1, decode_bloscpack_header('%s\x01\x00\x00\x00' % MAGIC))
    nt.assert_equal(4294967295,
            decode_bloscpack_header('%s\xff\xff\xff\xff' % MAGIC))
    nt.assert_raises(ValueError, decode_bloscpack_header, 'blpk')
    nt.assert_raises(ValueError, decode_bloscpack_header, 'xxxxxxxx')

