#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


from unittest import TestCase


import nose.tools as nt


from bloscpack.args import (DEFAULT_BLOSC_ARGS,
                            DEFAULT_BLOSCPACK_ARGS,
                            DEFAULT_METADATA_ARGS,
                            DEFAULT_TYPESIZE,
                            DEFAULT_CLEVEL,
                            DEFAULT_SHUFFLE,
                            DEFAULT_CNAME,
                            DEFAULT_OFFSETS,
                            DEFAULT_CHECKSUM,
                            DEFAULT_MAX_APP_CHUNKS,
                            calculate_nchunks,
                            _handle_max_apps,
                            _check_blosc_args,
                            _check_bloscpack_args,
                            _check_metadata_arguments,
                            BloscArgs,
                            BloscpackArgs,
                            )
from bloscpack.exceptions import ChunkingException
from bloscpack.headers import MAX_CHUNKS
from bloscpack.pretty import reverse_pretty


def test_check_blosc_arguments():
    missing = DEFAULT_BLOSC_ARGS.copy()
    missing.pop('typesize')
    nt.assert_raises(ValueError, _check_blosc_args, missing)
    extra = DEFAULT_BLOSC_ARGS.copy()
    extra['wtf'] = 'wtf'
    nt.assert_raises(ValueError, _check_blosc_args, extra)


def test_check_bloscpack_arguments():
    missing = DEFAULT_BLOSCPACK_ARGS.copy()
    missing.pop('offsets')
    nt.assert_raises(ValueError, _check_bloscpack_args, missing)
    extra = DEFAULT_BLOSCPACK_ARGS.copy()
    extra['wtf'] = 'wtf'
    nt.assert_raises(ValueError, _check_bloscpack_args, extra)


def test_check_bloscpack_arguments_accpets_None_as_checksum():
    args = BloscpackArgs(checksum=None)
    nt.assert_equal(args.checksum, 'None')


def test_check_metadata_arguments():
    missing = DEFAULT_METADATA_ARGS.copy()
    missing.pop('magic_format')
    nt.assert_raises(ValueError, _check_metadata_arguments, missing)
    extra = DEFAULT_METADATA_ARGS.copy()
    extra['wtf'] = 'wtf'
    nt.assert_raises(ValueError, _check_metadata_arguments, extra)


def test_calculate_nchunks():
    # check for zero or negative chunk_size
    nt.assert_raises(ValueError, calculate_nchunks,
                     23, chunk_size=0)
    nt.assert_raises(ValueError, calculate_nchunks,
                     23, chunk_size=-1)

    nt.assert_equal((9, 1, 1), calculate_nchunks(9, chunk_size=1))
    nt.assert_equal((5, 2, 1), calculate_nchunks(9, chunk_size=2))
    nt.assert_equal((3, 3, 3), calculate_nchunks(9, chunk_size=3))
    nt.assert_equal((3, 4, 1), calculate_nchunks(9, chunk_size=4))
    nt.assert_equal((2, 5, 4), calculate_nchunks(9, chunk_size=5))
    nt.assert_equal((2, 6, 3), calculate_nchunks(9, chunk_size=6))
    nt.assert_equal((2, 7, 2), calculate_nchunks(9, chunk_size=7))
    nt.assert_equal((2, 8, 1), calculate_nchunks(9, chunk_size=8))
    nt.assert_equal((1, 9, 9), calculate_nchunks(9, chunk_size=9))

    # check downgrade
    nt.assert_equal((1, 23, 23), calculate_nchunks(23, chunk_size=24))

    # single byte file
    nt.assert_equal((1, 1,  1),
                    calculate_nchunks(1, chunk_size=1))

    # check that a zero length input is handled correctly
    nt.assert_equal((1, 0,  0),
                    calculate_nchunks(0, chunk_size=1))
    # check that the chunk_size is ignored in this case
    nt.assert_equal((1, 0,  0),
                    calculate_nchunks(0, chunk_size=512))
    # in_file_size must be strictly positive
    nt.assert_raises(ValueError, calculate_nchunks, -1)

    # check overflow of nchunks due to chunk_size being too small
    # and thus stuff not fitting into the header
    nt.assert_raises(ChunkingException, calculate_nchunks,
                     MAX_CHUNKS+1, chunk_size=1)

    # check that strings are converted correctly
    nt.assert_equal((6, 1048576, 209715),
                    calculate_nchunks(reverse_pretty('5.2M')))
    nt.assert_equal((3, 2097152, 1258291),
                    calculate_nchunks(reverse_pretty('5.2M'),
                                      chunk_size='2M'))


def test_handle_max_apps():
    nt.assert_equals(_handle_max_apps(True, 10, 10), 10)
    nt.assert_equals(_handle_max_apps(True, 10, lambda x: x*10), 100)
    nt.assert_equals(_handle_max_apps(True, 1, lambda x: MAX_CHUNKS),
                     MAX_CHUNKS-1)
    nt.assert_equals(_handle_max_apps(True, 1, lambda x: MAX_CHUNKS+10),
                     MAX_CHUNKS-1)
    nt.assert_equals(_handle_max_apps(True, 1, MAX_CHUNKS),
                     MAX_CHUNKS-1)
    nt.assert_equals(_handle_max_apps(True, 10, MAX_CHUNKS),
                     MAX_CHUNKS-10)
    nt.assert_raises(TypeError, _handle_max_apps, True, 10, 10.0)
    nt.assert_raises(ValueError, _handle_max_apps,
                     True, 10, lambda x: -1)
    nt.assert_raises(ValueError, _handle_max_apps,
                     True, 10, lambda x: 1.0)


class TestBloscArgs(TestCase):

    def test_init(self):
        blosc_args = BloscArgs()
        self.assertEqual(DEFAULT_TYPESIZE, blosc_args.typesize)
        self.assertEqual(DEFAULT_CLEVEL, blosc_args.clevel)
        self.assertEqual(DEFAULT_SHUFFLE, blosc_args.shuffle)
        self.assertEqual(DEFAULT_CNAME, blosc_args.cname)


class TestBloscpackArgs(TestCase):

    def test_init(self):
        bloscpack_args = BloscpackArgs()
        self.assertEqual(DEFAULT_OFFSETS, bloscpack_args.offsets)
        self.assertEqual(DEFAULT_CHECKSUM, bloscpack_args.checksum)
        self.assertEqual(DEFAULT_MAX_APP_CHUNKS, bloscpack_args.max_app_chunks)
