# -*- coding: utf-8 -*-
# vim :set ft=py:


from unittest import TestCase


import pytest

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
    with pytest.raises(ValueError):
        _check_blosc_args(missing)
    extra = DEFAULT_BLOSC_ARGS.copy()
    extra['wtf'] = 'wtf'
    with pytest.raises(ValueError):
        _check_blosc_args(extra)


def test_check_bloscpack_arguments():
    missing = DEFAULT_BLOSCPACK_ARGS.copy()
    missing.pop('offsets')
    with pytest.raises(ValueError):
        _check_bloscpack_args(missing)
    extra = DEFAULT_BLOSCPACK_ARGS.copy()
    extra['wtf'] = 'wtf'
    with pytest.raises(ValueError):
        _check_bloscpack_args(extra)


def test_check_bloscpack_arguments_accpets_None_as_checksum():
    args = BloscpackArgs(checksum=None)
    assert args.checksum == 'None'


def test_check_metadata_arguments():
    missing = DEFAULT_METADATA_ARGS.copy()
    missing.pop('magic_format')
    with pytest.raises(ValueError):
        _check_metadata_arguments(missing)
    extra = DEFAULT_METADATA_ARGS.copy()
    extra['wtf'] = 'wtf'
    with pytest.raises(ValueError):
        _check_metadata_arguments(extra)


def test_calculate_nchunks():
    # check for zero or negative chunk_size
    with pytest.raises(ValueError):
        calculate_nchunks(23, chunk_size=0)
    with pytest.raises(ValueError):
        calculate_nchunks(23, chunk_size=-1)

    assert (9, 1, 1) == calculate_nchunks(9, chunk_size=1)
    assert (5, 2, 1) == calculate_nchunks(9, chunk_size=2)
    assert (3, 3, 3) == calculate_nchunks(9, chunk_size=3)
    assert (3, 4, 1) == calculate_nchunks(9, chunk_size=4)
    assert (2, 5, 4) == calculate_nchunks(9, chunk_size=5)
    assert (2, 6, 3) == calculate_nchunks(9, chunk_size=6)
    assert (2, 7, 2) == calculate_nchunks(9, chunk_size=7)
    assert (2, 8, 1) == calculate_nchunks(9, chunk_size=8)
    assert (1, 9, 9) == calculate_nchunks(9, chunk_size=9)

    # check downgrade
    assert (1, 23, 23) == calculate_nchunks(23, chunk_size=24)

    # single byte file
    assert (1, 1,  1) == calculate_nchunks(1, chunk_size=1)

    # check that a zero length input is handled correctly
    assert (1, 0,  0) == calculate_nchunks(0, chunk_size=1)
    # check that the chunk_size is ignored in this case
    assert (1, 0,  0) == calculate_nchunks(0, chunk_size=512)
    # in_file_size must be strictly positive
    with pytest.raises(ValueError):
        calculate_nchunks(-1)

    # check overflow of nchunks due to chunk_size being too small
    # and thus stuff not fitting into the header
    with pytest.raises(ChunkingException):
        calculate_nchunks(MAX_CHUNKS+1, chunk_size=1)

    # check that strings are converted correctly
    assert (6, 1048576, 209715) == calculate_nchunks(reverse_pretty('5.2M'))
    assert (3, 2097152, 1258291) == \
        calculate_nchunks(reverse_pretty('5.2M'), chunk_size='2M')


def test_handle_max_apps():
    assert _handle_max_apps(True, 10, 10) == 10
    assert _handle_max_apps(True, 10, lambda x: x*10) == 100
    assert _handle_max_apps(True, 1, lambda x: MAX_CHUNKS) == MAX_CHUNKS-1
    assert _handle_max_apps(True, 1, lambda x: MAX_CHUNKS+10) == MAX_CHUNKS-1
    assert _handle_max_apps(True, 1, MAX_CHUNKS) == MAX_CHUNKS-1
    assert _handle_max_apps(True, 10, MAX_CHUNKS) == MAX_CHUNKS-10
    with pytest.raises(TypeError):
        _handle_max_apps(True, 10, 10.0)
    with pytest.raises(ValueError):
        _handle_max_apps(True, 10, lambda x: -1)
    with pytest.raises(ValueError):
        _handle_max_apps(True, 10, lambda x: 1.0)


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
