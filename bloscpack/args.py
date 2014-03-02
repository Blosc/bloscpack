#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import blosc


from .headers import (check_range,
                      )
from .defaults import (DEFAULT_TYPESIZE,
                       DEFAULT_CLEVEL,
                       DEFAULT_SHUFFLE,
                       DEFAULT_CNAME,
                       DEFAULT_CHUNK_SIZE,
                       DEFAULT_CHECKSUM,
                       DEFAULT_MAX_APP_CHUNKS,
                       DEFAULT_OFFSETS,
                       )
from .exceptions import(ChunkingException,
                        )
from .headers import (MAX_CHUNKS,
                      )
from .pretty import (double_pretty_size,
                     reverse_pretty,
                     )
from bloscpack import log

# Bloscpack args
BLOSCPACK_ARGS = ('offsets', 'checksum', 'max_app_chunks')
_BLOSCPACK_ARGS_SET = set(BLOSCPACK_ARGS)  # cached
DEFAULT_BLOSCPACK_ARGS = dict(zip(BLOSCPACK_ARGS,
    (DEFAULT_OFFSETS, DEFAULT_CHECKSUM, DEFAULT_MAX_APP_CHUNKS)))


# Blosc args
BLOSC_ARGS = ('typesize', 'clevel', 'shuffle', 'cname')
_BLOSC_ARGS_SET = set(BLOSC_ARGS)  # cached
DEFAULT_BLOSC_ARGS = dict(zip(BLOSC_ARGS,
    (DEFAULT_TYPESIZE, DEFAULT_CLEVEL, DEFAULT_SHUFFLE, DEFAULT_CNAME)))


# metadata args
METADATA_ARGS = ('magic_format', 'meta_checksum',
                 'meta_codec', 'meta_level', 'max_meta_size')
_METADATA_ARGS_SET = set(METADATA_ARGS)  # cached
DEFAULT_MAGIC_FORMAT = 'JSON'
DEFAULT_META_CHECKSUM = 'adler32'
DEFAULT_META_CODEC = 'zlib'
DEFAULT_META_LEVEL = 6
DEFAULT_MAX_META_SIZE = lambda x: 10 * x
DEFAULT_METADATA_ARGS = dict(zip(METADATA_ARGS,
    (DEFAULT_MAGIC_FORMAT, DEFAULT_META_CHECKSUM,
     DEFAULT_META_CODEC, DEFAULT_META_LEVEL, DEFAULT_MAX_META_SIZE)))


def _check_blosc_args(blosc_args):
    """ Check the integrity of the blosc arguments dict.

    Parameters
    ----------
    blosc_args : dict
        blosc args dictionary

    Raises
    ------
    ValueError
        if there are missing or unexpected keys present

    Notes
    -----
    Check the value of the 'BLOSC_ARGS' constant for the details of what
    keys should be contained in the dictionary.

    """
    __check_args('blosc', blosc_args, _BLOSC_ARGS_SET)


def _check_bloscpack_args(bloscpack_args):
    """ Check the integrity of the bloscpack arguments dict.

    Parameters
    ----------
    bloscpack_args : dict
        blosc args dictionary

    Raises
    ------
    ValueError
        if there are missing or unexpected keys present

    Notes
    -----
    Check the value of the 'BLOSCPACK_ARGS' constant for the details of what
    keys should be contained in the dictionary.

    """
    __check_args('bloscpack', bloscpack_args, _BLOSCPACK_ARGS_SET)


def _check_metadata_arguments(metadata_args):
    """ Check the integrity of the metadata arguments dict.

    Parameters
    ----------
    metadata_args : dict
        metadata args dictionary

    Raises
    ------
    ValueError
        if there are missing or unexpected keys present

    Notes
    -----
    Check the value of the 'METADATA_ARGS' constant for the details of what
    keys should be contained in the dictionary.

    """
    __check_args('metadata', metadata_args, _METADATA_ARGS_SET)


def __check_args(name, received, expected):
    """ Check an arg dict.

    Parameters
    ----------
    name : str
        the name of the arg dict
    received : dict
        the arg dict received
    expected : set of str
        the keys that should have been contained
    """

    received = set(received.keys())
    missing = expected.difference(received)
    if len(missing) != 0:
        raise ValueError("%s args was missing: '%s'" % (name, repr(missing)))
    extra = received.difference(expected)
    if len(extra) != 0:
        raise ValueError("%s args had some extras: '%s'" % (name, repr(extra)))


def calculate_nchunks(in_file_size, chunk_size=DEFAULT_CHUNK_SIZE):
    """ Determine chunking for an input file.

    Parameters
    ----------
    in_file_size : int
        the size of the input file
    chunk_size : int or str
        the desired chunk size

    Returns
    -------
    nchunks, chunk_size, last_chunk_size

    nchunks : int
        the number of chunks
    chunk_size : int
        the size of each chunk in bytes
    last_chunk_size : int
        the size of the last chunk in bytes

    Raises
    ------
    ChunkingException
        if the resulting nchunks is larger than MAX_CHUNKS

    """
    if in_file_size <= 0:
            raise ValueError("'in_file_size' must be strictly positive, not %d"
                             % in_file_size)
    # convert a human readable description to an int
    if isinstance(chunk_size, basestring):
        chunk_size = reverse_pretty(chunk_size)
    check_range('chunk_size', chunk_size, 1, blosc.BLOSC_MAX_BUFFERSIZE)
    # downcast
    if chunk_size > in_file_size:
        log.verbose(
            "Input was smaller than the given 'chunk_size': %s using: %s"
            % (double_pretty_size(chunk_size),
               double_pretty_size(in_file_size)))
        chunk_size = in_file_size
    quotient, remainder = divmod(in_file_size, chunk_size)
    # the user wants a single chunk
    if chunk_size == in_file_size:
        nchunks = 1
        chunk_size = in_file_size
        last_chunk_size = in_file_size
    # no remainder, perfect fit
    elif remainder == 0:
        nchunks = quotient
        last_chunk_size = chunk_size
    # with a remainder
    else:
        nchunks = quotient + 1
        last_chunk_size = remainder
    if nchunks > MAX_CHUNKS:
        raise ChunkingException(
            "nchunks: '%d' is greater than the MAX_CHUNKS: '%d'" %
            (nchunks, MAX_CHUNKS))
    log.verbose('nchunks: %d' % nchunks)
    log.verbose('chunk_size: %s' % double_pretty_size(chunk_size))
    log.verbose('last_chunk_size: %s' % double_pretty_size(last_chunk_size))
    return nchunks, chunk_size, last_chunk_size


def _handle_max_apps(offsets, nchunks, max_app_chunks):
    """ Process and handle the 'max_app_chunks' argument

    Parameters
    ----------
    offsets: bool
        if the offsets to the chunks are present
    nchunks : int
        the number of chunks
    max_app_chunks : callable or int
        the total number of possible append chunks

    Returns
    -------
    max_app_chunks : int
        the int value

    Raises
    ------
    TypeError
        if 'max_app_chunks' is neither a callable or an int
    ValueError
        if 'max_app_chunks' is a callable and returned either a non-int or a
        negative int.

    Notes
    -----
    The 'max_app_chunks' parameter can either be a function of 'nchunks'
    (callable that takes a single int as argument and returns a single int) or
    an int.  The sum of the resulting value and 'nchunks' can not be larger
    than MAX_CHUNKS.  The value of 'max_app_chunks' must be '0' if there is not
    offsets section or if nchunks is unknown (has the value '-1').

    The function performs some silent optimisations. First, if there are no
    offsets or 'nchunks' is unknown any value for 'max_app_chunks' will be
    silently ignored. Secondly, if the resulting value of max_app_chunks would
    be too large, i.e. the sum of 'nchunks' and 'max_app_chunks' is larger than
    'MAX_CHUNKS', then 'max_app_chunks' is automatically set to the maximum
    permissible value.

    """
    # first check that the args are actually valid
    check_range('nchunks',        nchunks,       -1, MAX_CHUNKS)
    # then check that we actually need to evaluate it
    if offsets and nchunks != -1:
        if hasattr(max_app_chunks, '__call__'):
            # it's a callable all right
            log.debug("max_app_chunks is a callable")
            max_app_chunks = max_app_chunks(nchunks)
            if not isinstance(max_app_chunks, (int, long)):
                raise ValueError(
                        "max_app_chunks callable returned a non integer "
                        "of type '%s'" % type(max_app_chunks))
            # check that the result is still positive, might be quite large
            if max_app_chunks < 0:
                raise ValueError(
                        'max_app_chunks callable returned a negative integer')
        elif isinstance(max_app_chunks, (int, long)):
            # it's a plain int, check its range
            log.debug("max_app_chunks is an int")
            check_range('max_app_chunks', max_app_chunks, 0, MAX_CHUNKS)
        else:
            raise TypeError('max_app_chunks was neither a callable or an int')
        # we managed to get a reasonable value, make sure it's not too large
        if nchunks + max_app_chunks > MAX_CHUNKS:
            max_app_chunks = MAX_CHUNKS - nchunks
            log.debug(
                    "max_app_chunks was too large, setting to max value: %d"
                    % max_app_chunks)
    else:
        if max_app_chunks is not None:
            log.debug('max_app_chunks will be silently ignored')
        max_app_chunks = 0
    log.debug("max_app_chunks was set to: %d" % max_app_chunks)
    return max_app_chunks


