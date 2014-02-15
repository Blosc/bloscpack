#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

""" Command line interface to Blosc via python-blosc """

from __future__ import division

import abc
import contextlib
import cStringIO
import json
import itertools
import os.path as path
import pprint
import struct
import sys


import blosc
import numpy as np

from .checksums import (check_valid_checksum,
                        CHECKSUMS_LOOKUP,
                        CHECKSUMS_AVAIL,
                        )

from .exceptions import FileNotFound

from .constants import (FORMAT_VERSION,
                        EXTENSION,
                        BLOSC_HEADER_LENGTH,
                        BLOSCPACK_HEADER_LENGTH,
                        METADATA_HEADER_LENGTH,
                        MAX_CHUNKS,
                        MAX_META_SIZE,
                        MAX_CLEVEL,
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
from .metacodecs import (CODECS_AVAIL,
                         CODECS_LOOKUP,
                         check_valid_codec,
                         )
from .pretty import (pretty_size,
                     double_pretty_size,
                     reverse_pretty,
                     )
from .serializers import(SERIZLIALIZERS_LOOKUP,
                         check_valid_serializer,
                         )
from .cli import (check_files,
                  create_parser,
                  )
from .log import (print_verbose,
                  print_debug,
                  print_normal,
                  error,
                  )

from .version import __version__  # pragma: no cover


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
METADATA_ARGS = ('magic_format', 'meta_checksum', 'meta_codec', 'meta_level', 'max_meta_size')
_METADATA_ARGS_SET = set(METADATA_ARGS)  # cached
DEFAULT_MAGIC_FORMAT = 'JSON'
DEFAULT_META_CHECKSUM = 'adler32'
DEFAULT_META_CODEC = 'zlib'
DEFAULT_META_LEVEL = 6
DEFAULT_MAX_META_SIZE = lambda x: 10 * x
DEFAULT_METADATA_ARGS = dict(zip(METADATA_ARGS,
    (DEFAULT_MAGIC_FORMAT, DEFAULT_META_CHECKSUM,
     DEFAULT_META_CODEC, DEFAULT_META_LEVEL, DEFAULT_MAX_META_SIZE)))



class ChunkingException(BaseException):
    pass


class FormatVersionMismatch(RuntimeError):
    pass


class ChecksumMismatch(RuntimeError):
    pass


class ChecksumLengthMismatch(RuntimeError):
    pass


class NoMetadataFound(RuntimeError):
    pass


class NoChangeInMetadata(RuntimeError):
    pass


class MetadataSectionTooSmall(RuntimeError):
    pass


class NonUniformTypesize(RuntimeError):
    pass


class NotEnoughSpace(RuntimeError):
    pass


class NotANumpyArray(RuntimeError):
    pass


def decode_uint8(byte):
    return struct.unpack('<B', byte)[0]


def decode_uint32(fourbyte):
    return struct.unpack('<I', fourbyte)[0]


def decode_int32(fourbyte):
    return struct.unpack('<i', fourbyte)[0]


def decode_int64(eightbyte):
    return struct.unpack('<q', eightbyte)[0]


def decode_bitfield(byte):
    return bin(decode_uint8(byte))[2:].rjust(8, '0')


def decode_magic_string(str_):
    return str_.strip('\x00')


def encode_uint8(byte):
    return struct.pack('<B', byte)


def encode_uint32(byte):
    return struct.pack('<I', byte)


def encode_int32(fourbyte):
    return struct.pack('<i', fourbyte)


def encode_int64(eightbyte):
    return struct.pack('<q', eightbyte)


@contextlib.contextmanager
def open_two_file(input_fp, output_fp):
    """ Hack for making with statement work on two files with 2.6. """
    yield input_fp, output_fp
    input_fp.close()
    output_fp.close()

PYTHON_VERSION = sys.version_info[0:3]
if sys.version_info < (2, 7, 5):  # pragma: no cover
    memoryview = lambda x: x


def decode_blosc_header(buffer_):
    """ Read and decode header from compressed Blosc buffer.

    Parameters
    ----------
    buffer_ : string of bytes
        the compressed buffer

    Returns
    -------
    settings : dict
        a dict containing the settings from Blosc

    Notes
    -----
    Please see the readme for a precise descripttion of the blosc header
    format.

    """
    buffer_ = memoryview(buffer_)
    return {'version':   decode_uint8(buffer_[0]),
            'versionlz': decode_uint8(buffer_[1]),
            'flags':     decode_uint8(buffer_[2]),
            'typesize':  decode_uint8(buffer_[3]),
            'nbytes':    decode_uint32(buffer_[4:8]),
            'blocksize': decode_uint32(buffer_[8:12]),
            'ctbytes':   decode_uint32(buffer_[12:16])}


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
        print_verbose(
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
    print_verbose('nchunks: %d' % nchunks)
    print_verbose('chunk_size: %s' % double_pretty_size(chunk_size))
    print_verbose('last_chunk_size: %s' % double_pretty_size(last_chunk_size))
    return nchunks, chunk_size, last_chunk_size


def check_range(name, value, min_, max_):
    """ Check that a variable is in range. """
    if not isinstance(value, (int, long)):
        raise TypeError("'%s' must be of type 'int'" % name)
    elif not min_ <= value <= max_:
        raise ValueError(
                "'%s' must be in the range %s <= n <= %s, not '%s'" %
                tuple(map(str, (name, min_, max_, value))))


def _check_str(name, value, max_len):
    if not isinstance(value, str):
        raise TypeError("'%s' must be of type 'int'" % name)
    elif len(value) > max_len:
        raise ValueError("'%s' can be of max length '%i' but is: '%s'" %
                (name, max_len, len(value)))


def _pad_with_nulls(str_, len_):
    """ Pad string with null bytes.

    Parameters
    ----------
    str_ : str
        the string to pad
    len_ : int
        the final desired length
    """
    return str_ + ("\x00" * (len_ - len(str_)))


def _check_options(options):
    """ Check the options bitfield.

    Parameters
    ----------
    options : str

    Raises
    ------
    TypeError
        if options is not a string
    ValueError
        either if any character in option is not a zero or a one, or if options
        is not of length 8
    """

    if not isinstance(options, str):
        raise TypeError("'options' must be of type 'str', not '%s'" %
                type(options))
    elif (not len(options) == 8 or
            not all(map(lambda x: x in ['0', '1'], iter(options)))):
        raise ValueError(
                "'options' must be string of 0s and 1s of length 8, not '%s'" %
                options)


def _check_options_zero(options, indices):
    for i in indices:
        if options[i] != '0':
            raise ValueError(
                'Element %i was non-zero when attempting to decode options')


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


def create_options(offsets=DEFAULT_OFFSETS, metadata=False):
    """ Create the options bitfield.

    Parameters
    ----------
    offsets : bool
    metadata : bool
    """
    return "".join([str(int(i)) for i in
            [False, False, False, False, False, False, metadata, offsets]])


def decode_options(options):
    """ Parse the options bitfield.

    Parameters
    ----------
    options : str
        the options bitfield

    Returns
    -------
    options : dict mapping str -> bool
    """

    _check_options(options)
    _check_options_zero(options, range(6))
    return {'offsets': bool(int(options[7])),
            'metadata': bool(int(options[6])),
            }


def create_metadata_options():
    """ Create the metadata options bitfield. """
    return "00000000"


def decode_metadata_options(options):
    _check_options(options)
    _check_options_zero(options, range(8))
    return {}

from .headers import BloscPackHeader

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
            print_debug("max_app_chunks is a callable")
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
            # it's a plain int, check it's range
            print_debug("max_app_chunks is an int")
            check_range('max_app_chunks', max_app_chunks, 0, MAX_CHUNKS)
        else:
            raise TypeError('max_app_chunks was neither a callable or an int')
        # we managed to get a reasonable value, make sure it's not too large
        if nchunks + max_app_chunks > MAX_CHUNKS:
            max_app_chunks = MAX_CHUNKS - nchunks
            print_debug(
                    "max_app_chunks was too large, setting to max value: %d"
                    % max_app_chunks)
    else:
        if max_app_chunks is not None:
            print_debug('max_app_chunks will be silently ignored')
        max_app_chunks = 0
    print_debug("max_app_chunks was set to: %d" % max_app_chunks)
    return max_app_chunks


def create_metadata_header(magic_format='',
       options="00000000",
       meta_checksum='None',
       meta_codec='None',
       meta_level=0,
       meta_size=0,
       max_meta_size=0,
       meta_comp_size=0,
       user_codec='',
       ):
    _check_str('magic-format',     magic_format,  8)
    _check_options(options)
    check_valid_checksum(meta_checksum)
    check_valid_codec(meta_codec)
    check_range('meta_level',      meta_level,     0, MAX_CLEVEL)
    check_range('meta_size',       meta_size,      0, MAX_META_SIZE)
    check_range('max_meta_size',   max_meta_size,  0, MAX_META_SIZE)
    check_range('meta_comp_size',  meta_comp_size, 0, MAX_META_SIZE)
    _check_str('user_codec',       user_codec,     8)

    magic_format        = _pad_with_nulls(magic_format, 8)
    options             = encode_uint8(int(options, 2))
    meta_checksum       = encode_uint8(CHECKSUMS_AVAIL.index(meta_checksum))
    meta_codec          = encode_uint8(CODECS_AVAIL.index(meta_codec))
    meta_level          = encode_uint8(meta_level)
    meta_size           = encode_uint32(meta_size)
    max_meta_size       = encode_uint32(max_meta_size)
    meta_comp_size      = encode_uint32(meta_comp_size)
    user_codec          = _pad_with_nulls(user_codec, 8)

    return magic_format + options + meta_checksum + meta_codec + meta_level + \
            meta_size + max_meta_size + meta_comp_size + user_codec


def decode_metadata_header(buffer_):
    if len(buffer_) != 32:
        raise ValueError(
            "attempting to decode a bloscpack metadata header of length '%d', not '32'"
            % len(buffer_))
    return {'magic_format':        decode_magic_string(buffer_[:8]),
            'meta_options':        decode_bitfield(buffer_[8]),
            'meta_checksum':       CHECKSUMS_AVAIL[decode_uint8(buffer_[9])],
            'meta_codec':          CODECS_AVAIL[decode_uint8(buffer_[10])],
            'meta_level':          decode_uint8(buffer_[11]),
            'meta_size':           decode_uint32(buffer_[12:16]),
            'max_meta_size':       decode_uint32(buffer_[16:20]),
            'meta_comp_size':      decode_uint32(buffer_[20:24]),
            'user_codec':          decode_magic_string(buffer_[24:32])
            }


def _blosc_args_from_args(args):
    return dict((arg, args.__getattribute__(arg)) for arg in BLOSC_ARGS)


def process_compression_args(args):
    """ Extract and check the compression args after parsing by argparse.

    Parameters
    ----------
    args : argparse.Namespace
        the parsed command line arguments

    Returns
    -------
    in_file : str
        the input file name
    out_file : str
        the out_file name
    blosc_args : tuple of (int, int, bool)
        typesize, clevel and shuffle
    """
    in_file = args.in_file
    out_file = args.out_file or in_file + EXTENSION
    return in_file, out_file, _blosc_args_from_args(args)


def process_decompression_args(args):
    """ Extract and check the decompression args after parsing by argparse.

    Warning: may call sys.exit()

    Parameters
    ----------
    args : argparse.Namespace
        the parsed command line arguments

    Returns
    -------
    in_file : str
        the input file name
    out_file : str
        the out_file name
    """
    in_file = args.in_file
    out_file = args.out_file
    # remove the extension for output file
    if args.no_check_extension:
        if out_file is None:
            error('--no-check-extension requires use of <out_file>')
    else:
        if in_file.endswith(EXTENSION):
            out_file = args.out_file or in_file[:-len(EXTENSION)]
        else:
            error("input file '%s' does not end with '%s'" %
                    (in_file, EXTENSION))
    return in_file, out_file


def process_append_args(args):
    original_file = args.original_file
    new_file = args.new_file
    if not args.no_check_extension and not original_file.endswith(EXTENSION):
        error("original file '%s' does not end with '%s'" %
                    (original_file, EXTENSION))

    return original_file, new_file


def process_metadata_args(args):
    if args.metadata is not None:
        try:
            with open(args.metadata, 'r') as metadata_file:
                return json.loads(metadata_file.read().strip())
        except IOError as ioe:
            error(ioe.message)


def process_nthread_arg(args):
    """ Extract and set nthreads. """
    if args.nthreads != blosc.ncores:
        blosc.set_nthreads(args.nthreads)
    print_verbose('using %d thread%s' %
            (args.nthreads, 's' if args.nthreads > 1 else ''))


def _write_metadata(output_fp, metadata, metadata_args):
    """ Write the metadata to a file pointer.

    Parameters
    ----------
    output_fp : file like
        the file pointer to write to
    metadata : dict
        the metadata to write
    metadata_args : dict
        the metadata args

    Returns
    -------
    metadata_total : int
        the total number of bytes occupied by metadata header, metadata plus
        preallocation and checksum

    Notes
    -----
    The 'output_fp' should point to the position in the file where the metadata
    should be written.

    """
    _check_metadata_arguments(metadata_args)
    metadata_total = 0
    print_debug('metadata args are:')
    for arg, value in metadata_args.iteritems():
        print_debug('\t%s: %s' % (arg, value))
    metadata_total += METADATA_HEADER_LENGTH
    serializer_impl = SERIZLIALIZERS_LOOKUP[metadata_args['magic_format']]
    metadata = serializer_impl.dumps(metadata)
    codec = 'None'
    if metadata_args['meta_codec'] != CODECS_AVAIL[0]:
        codec_impl = CODECS_LOOKUP[metadata_args['meta_codec']]
        metadata_compressed = codec_impl.compress(metadata,
                metadata_args['meta_level'])
        meta_size = len(metadata)
        meta_comp_size = len(metadata_compressed)
        # be opportunistic, avoid compression if not beneficial
        if meta_size < meta_comp_size:
            print_debug('metadata compression requested, but it was not '
                    'beneficial, deactivating '
                    "(raw: '%s' vs. compressed: '%s') " %
                    (meta_size, meta_comp_size))
            meta_comp_size = meta_size
        else:
            codec = codec_impl.name
            metadata = metadata_compressed
    else:
        meta_size = len(metadata)
        meta_comp_size = meta_size
    print_debug("Raw %s metadata of size '%s': %s" %
            ('compressed' if metadata_args['meta_codec'] != 'None' else
                'uncompressed', meta_comp_size, repr(metadata)))
    if hasattr(metadata_args['max_meta_size'], '__call__'):
        max_meta_size = metadata_args['max_meta_size'](meta_size)
    elif isinstance(metadata_args['max_meta_size'], int):
        max_meta_size = metadata_args['max_meta_size']
    print_debug('max meta size is deemed to be: %d' %
            max_meta_size)
    if meta_comp_size > max_meta_size:
        raise MetadataSectionTooSmall(
                'metadata section is too small to contain the metadata '
                'required: %d allocated: %d' %
                (meta_comp_size, max_meta_size))
    metadata_total += meta_comp_size
    # create metadata header
    raw_metadata_header = create_metadata_header(
            magic_format=metadata_args['magic_format'],
            meta_checksum=metadata_args['meta_checksum'],
            meta_codec=codec,
            meta_level=metadata_args['meta_level'],
            meta_size=meta_size,
            max_meta_size=max_meta_size,
            meta_comp_size=meta_comp_size)
    print_debug('raw_metadata_header: %s' % repr(raw_metadata_header))
    output_fp.write(raw_metadata_header)
    output_fp.write(metadata)
    prealloc = max_meta_size - meta_comp_size
    for i in xrange(prealloc):
        output_fp.write('\x00')
    metadata_total += prealloc
    print_debug("metadata has %d preallocated empty bytes" % prealloc)
    if metadata_args['meta_checksum'] != CHECKSUMS_AVAIL[0]:
        metadata_checksum_impl = CHECKSUMS_LOOKUP[metadata_args['meta_checksum']]
        metadata_digest = metadata_checksum_impl(metadata)
        metadata_total += metadata_checksum_impl.size
        output_fp.write(metadata_digest)
        print_debug("metadata checksum (%s): %s" %
                (metadata_args['meta_checksum'], repr(metadata_digest)))
    print_debug("metadata section occupies a total of %s" %
            double_pretty_size(metadata_total))
    return metadata_total


def _compress_chunk_str(chunk, blosc_args):
    return blosc.compress(chunk, **blosc_args)


def _compress_chunk_ptr(chunk, blosc_args):
    ptr, size = chunk
    return blosc.compress_ptr(ptr, size, **blosc_args)


def _write_compressed_chunk(output_fp, compressed, digest):
    output_fp.write(compressed)
    if len(digest) > 0:
        output_fp.write(digest)


def pack_file(in_file, out_file, chunk_size=DEFAULT_CHUNK_SIZE, metadata=None,
        blosc_args=DEFAULT_BLOSC_ARGS,
        bloscpack_args=DEFAULT_BLOSCPACK_ARGS,
        metadata_args=DEFAULT_METADATA_ARGS):
    """ Main function for compressing a file.

    Parameters
    ----------
    in_file : str
        the name of the input file
    out_file : str
        the name of the output file
    chunk_size : int
        the desired chunk size in bytes
    metadata : dict
        the metadata dict
    blosc_args : dict
        blosc keyword args
    bloscpack_args : dict
        bloscpack keyword args
    metadata_args : dict
        metadata keyword args

    Raises
    ------

    ChunkingException
        if there was a problem caculating the chunks

    # TODO document which arguments are silently ignored

    """
    in_file_size = path.getsize(in_file)
    print_verbose('input file size: %s' % double_pretty_size(in_file_size))
    # calculate chunk sizes
    nchunks, chunk_size, last_chunk_size = \
            calculate_nchunks(in_file_size, chunk_size)
    with open_two_file(open(in_file, 'rb'), open(out_file, 'wb')) as \
            (input_fp, output_fp):
        source = PlainFPSource(input_fp)
        sink = CompressedFPSink(output_fp)
        pack(source, sink,
                nchunks, chunk_size, last_chunk_size,
                metadata=metadata,
                blosc_args=blosc_args,
                bloscpack_args=bloscpack_args,
                metadata_args=metadata_args)
    out_file_size = path.getsize(out_file)
    print_verbose('output file size: %s' % double_pretty_size(out_file_size))
    print_verbose('compression ratio: %f' % (out_file_size/in_file_size))


class PlainSource(object):

    _metaclass__ = abc.ABCMeta

    def configure(self, chunk_size, last_chunk, nchunks):
        self.chunk_size = chunk_size
        self.last_chunk = last_chunk
        self.nchunks = nchunks

    @property
    def compress_func(self):
        return _compress_chunk_str

    def __iter__(self):
        return self()

    @abc.abstractmethod
    def __call__(self):
        pass


class CompressedSource(object):

    _metaclass__ = abc.ABCMeta

    def __iter__(self):
        return self()

    @abc.abstractmethod
    def __call__(self):
        pass

class PlainFPSource(PlainSource):

    def __init__(self, input_fp):
        self.input_fp = input_fp

    def __call__(self):
        # if nchunks == 1 the last_chunk_size is the size of the single chunk
        for num_bytes in ([self.chunk_size] * (self.nchunks - 1) +
                [self.last_chunk]):
            yield self.input_fp.read(num_bytes)


class CompressedFPSource(CompressedSource):

    def __init__(self, input_fp):

        self.input_fp = input_fp
        self.bloscpack_header, self.metadata, self.metadata_header, \
                self.offsets = _read_beginning(input_fp)
        self.checksum_impl = self.bloscpack_header.checksum_impl
        self.nchunks = self.bloscpack_header.nchunks

    def __call__(self):
        for i in xrange(self.nchunks):
            compressed, header = _read_compressed_chunk_fp(self.input_fp, self.checksum_impl)
            yield compressed


class PlainMemorySource(PlainSource):

    def __init__(self, chunks):
        self.chunks = chunks

    def __call__(self):
        for c in self.chunks:
            yield c


class CompressedMemorySource(CompressedSource):

    @property
    def metadata(self):
        return self.compressed_memory_sink.metadata

    def __init__(self, compressed_memory_sink):
        self.compressed_memory_sink = compressed_memory_sink
        self.checksum_impl = compressed_memory_sink.checksum_impl
        self.checksum = compressed_memory_sink.checksum
        self.nchunks = compressed_memory_sink.nchunks

        self.chunks = compressed_memory_sink.chunks
        if self.checksum:
            self.checksums = compressed_memory_sink.checksums

    def __call__(self):
        for i in xrange(self.nchunks):
            compressed = self.chunks[i]
            if self.checksum:
                expected_digest = self.checksums[i]
                received_digest = self.checksum_impl(compressed)
                if received_digest != expected_digest:
                    raise ChecksumMismatch(
                            "Checksum mismatch detected in chunk, "
                            "expected: '%s', received: '%s'" %
                            (repr(expected_digest), repr(received_digest)))
            yield compressed


class PlainNumpySource(PlainSource):

    def __init__(self, ndarray):

        # Reagrding the dtype, quote from numpy/lib/format.py:dtype_to_descr
        #
        # The .descr attribute of a dtype object cannot be round-tripped
        # through the dtype() constructor. Simple types, like dtype('float32'),
        # have a descr which looks like a record array with one field with ''
        # as a name. The dtype() constructor interprets this as a request to
        # give a default name.  Instead, we construct descriptor that can be
        # passed to dtype().
        self.metadata = {'dtype': ndarray.dtype.descr
                         if ndarray.dtype.names is not None
                         else ndarray.dtype.str,
                         'shape': ndarray.shape,
                         'order': 'F' if np.isfortran(ndarray) else 'C',
                         'container': 'numpy',
                         }
        self.size = ndarray.size * ndarray.itemsize
        self.ndarray = np.ascontiguousarray(ndarray)
        self.ptr = ndarray.__array_interface__['data'][0]

    @property
    def compress_func(self):
        return _compress_chunk_ptr

    def __call__(self):
        self.nitems = int(self.chunk_size / self.ndarray.itemsize)
        offset = self.ptr
        for i in xrange(self.nchunks - 1):
            yield offset, self.nitems
            offset += self.chunk_size
        yield offset, int(self.last_chunk / self.ndarray.itemsize)


class PlainSink(object):

    _metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def put(self, chunk):
        pass


class CompressedSink(object):

    _metaclass__ = abc.ABCMeta

    def configure(self, blosc_args, bloscpack_header):
        self.blosc_args = blosc_args
        self.bloscpack_header = bloscpack_header
        self.checksum_impl = bloscpack_header.checksum_impl
        self.offsets = bloscpack_header.offsets

    @abc.abstractmethod
    def write_bloscpack_header(self):
        pass

    @abc.abstractmethod
    def write_metadata(self, metadata, metadata_args):
        pass

    @abc.abstractmethod
    def init_offsets(self):
        pass

    @abc.abstractmethod
    def finalize(self):
        pass

    @abc.abstractmethod
    def put(self, i, compressed):
        pass

    def do_checksum(self, compressed):
        if self.checksum_impl.size > 0:
            # compute the checksum on the compressed data
            digest = self.checksum_impl(compressed)
            print_debug('checksum (%s): %s ' %
                    (self.checksum_impl.name, repr(digest)))
        else:
            digest = ''
            print_debug('no checksum')
        return digest


class PlainFPSink(PlainSink):

    def __init__(self, output_fp, nchunks=None):
        self.output_fp = output_fp
        self.nchunks = nchunks
        self.i = 0

    def put(self, compressed):
        print_debug("decompressing chunk '%d'%s" %
                (self.i, ' (last)' if self.nchunks is not None
                                   and self.i == self.nchunks - 1 else ''))
        decompressed = blosc.decompress(compressed)
        print_debug("chunk handled, in: %s out: %s" %
                (pretty_size(len(compressed)),
                    pretty_size(len(decompressed))))
        self.output_fp.write(decompressed)
        self.i += 1


class CompressedFPSink(CompressedSink):

    def __init__(self, output_fp):
        self.output_fp = output_fp
        self.meta_total = 0

    def write_bloscpack_header(self):
        raw_bloscpack_header = self.bloscpack_header.encode()
        self.output_fp.write(raw_bloscpack_header)

    def write_metadata(self, metadata, metadata_args):
        self.meta_total += _write_metadata(self.output_fp,
                metadata, metadata_args)

    def init_offsets(self):
        if self.offsets:
            total_entries = self.bloscpack_header.nchunks + \
                    self.bloscpack_header.max_app_chunks
            self.offset_storage = list(itertools.repeat(-1,
                self.bloscpack_header.nchunks))
            self.output_fp.write(encode_int64(-1) * total_entries)

    def finalize(self):
        if self.offsets:
            self.output_fp.seek(BLOSCPACK_HEADER_LENGTH + self.meta_total, 0)
            _write_offsets(self.output_fp, self.offset_storage)

    def put(self, i, compressed):
        offset = self.output_fp.tell()
        digest = self.do_checksum(compressed)
        _write_compressed_chunk(self.output_fp, compressed, digest)
        if self.offsets:
            self.offset_storage[i] = offset
        return offset, compressed, digest


class PlainMemorySink(PlainSink):

    def __init__(self, nchunks=None):
        if nchunks is not None:
            self.have_chunks = True
            self.chunks = [None] * nchunks
            self.i = 0
        else:
            self.have_chunks = False
            self.chunks = []

    def put(self, chunk):
        if self.have_chunks:
            self.chunks[self.i] = chunk
            self.i += 1
        else:
            self.chunks.append(chunk)


class CompressedMemorySink(CompressedSink):

    def configure(self, blosc_args, bloscpack_header):
        self.blosc_args = blosc_args
        self.bloscpack_header = bloscpack_header
        self.checksum_impl = bloscpack_header.checksum_impl
        self.checksum = bloscpack_header.checksum
        self.nchunks = bloscpack_header.nchunks

        self.chunks = [None] * self.bloscpack_header.nchunks
        if self.checksum:
            self.checksums = [None] * self.bloscpack_header.nchunks

        self.metadata = None
        self.metadata_args = None

    def write_bloscpack_header(self):
        # no op
        pass

    def write_metadata(self, metadata, metadata_args):
        self.metadata = metadata
        self.metadata_args = metadata_args

    def init_offsets(self):
        # no op
        pass

    def put(self, i, compressed):
        self.chunks[i] = compressed
        if self.checksum:
            self.checksums[i] = self.do_checksum(compressed)


class PlainNumpySink(PlainSink):

    def __init__(self, metadata):
        self.metadata = metadata
        if metadata is None or metadata['container'] != 'numpy':
            raise NotANumpyArray
        self.ndarray = np.empty(metadata['shape'],
                dtype=np.dtype(metadata['dtype']),
                order=metadata['order'])
        self.ptr = self.ndarray.__array_interface__['data'][0]

    def put(self, compressed):
        bwritten = blosc.decompress_ptr(compressed, self.ptr)
        self.ptr += bwritten


def pack(source, sink,
        nchunks, chunk_size, last_chunk,
        metadata=None,
        blosc_args=DEFAULT_BLOSC_ARGS,
        bloscpack_args=DEFAULT_BLOSCPACK_ARGS,
        metadata_args=DEFAULT_METADATA_ARGS):
    """ Core packing function.  """
    _check_blosc_args(blosc_args)
    print_debug('blosc args are:')
    for arg, value in blosc_args.iteritems():
        print_debug('\t%s: %s' % (arg, value))
    _check_bloscpack_args(bloscpack_args)
    print_debug('bloscpack args are:')
    for arg, value in bloscpack_args.iteritems():
        print_debug('\t%s: %s' % (arg, value))
    max_app_chunks = _handle_max_apps(bloscpack_args['offsets'],
            nchunks,
            bloscpack_args['max_app_chunks'])
    # create the bloscpack header
    bloscpack_header = BloscPackHeader(
            offsets=bloscpack_args['offsets'],
            metadata=metadata is not None,
            checksum=bloscpack_args['checksum'],
            typesize=blosc_args['typesize'],
            chunk_size=chunk_size,
            last_chunk=last_chunk,
            nchunks=nchunks,
            max_app_chunks=max_app_chunks
            )
    source.configure(chunk_size, last_chunk, nchunks)
    sink.configure(blosc_args, bloscpack_header)
    sink.write_bloscpack_header()
    # deal with metadata
    if metadata is not None:
        sink.write_metadata(metadata, metadata_args)
    elif metadata_args is not None:
        print_debug('metadata_args will be silently ignored')
    sink.init_offsets()

    compress_func = source.compress_func
    # read-compress-write loop
    for i, chunk in enumerate(source()):
        print_debug("Handle chunk '%d' %s" %
                    (i,'(last)' if i == nchunks -1 else ''))
        compressed = compress_func(chunk, blosc_args)
        sink.put(i, compressed)

    sink.finalize()


def pack_ndarray(ndarray, sink,
        chunk_size=DEFAULT_CHUNK_SIZE,
        blosc_args=DEFAULT_BLOSC_ARGS,
        bloscpack_args=DEFAULT_BLOSCPACK_ARGS,
        metadata_args=DEFAULT_METADATA_ARGS):
    """ Serialialize a Numpy array.

    Parameters
    ----------
    ndarray : ndarray
        the numpy array to serialize
    sink : CompressedSink
        the sink to serialize to
    blosc_args : dict
        the args for blosc
    bloscpack_args : dict
        the args for bloscpack
    metadata_args : dict
        the args for the metadata

    Notes
    -----

    The 'typesize' value of 'blosc_args' will be silently ignored and replaced
    with the itemsize of the Numpy array's dtype.
    """

    blosc_args = blosc_args.copy()
    blosc_args['typesize'] = ndarray.dtype.itemsize
    source = PlainNumpySource(ndarray)
    nchunks, chunk_size, last_chunk_size = \
            calculate_nchunks(source.size, chunk_size)
    pack(source, sink,
            nchunks, chunk_size, last_chunk_size,
            metadata=source.metadata,
            blosc_args=blosc_args,
            bloscpack_args=bloscpack_args,
            metadata_args=metadata_args)
    #out_file_size = path.getsize(file_pointer)
    #print_verbose('output file size: %s' % double_pretty_size(out_file_size))
    #print_verbose('compression ratio: %f' % (out_file_size/source.size))


def pack_ndarray_file(ndarray, filename,
                      chunk_size=DEFAULT_CHUNK_SIZE,
                      blosc_args=DEFAULT_BLOSC_ARGS,
                      bloscpack_args=DEFAULT_BLOSCPACK_ARGS,
                      metadata_args=DEFAULT_METADATA_ARGS):
    with open(filename, 'wb') as fp:
        sink = CompressedFPSink(fp)
        pack_ndarray(ndarray, sink,
                    chunk_size=chunk_size,
                    blosc_args=blosc_args,
                    bloscpack_args=bloscpack_args,
                    metadata_args=metadata_args)


def pack_ndarray_str(ndarray,
                      chunk_size=DEFAULT_CHUNK_SIZE,
                      blosc_args=DEFAULT_BLOSC_ARGS,
                      bloscpack_args=DEFAULT_BLOSCPACK_ARGS,
                      metadata_args=DEFAULT_METADATA_ARGS):
    sio = cStringIO.StringIO()
    sink = CompressedFPSink(sio)
    pack_ndarray(ndarray, sink,
                    chunk_size=chunk_size,
                    blosc_args=blosc_args,
                    bloscpack_args=bloscpack_args,
                    metadata_args=metadata_args)
    return sio.getvalue()

def unpack_ndarray(source):
    """ Deserialize a Numpy array.

    Parameters
    ----------
    source : CompressedSource
        the source containing the serialized Numpy array

    Returns
    -------
    ndarray : ndarray
        the Numpy array

    Raises
    ------
    NotANumpyArray
        if the source doesn't seem to contain a Numpy array
    """

    sink = PlainNumpySink(source.metadata)
    for compressed in iter(source):
        sink.put(compressed)
    return sink.ndarray


def unpack_ndarray_file(filename):
    source = CompressedFPSource(open(filename, 'rb'))
    return unpack_ndarray(source)

def unpack_ndarray_str(str_):
    sio = cStringIO.StringIO(str_)

    source = CompressedFPSource(sio)
    sink = PlainNumpySink(source.metadata)
    for compressed in iter(source):
        sink.put(compressed)
    return sink.ndarray


def _read_bloscpack_header(input_fp):
    """ Read the bloscpack header.

    Parameters
    ----------
    input_fp : file like
        a file pointer to read from

    Returns
    -------
    bloscpack_header : BloscPackHeader
        the decoded bloscpack header

    Raises
    ------
    FormatVersionMismatch
        if the received format version is not equal to the one this module can
        decode

    """
    print_debug('reading bloscpack header')
    bloscpack_header_raw = input_fp.read(BLOSCPACK_HEADER_LENGTH)
    print_debug('bloscpack_header_raw: %s' %
            repr(bloscpack_header_raw))
    bloscpack_header = BloscPackHeader.decode(bloscpack_header_raw)
    print_debug("bloscpack header: %s" % repr(bloscpack_header))
    if FORMAT_VERSION != bloscpack_header.format_version:
        raise FormatVersionMismatch(
                "format version of file was not '%s' as expected, but '%d'" %
                (FORMAT_VERSION, bloscpack_header.format_version))
    return bloscpack_header


def _read_metadata(input_fp):
    """ Read the metadata and header from a file pointer.

    Parameters
    ----------
    input_fp : file like
        a file pointer to read from

    Returns
    -------
    metadata : dict
        the metadata
    metadata_header : dict
        the metadata contents as dict

    Notes
    -----
    The 'input_fp' should point to the position where the metadata starts. The
    number of bytes to read will be determined from the metadata header.

    """
    raw_metadata_header = input_fp.read(METADATA_HEADER_LENGTH)
    print_debug("raw metadata header: '%s'" % repr(raw_metadata_header))
    metadata_header = decode_metadata_header(raw_metadata_header)
    print_debug("metadata header: ")
    for arg, value in metadata_header.iteritems():
        print_debug('\t%s: %s' % (arg, value))
    metadata = input_fp.read(metadata_header['meta_comp_size'])
    prealloc = metadata_header['max_meta_size'] - metadata_header['meta_comp_size']
    input_fp.seek(prealloc, 1)
    if metadata_header['meta_checksum'] != 'None':
        metadata_checksum_impl = CHECKSUMS_LOOKUP[metadata_header['meta_checksum']]
        metadata_expected_digest = input_fp.read(metadata_checksum_impl.size)
        metadata_received_digest = metadata_checksum_impl(metadata)
        if metadata_received_digest != metadata_expected_digest:
            raise ChecksumMismatch(
                    "Checksum mismatch detected in metadata "
                    "expected: '%s', received: '%s'" %
                    (repr(metadata_expected_digest),
                        repr(metadata_received_digest)))
        else:
            print_debug('metadata checksum OK (%s): %s ' %
                    (metadata_checksum_impl.name,
                        repr(metadata_received_digest)))
    if metadata_header['meta_codec'] != 'None':
        metadata_codec_impl = CODECS_LOOKUP[metadata_header['meta_codec']]
        metadata = metadata_codec_impl.decompress(metadata)
    print_verbose("read %s metadata of size: '%s'" %
            # FIXME meta_codec?
            ('compressed' if metadata_header['meta_codec'] != 'None' else
                'uncompressed', metadata_header['meta_comp_size']))
    serializer_impl = SERIZLIALIZERS_LOOKUP[metadata_header['magic_format']]
    metadata = serializer_impl.loads(metadata)
    return metadata, metadata_header


def _read_offsets(input_fp, bloscpack_header):
    """ Read the offsets from a file pointer.

    Parameters
    ----------
    input_fp : file like
        a file pointer to read from

    Returns
    -------
    offsets : list of int
        the offsets

    Notes
    -----
    The 'input_fp' should point to the position where the offsets start. Any
    unused offsets will not be returned.

    """
    if bloscpack_header.offsets:
        total_entries = bloscpack_header.nchunks + \
                bloscpack_header.max_app_chunks
        offsets_raw = input_fp.read(8 * total_entries)
        print_debug('Read raw offsets: %s' % repr(offsets_raw))
        offsets = [decode_int64(offsets_raw[j - 8:j]) for j in
                xrange(8, bloscpack_header.nchunks * 8 + 1, 8)]
        print_debug('Offsets: %s' % offsets)
        return offsets
    else:
        return []


def _read_beginning(input_fp):
    """ Read the bloscpack_header, metadata, metadata_header and offsets.

    Parameters
    ----------
    input_fp : file like
        input file pointer

    Returns
    -------
    bloscpack_header : dict
    metadata : object
    metadata_header : dict
    offsets : list of ints

    """
    bloscpack_header = _read_bloscpack_header(input_fp)
    metadata, metadata_header = _read_metadata(input_fp) \
            if bloscpack_header.metadata\
            else (None, None)
    offsets = _read_offsets(input_fp, bloscpack_header)
    return bloscpack_header, metadata, metadata_header, offsets


def _write_offsets(output_fp, offsets):
    print_debug("Writing '%d' offsets: '%s'" %
            (len(offsets), repr(offsets)))
    # write the offsets encoded into the reserved space in the file
    encoded_offsets = "".join([encode_int64(i) for i in offsets])
    print_debug("Raw offsets: %s" % repr(encoded_offsets))
    output_fp.write(encoded_offsets)


def _read_compressed_chunk_fp(input_fp, checksum_impl):
    """ Read a compressed chunk from a file pointer.

    Parameters
    ----------
    input_fp : file like
        the file pointer to read the chunk from
    checksum_impl : Checksum
        the checksum that has been used

    Returns
    -------
    compressed : str
        the compressed data
    blosc_header : dict
        the blosc header from the chunk
    """
    # read blosc header
    blosc_header_raw = input_fp.read(BLOSC_HEADER_LENGTH)
    blosc_header = decode_blosc_header(blosc_header_raw)
    if log.LEVEL == log.DEBUG:
        print_debug('blosc_header: %s' % repr(blosc_header))
    ctbytes = blosc_header['ctbytes']
    # Seek back BLOSC_HEADER_LENGTH bytes in file relative to current
    # position. Blosc needs the header too and presumably this is
    # better than to read the whole buffer and then concatenate it...
    input_fp.seek(-BLOSC_HEADER_LENGTH, 1)
    # read chunk
    compressed = input_fp.read(ctbytes)
    if checksum_impl.size > 0:
        # do checksum
        expected_digest = input_fp.read(checksum_impl.size)
        received_digest = checksum_impl(compressed)
        if received_digest != expected_digest:
            raise ChecksumMismatch(
                    "Checksum mismatch detected in chunk, "
                    "expected: '%s', received: '%s'" %
                    (repr(expected_digest), repr(received_digest)))
        else:
            print_debug('checksum OK (%s): %s ' %
                    (checksum_impl.name, repr(received_digest)))
    return compressed, blosc_header


def unpack_file(in_file, out_file):
    """ Main function for decompressing a file.

    Parameters
    ----------
    in_file : str
        the name of the input file
    out_file : str
        the name of the output file

    Returns
    -------
    metadata : str
        the metadata contained in the file if present

    Raises
    ------

    FormatVersionMismatch
        if the file has an unmatching format version number
    ChecksumMismatch
        if any of the chunks fail to produce the correct checksum
    """
    in_file_size = path.getsize(in_file)
    print_verbose('input file size: %s' % pretty_size(in_file_size))
    with open_two_file(open(in_file, 'rb'), open(out_file, 'wb')) as \
            (input_fp, output_fp):
        source = CompressedFPSource(input_fp)
        sink = PlainFPSink(output_fp, source.nchunks)
        metadata = unpack(source, sink)
    out_file_size = path.getsize(out_file)
    print_verbose('output file size: %s' % pretty_size(out_file_size))
    print_verbose('decompression ratio: %f' % (out_file_size / in_file_size))
    return metadata


def unpack(source, sink):
    # read, decompress, write loop
    for compressed in iter(source):
        sink.put(compressed)
    return source.metadata


def _seek_to_metadata(target_fp):
    """ Given a target file pointer, seek to the metadata section.

    Parameters
    ----------

    target_fp : file like
        the target file pointer

    Returns
    -------
    metadata_position : int

    Raises
    ------
    NoMetadataFound
        if there is no metadata section in this file

    """
    bloscpack_header = _read_bloscpack_header(target_fp)
    if not bloscpack_header.metadata:
        raise NoMetadataFound("unable to seek to metadata if it does not exist")
    else:
        return target_fp.tell()


def _rewrite_metadata_fp(target_fp, new_metadata,
            magic_format=None, checksum=None,
            codec=DEFAULT_META_CODEC, level=DEFAULT_META_LEVEL):
    """ Rewrite the metadata section in a file pointer.

    Parameters
    ----------
    target_fp : file like
        the target file pointer to rewrite in
    new_metadata: dict
        the new metadata to save

    See the notes in ``_recreate_metadata`` for a description of the keyword
    arguments.

    """
    # cache the current position
    current_pos = target_fp.tell()
    # read the metadata section
    old_metadata, old_metadata_header = _read_metadata(target_fp)
    if old_metadata == new_metadata:
        raise NoChangeInMetadata(
                'you requested to update metadata, but this has not changed')
    new_metadata_args = _recreate_metadata(old_metadata_header, new_metadata,
            magic_format=magic_format, checksum=checksum,
            codec=codec, level=level)
    # seek back to where the metadata begins...
    target_fp.seek(current_pos, 0)
    # and re-write it
    _write_metadata(target_fp, new_metadata, new_metadata_args)


def _recreate_metadata(old_metadata_header, new_metadata,
            magic_format=None, checksum=None,
            codec=DEFAULT_META_CODEC, level=DEFAULT_META_LEVEL):
    """ Update the metadata section.

    Parameters
    ----------
    old_metadata_header: dict
        the header of the old metadata
    new_metadata: dict
        the new metadata to save

    See the notes below for a description of the keyword arguments.

    Returns
    -------
    new_metadata_args: dict
        the new arguments for ``_write_metadata``

    Raises
    ------
    ChecksumLengthMismatch
        if the new checksum has a different length than the old one
    NoChangeInMetadata
        if the metadata has not changed

    Notes
    -----
    This create new ``metadata_args`` based on an old metadata_header, Since
    the space has already been allocated, only certain metadata arguments can
    be overridden. The keyword arguments specify which ones these are. If a
    keyword argument value is 'None' the existing argument which is obtained
    from the header is used.  Otherwise the value from the keyword argument
    takes precedence. Due to a policy of opportunistic compression, the 'codec'
    and 'level' arguments are not 'None' by default, to ensure that previously
    uncompressed metadata, which might be favourably compressible as a result
    of the enlargement process, will actually be compressed. As for the
    'checksum' only a checksum with the same digest size can be used.

    The ``metadata_args`` returned by this function are suitable to be passed
    on to ``_write_metadata``.

    """
    # get the settings from the metadata header
    metadata_args = dict((k, old_metadata_header[k]) for k in METADATA_ARGS)
    # handle and check validity of overrides
    if magic_format is not None:
        check_valid_serializer(magic_format)
        metadata_args['magic_format'] = magic_format
    if checksum is not None:
        check_valid_checksum(checksum)
        old_impl = CHECKSUMS_LOOKUP[old_metadata_header['meta_checksum']]
        new_impl = CHECKSUMS_LOOKUP[checksum]
        if old_impl.size != new_impl.size:
            raise ChecksumLengthMismatch(
                    'checksums have a size mismatch')
        metadata_args['meta_checksum'] = checksum
    if codec is not None:
        check_valid_codec(codec)
        metadata_args['meta_codec'] = codec
    if level is not None:
        check_range('meta_level', level, 0, MAX_CLEVEL)
        metadata_args['meta_level'] = level
    return metadata_args


def append_fp(original_fp, new_content_fp, new_size, blosc_args=None):
    """ Append from a file pointer to a file pointer.

    Parameters
    ----------
    original : file_like
        the original file_pointer
    new_content : str
        the file pointer for the data to be appended
    new_size : int
        the size of the new_content
    blosc_args : dict
        the blosc_args

    Returns
    -------
    nchunks_written : int
        the total number of new chunks written to the file

    Notes
    -----
    The blosc_args argument can be supplied if different blosc arguments are
    desired.

    """
    bloscpack_header, metadata, metadata_header, offsets = \
        _read_beginning(original_fp)
    checksum_impl = bloscpack_header.checksum_impl
    if not offsets:
        raise RuntimeError(
                'Appending to a file without offsets is not yet supported')
    if blosc_args is None:
        blosc_args = dict(zip(BLOSC_ARGS, [None] * len(BLOSC_ARGS)))
    # handle blosc_args
    if blosc_args['typesize'] is None:
        if bloscpack_header.typesize == -1:
            raise NonUniformTypesize(
                    "Non uniform type size, can not append to file.")
        else:
            # use the typesize from the bloscpack header
            blosc_args['typesize'] = bloscpack_header.typesize
    if blosc_args['clevel'] is None:
        # use the default
        blosc_args['clevel'] = DEFAULT_CLEVEL
    if blosc_args['shuffle'] is None:
        blosc_args['shuffle'] = DEFAULT_SHUFFLE
    if blosc_args['cname'] is None:
        blosc_args['cname'] = DEFAULT_CNAME
    _check_blosc_args(blosc_args)
    offsets_pos = (BLOSCPACK_HEADER_LENGTH +
                  (METADATA_HEADER_LENGTH + metadata_header['max_meta_size'] +
                      CHECKSUMS_LOOKUP[metadata_header['meta_checksum']].size
                   if metadata is not None else 0))
    # seek to the final offset
    original_fp.seek(offsets[-1], 0)
    # decompress the last chunk
    compressed, blosc_header = _read_compressed_chunk_fp(original_fp, checksum_impl)
    decompressed = blosc.decompress(compressed)
    # figure out how many bytes we need to read to rebuild the last chunk
    ultimo_length = len(decompressed)
    bytes_to_read = bloscpack_header.chunk_size - ultimo_length
    if new_size <= bytes_to_read:
        # special case
        # must squeeze data into last chunk
        fill_up = new_content_fp.read(new_size)
        # seek back to the position of the original last chunk
        original_fp.seek(offsets[-1], 0)
        # write the chunk that has been filled up
        compressed = _compress_chunk_str(decompressed + fill_up, blosc_args)
        digest = checksum_impl(compressed)
        _write_compressed_chunk(original_fp, compressed, digest)
        # return 0 to indicate that no new chunks have been written
        # build the new header
        bloscpack_header.last_chunk += new_size
        # create the new header
        raw_bloscpack_header = bloscpack_header.encode()
        original_fp.seek(0)
        original_fp.write(raw_bloscpack_header)
        return 0

    # figure out what is left over
    new_new_size = new_size - bytes_to_read
    # read those bytes
    fill_up = new_content_fp.read(bytes_to_read)
    # figure out how many chunks we will need
    nchunks, chunk_size, last_chunk_size = \
            calculate_nchunks(new_new_size,
                chunk_size=bloscpack_header.chunk_size)
    # make sure that we actually have that kind of space
    if nchunks > bloscpack_header.max_app_chunks:
        raise NotEnoughSpace('not enough space')
    # seek back to the position of the original last chunk
    original_fp.seek(offsets[-1], 0)
    # write the chunk that has been filled up
    compressed = _compress_chunk_str(decompressed + fill_up, blosc_args)
    digest = checksum_impl(compressed)
    _write_compressed_chunk(original_fp, compressed, digest)
    # append to the original file, again original_fp should be adequately
    # positioned
    sink = CompressedFPSink(original_fp)
    sink.configure(blosc_args, bloscpack_header)
    # allocate new offsets
    sink.offset_storage = list(itertools.repeat(-1, nchunks))
    # read from the new input file, new_content_fp should be adequately
    # positioned
    source = PlainFPSource(new_content_fp)
    source.configure(chunk_size, last_chunk_size, nchunks)
    # read, compress, write loop
    for i, chunk in enumerate(source()):
        print_debug("Handle chunk '%d' %s" % (i,'(last)' if i == nchunks -1
            else ''))

        compressed = _compress_chunk_str(chunk, blosc_args)
        sink.put(i, compressed)

    # build the new header
    bloscpack_header.last_chunk = last_chunk_size
    bloscpack_header.nchunks += nchunks
    bloscpack_header.max_app_chunks -= nchunks
    # create the new header
    raw_bloscpack_header = bloscpack_header.encode()
    original_fp.seek(0)
    original_fp.write(raw_bloscpack_header)
    # write the new offsets, but only those that changed
    original_fp.seek(offsets_pos)
    # FIXME: write only those that changed
    _write_offsets(sink.output_fp, offsets + sink.offset_storage)
    return nchunks


def append(orig_file, new_file, blosc_args=None):
    """ Append from a file pointer to a file pointer.

    Parameters
    ----------
    orig_file : str
        the name of the file to append to
    new_file : str
        the name of the file to append from
    blosc_args : dict
        the blosc_args

    Notes
    -----
    The blosc_args argument can be supplied if different blosc arguments are
    desired.
    """
    orig_size_before = path.getsize(orig_file)
    new_size = path.getsize(new_file)
    print_verbose('orig file size before append: %s' %
            double_pretty_size(orig_size_before))
    print_verbose('new file size: %s' % double_pretty_size(new_size))

    with open_two_file(open(orig_file, 'r+b'), open(new_file, 'rb')) as \
            (orig_fp, new_fp):
        append_fp(orig_fp, new_fp, new_size, blosc_args)
    orig_size_after = path.getsize(orig_file)
    print_verbose('orig file size after append: %s' %
            double_pretty_size(orig_size_after))
    print_verbose('Approximate compression ratio of appended data: %f' %
            ((orig_size_after-orig_size_before)/new_size))

if __name__ == '__main__':
    parser = create_parser()
    PREFIX = parser.prog
    args = parser.parse_args()
    if args.verbose:
        log.LEVEL = log.VERBOSE
    elif args.debug:
        log.LEVEL = log.DEBUG
    print_debug('command line argument parsing complete')
    print_debug('command line arguments are: ')
    for arg, val in vars(args).iteritems():
        print_debug('\t%s: %s' % (arg, str(val)))
    process_nthread_arg(args)

    # compression and decompression handled via subparsers
    if args.subcommand in ['compress', 'c']:
        print_verbose('getting ready for compression')
        in_file, out_file, blosc_args = process_compression_args(args)
        try:
            check_files(in_file, out_file, args)
        except FileNotFound as fnf:
            error(str(fnf))
        metadata = process_metadata_args(args)
        bloscpack_args = DEFAULT_BLOSCPACK_ARGS.copy()
        bloscpack_args['offsets'] = args.offsets
        bloscpack_args['checksum'] = args.checksum
        try:
            pack_file(in_file, out_file, chunk_size=args.chunk_size,
                    metadata=metadata,
                    blosc_args=blosc_args,
                    bloscpack_args=bloscpack_args,
                    metadata_args=DEFAULT_METADATA_ARGS)
        except ChunkingException as ce:
            error(str(ce))
    elif args.subcommand in ['decompress', 'd']:
        print_verbose('getting ready for decompression')
        in_file, out_file = process_decompression_args(args)
        try:
            check_files(in_file, out_file, args)
        except FileNotFound as fnf:
            error(str(fnf))
        try:
            metadata = unpack_file(in_file, out_file)
            if metadata:
                print_verbose("Metadata is:\n'%s'" % metadata)
        except FormatVersionMismatch as fvm:
            error(fvm.message)
        except ChecksumMismatch as csm:
            error(csm.message)
    elif args.subcommand in ['append', 'a']:
        print_verbose('getting ready for append')
        original_file, new_file = process_append_args(args)
        try:
            if not path.exists(original_file):
                raise FileNotFound("original file '%s' does not exist!" %
                        original_file)
            if not path.exists(new_file):
                raise FileNotFound("new file '%s' does not exist!" %
                        new_file)
        except FileNotFound as fnf:
            error(str(fnf))
        print_verbose("original file is: '%s'" % original_file)
        print_verbose("new file is: '%s'" % new_file)
        blosc_args = _blosc_args_from_args(args)
        metadata = process_metadata_args(args)
        append(original_file, new_file, blosc_args=blosc_args)
        if metadata is not None:
            with open(original_file, 'r+b') as fp:
                _seek_to_metadata(fp)
                _rewrite_metadata_fp(fp, metadata)
    elif args.subcommand in ('info', 'i'):
        try:
            if not path.exists(args.file_):
                raise FileNotFound("file '%s' does not exist!" %
                        args.file_)
        except FileNotFound as fnf:
            error(str(fnf))
        try:
            with open(args.file_) as fp:
                bloscpack_header, metadata, metadata_header, offsets = \
                        _read_beginning(fp)
        except ValueError as ve:
            error(str(ve) + "\n" +
            "This might not be a bloscpack compressed file.")
        print_normal(bloscpack_header.pformat())
        if metadata is not None:
            print_normal("'metadata':")
            print_normal(pprint.pformat(metadata, indent=4))
            print_normal("'metadata_header':")
            print_normal(pprint.pformat(metadata_header, indent=4))
        if offsets:
            print_normal("'offsets':")
            print_normal("[%s,...]" % (",".join(str(o) for o in offsets[:5])))

    else:  # pragma: no cover
        # we should never reach this
        error('You found the easter-egg, please contact the author')
    print_verbose('done')
