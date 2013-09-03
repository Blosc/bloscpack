#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

""" Command line interface to Blosc via python-blosc """

from __future__ import division

import abc
import argparse
import contextlib
import collections
import cStringIO
import copy
import hashlib
import json
import itertools
import os
import os.path as path
import pprint
import struct
import sys
import zlib
try:
    from collections import OrderedDict
except ImportError:  # pragma: no cover
    from ordereddict import OrderedDict
import blosc
import numpy as np

__version__ = '0.4.0-rc2'
__author__ = 'Valentin Haenel <valentin.haenel@gmx.de>'

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

# Bloscpack args
BLOSCPACK_ARGS = ('offsets', 'checksum', 'max_app_chunks')
_BLOSCPACK_ARGS_SET = set(BLOSCPACK_ARGS)  # cached
DEFAULT_OFFSETS = True
DEFAULT_CHECKSUM = 'adler32'
DEFAULT_MAX_APP_CHUNKS = lambda x: 10 * x
DEFAULT_BLOSCPACK_ARGS = dict(zip(BLOSCPACK_ARGS,
    (DEFAULT_OFFSETS, DEFAULT_CHECKSUM, DEFAULT_MAX_APP_CHUNKS)))

DEFAULT_CHUNK_SIZE = '1M'

# Blosc args
BLOSC_ARGS = ('typesize', 'clevel', 'shuffle')
_BLOSC_ARGS_SET = set(BLOSC_ARGS)  # cached
DEFAULT_TYPESIZE = 8
DEFAULT_CLEVEL = 7
MAX_CLEVEL = 9
DEFAULT_SHUFFLE = True
DEFAULT_BLOSC_ARGS = dict(zip(BLOSC_ARGS,
    (DEFAULT_TYPESIZE, DEFAULT_CLEVEL, DEFAULT_SHUFFLE)))

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

# verbosity levels
NORMAL  = 'NORMAL'
VERBOSE = 'VERBOSE'
DEBUG   = 'DEBUG'
LEVEL = NORMAL
VERBOSITY_LEVELS = (NORMAL, VERBOSE, DEBUG)

# lookup table for human readable sizes
SUFFIXES = OrderedDict((
             ("B", 2**0 ),
             ("K", 2**10),
             ("M", 2**20),
             ("G", 2**30),
             ("T", 2**40)))


class ChunkingException(BaseException):
    pass


class NoSuchChecksum(ValueError):
    pass


class NoSuchCodec(ValueError):
    pass


class NoSuchSerializer(ValueError):
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


class FileNotFound(IOError):
    pass


class NonUniformTypesize(RuntimeError):
    pass


class NotEnoughSpace(RuntimeError):
    pass


class NotANumpyArray(RuntimeError):
    pass


class Hash(object):
    """ Uniform hash object.

    Parameters
    ----------
    name : str
        the name of the hash
    size : int
        the length of the digest in bytes
    function : callable
        the hash function implementation

    Notes
    -----
    The 'function' argument should return the raw bytes as string.

    """

    def __init__(self, name, size, function):
        self.name, self.size, self._function = name, size, function

    def __call__(self, data):
        return self._function(data)


def zlib_hash(func):
    """ Wrapper for zlib hashes. """
    def hash_(data):
        # The binary OR is recommended to obtain uniform hashes on all python
        # versions and platforms. The type with be 'uint32'.
        return struct.pack('<I', func(data) & 0xffffffff)
    return 4, hash_


def hashlib_hash(func):
    """ Wrapper for hashlib hashes. """
    def hash_(data):
        return func(data).digest()
    return func().digest_size, hash_


CHECKSUMS = [Hash('None', 0, lambda data: ''),
             Hash('adler32', *zlib_hash(zlib.adler32)),
             Hash('crc32', *zlib_hash(zlib.crc32)),
             Hash('md5', *hashlib_hash(hashlib.md5)),
             Hash('sha1', *hashlib_hash(hashlib.sha1)),
             Hash('sha224', *hashlib_hash(hashlib.sha224)),
             Hash('sha256', *hashlib_hash(hashlib.sha256)),
             Hash('sha384', *hashlib_hash(hashlib.sha384)),
             Hash('sha512', *hashlib_hash(hashlib.sha512)),
             ]
CHECKSUMS_AVAIL = [c.name for c in CHECKSUMS]
CHECKSUMS_LOOKUP = dict(((c.name, c) for c in CHECKSUMS))


def _check_valid_checksum(checksum):
    """ Check the validity of a checksum.

    Parameters
    ----------
    checksum : str
        the string descriptor of the checksum

    Raises
    ------
    NoSuchChecksum
        if no such checksum exists.
    """
    if checksum not in CHECKSUMS_AVAIL:
        raise NoSuchChecksum("checksum '%s' does not exist" % checksum)


class Codec(object):
    """ Uniform codec object.

    Parameters
    ----------
    name : str
        the name of the codec
    compress : callable
        a compression function taking data and level as args
    decompress : callable
        a decompression function taking data as arg

    """

    def __init__(self, name, compress, decompress):
        self.name = name
        self._compress = compress
        self._decompress = decompress

    def compress(self, data, level):
        return self._compress(data, level)

    def decompress(self, data):
        return self._decompress(data)

CODECS = [Codec('None', lambda data, level: data, lambda data: data),
          Codec('zlib', zlib.compress, zlib.decompress)]
CODECS_AVAIL = [c.name for c in CODECS]
CODECS_LOOKUP = dict(((c.name, c) for c in CODECS))


def _check_valid_codec(codec):
    """ Check the validity of a codec.

    Parameters
    ----------
    codec : str
        the string descriptor of the codec

    Raises
    ------
    NoSuchCodec
        if no such checksum exists.
    """
    if codec not in CODECS_AVAIL:
        raise NoSuchCodec("codec '%s' does not exist" % codec)


class Serializer(object):
    """ Uniform serializer object.

    Parameters
    ----------
    name : str
        the name of the serializer
    compress : callable
        a compression function taking a dict as arg
    decompress : callable
        a decompression function taking serialized data as arg

    """
    def __init__(self, name, dumps, loads):
        self.name = name
        self._loads = loads
        self._dumps = dumps

    def dumps(self, dict_):
        return self._dumps(dict_)

    def loads(self, data):
        return self._loads(data)


SERIZLIALIZERS = [Serializer('JSON',
                  lambda x: json.dumps(x, separators=(',', ':')),
                  lambda x: json.loads(x))]
SERIZLIALIZERS_AVAIL = [s.name for s in SERIZLIALIZERS]
SERIZLIALIZERS_LOOKUP = dict(((s.name, s) for s in SERIZLIALIZERS))


def _check_valid_serializer(serializer):
    """ Check the validity of a serializer.

    Parameters
    ----------
    serializer : str
        the magic format of the serializer

    Raises
    ------
    NoSuchSerializer
        if no such serializer exists.
    """
    if serializer not in SERIZLIALIZERS_AVAIL:
        raise NoSuchSerializer("serializer '%s' does not exist" % serializer)


def print_verbose(message, level=VERBOSE):
    """ Print message with desired verbosity level. """
    if level not in VERBOSITY_LEVELS:
        raise TypeError("Desired level '%s' is not one of %s" % (level,
                        str(VERBOSITY_LEVELS)))
    if VERBOSITY_LEVELS.index(level) <= VERBOSITY_LEVELS.index(LEVEL):
        for line in [l for l in message.split('\n') if l != '']:
            print('%s: %s' % (PREFIX, line))


def print_debug(message):
    """ Print message with verbosity level ``DEBUG``. """
    print_verbose(message, level=DEBUG)


def print_normal(message):
    """ Print message with verbosity level ``NORMAL``. """
    print_verbose(message, level=NORMAL)


def error(message, exit_code=1):
    """ Print message and exit with desired code. """
    for line in [l for l in message.split('\n') if l != '']:
        print('%s: error: %s' % (PREFIX, line))
    sys.exit(exit_code)


def pretty_size(size_in_bytes):
    """ Pretty print filesize.  """
    if size_in_bytes == 0:
        return "0B"
    for suf, lim in reversed(sorted(SUFFIXES.items(), key=lambda x: x[1])):
        if size_in_bytes < lim:
            continue
        else:
            return str(round(size_in_bytes/lim, 2))+suf


def double_pretty_size(size_in_bytes):
    """ Pretty print filesize including size in bytes. """
    return ("%s (%dB)" % (pretty_size(size_in_bytes), size_in_bytes))


def reverse_pretty(readable):
    """ Reverse pretty printed file size. """
    # otherwise we assume it has a suffix
    suffix = readable[-1]
    if suffix not in SUFFIXES.keys():
        raise ValueError(
                "'%s' is not a valid prefix multiplier, use one of: '%s'" %
                (suffix, SUFFIXES.keys()))
    else:
        return int(float(readable[:-1]) * SUFFIXES[suffix])


def drop_caches():  # pragma: no cover
    if os.geteuid() == 0:
        os.system('echo 3 > /proc/sys/vm/drop_caches')
    else:
        raise RuntimeError('Need root permission to drop caches')


def sync():
    os.system('sync')


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


class BloscPackCustomFormatter(argparse.HelpFormatter):
    """ Custom HelpFormatter.

    Basically a combination and extension of ArgumentDefaultsHelpFormatter and
    RawTextHelpFormatter. Adds default values to argument help, but only if the
    default is not in [None, True, False]. Also retains all whitespace as it
    is.

    """
    def _get_help_string(self, action):
        help_ = action.help
        if '%(default)' not in action.help \
                and action.default not in \
                [argparse.SUPPRESS, None, True, False]:
            defaulting_nargs = [argparse.OPTIONAL, argparse.ZERO_OR_MORE]
            if action.option_strings or action.nargs in defaulting_nargs:
                help_ += ' (default: %(default)s)'
        return help_

    def _split_lines(self, text, width):
        return text.splitlines()


def _inject_blosc_group(parser):
    blosc_group = parser.add_argument_group(title='blosc settings')
    blosc_group.add_argument('-t', '--typesize',
            metavar='<size>',
            default=DEFAULT_TYPESIZE,
            type=int,
            help='typesize for blosc')
    blosc_group.add_argument('-l', '--clevel',
            default=DEFAULT_CLEVEL,
            choices=range(10),
            metavar='[0, 9]',
            type=int,
            help='compression level')
    blosc_group.add_argument('-s', '--no-shuffle',
            action='store_false',
            default=DEFAULT_SHUFFLE,
            dest='shuffle',
            help='deactivate shuffle')


def create_parser():
    """ Create and return the parser. """
    parser = argparse.ArgumentParser(
            #usage='%(prog)s [GLOBAL_OPTIONS] (compress | decompress)
            # [COMMAND_OPTIONS] <in_file> [<out_file>]',
            description='command line de/compression with blosc',
            formatter_class=BloscPackCustomFormatter)
    ## print version of bloscpack, python-blosc and blosc itself
    parser.add_argument('--version',
            action='version',
            version='%(prog)s:\t' + ("'%s'\n" % __version__) +
                    "python-blosc:\t'%s'\n"   % blosc.version.__version__ +
                    "blosc:\t\t'%s'\n"        % blosc.BLOSC_VERSION_STRING)
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument('-v', '--verbose',
            action='store_true',
            default=False,
            help='be verbose about actions')
    output_group.add_argument('-d', '--debug',
            action='store_true',
            default=False,
            help='print debugging output too')
    global_group = parser.add_argument_group(title='global options')
    global_group.add_argument('-f', '--force',
            action='store_true',
            default=False,
            help='disable overwrite checks for existing files\n' +
            '(use with caution)')
    class CheckThreadOption(argparse.Action):
        def __call__(self, parser, namespace, value, option_string=None):
            if not 1 <= value <= blosc.BLOSC_MAX_THREADS:
                error('%s must be 1 <= n <= %d'
                        % (option_string, blosc.BLOSC_MAX_THREADS))
            setattr(namespace, self.dest, value)
    global_group.add_argument('-n', '--nthreads',
            metavar='[1, %d]' % blosc.BLOSC_MAX_THREADS,
            action=CheckThreadOption,
            default=blosc.ncores,
            type=int,
            dest='nthreads',
            help='set number of threads, (default: %(default)s (ncores))')

    subparsers = parser.add_subparsers(title='subcommands',
            metavar='', dest='subcommand')

    compress_parser = subparsers.add_parser('compress',
            formatter_class=BloscPackCustomFormatter,
            help='perform compression on file')
    c_parser = subparsers.add_parser('c',
            formatter_class=BloscPackCustomFormatter,
            help="alias for 'compress'")

    class CheckChunkSizeOption(argparse.Action):
        def __call__(self, parser, namespace, value, option_string=None):
            if value == 'max':
                value = blosc.BLOSC_MAX_BUFFERSIZE
            else:
                try:
                    # try to get the value as bytes
                    if value[-1] in SUFFIXES.keys():
                        value = reverse_pretty(value)
                    # seems to be intended to be a naked int
                    else:
                        value = int(value)
                except ValueError as ve:
                    error('%s error: %s' % (option_string, str(ve) +
                        " or 'max'"))
                if value < 0:
                    error('%s must be > 0 ' % option_string)
            setattr(namespace, self.dest, value)
    for p in [compress_parser, c_parser]:
        _inject_blosc_group(p)
        bloscpack_chunking_group = p.add_mutually_exclusive_group()
        bloscpack_chunking_group.add_argument('-z', '--chunk-size',
                metavar='<size>',
                action=CheckChunkSizeOption,
                type=str,
                default=DEFAULT_CHUNK_SIZE,
                dest='chunk_size',
                help="set desired chunk size or 'max'")
        bloscpack_group = p.add_argument_group(title='bloscpack settings')
        def join_with_eol(items):
            return ', '.join(items) + '\n'
        checksum_format = join_with_eol(CHECKSUMS_AVAIL[0:3]) + \
                join_with_eol(CHECKSUMS_AVAIL[3:6]) + \
                join_with_eol(CHECKSUMS_AVAIL[6:])
        checksum_help = 'set desired checksum:\n' + checksum_format
        bloscpack_group.add_argument('-k', '--checksum',
                metavar='<checksum>',
                type=str,
                choices=CHECKSUMS_AVAIL,
                default=DEFAULT_CHECKSUM,
                dest='checksum',
                help=checksum_help)
        bloscpack_group.add_argument('-o', '--no-offsets',
                action='store_false',
                default=DEFAULT_OFFSETS,
                dest='offsets',
                help='deactivate offsets')
        bloscpack_group.add_argument('-m', '--metadata',
                metavar='<metadata>',
                type=str,
                dest='metadata',
                help="file containing the metadata, must contain valid JSON")

    decompress_parser = subparsers.add_parser('decompress',
            formatter_class=BloscPackCustomFormatter,
            help='perform decompression on file')

    d_parser = subparsers.add_parser('d',
            formatter_class=BloscPackCustomFormatter,
            help="alias for 'decompress'")

    for p in [decompress_parser, d_parser]:
        p.add_argument('-e', '--no-check-extension',
                action='store_true',
                default=False,
                dest='no_check_extension',
                help='disable checking input file for extension (*.blp)\n' +
                '(requires use of <out_file>)')

    for p, help_in, help_out in [(compress_parser,
            'file to be compressed', 'file to compress to'),
                                 (c_parser,
            'file to be compressed', 'file to compress to'),
                                 (decompress_parser,
            'file to be decompressed', 'file to decompress to'),
                                 (d_parser,
            'file to be decompressed', 'file to decompress to'),
                                  ]:
        p.add_argument('in_file',
                metavar='<in_file>',
                type=str,
                default=None,
                help=help_in)
        p.add_argument('out_file',
                metavar='<out_file>',
                type=str,
                nargs='?',
                default=None,
                help=help_out)

    append_parser = subparsers.add_parser('append',
            formatter_class=BloscPackCustomFormatter,
            help='append data to a compressed file')

    a_parser = subparsers.add_parser('a',
            formatter_class=BloscPackCustomFormatter,
            help="alias for 'append'")

    for p in (append_parser, a_parser):
        _inject_blosc_group(p)
        p.add_argument('original_file',
                metavar='<original_file>',
                type=str,
                help="file to append to")
        p.add_argument('new_file',
                metavar='<new_file>',
                type=str,
                help="file to append from")
        p.add_argument('-e', '--no-check-extension',
                action='store_true',
                default=False,
                dest='no_check_extension',
                help='disable checking original file for extension (*.blp)\n')
        p.add_argument('-m', '--metadata',
                metavar='<metadata>',
                type=str,
                dest='metadata',
                help="file containing the metadata, must contain valid JSON")


    info_parser = subparsers.add_parser('info',
            formatter_class=BloscPackCustomFormatter,
            help='print information about a compressed file')

    i_parser = subparsers.add_parser('i',
            formatter_class=BloscPackCustomFormatter,
            help="alias for 'info'")

    for p in (info_parser, i_parser):
        p.add_argument('file_',
                metavar='<file>',
                type=str,
                default=None,
                help="file to show info for")
    return parser


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
    print_verbose('nchunks: %d' % nchunks, level=VERBOSE)
    print_verbose('chunk_size: %s' % double_pretty_size(chunk_size),
            level=VERBOSE)
    print_verbose('last_chunk_size: %s' % double_pretty_size(last_chunk_size),
            level=DEBUG)
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


class BloscPackHeader(collections.MutableMapping):
    """ The Bloscpack header.

    Parameters
    ----------
    format_version : int
        the version format for the compressed file
    offsets: bool
        if the offsets to the chunks are present
    metadata: bool
        if the metadata is present
    checksum : str
        the checksum to be used
    typesize : int
        the typesize used for blosc in the chunks
    chunk_size : int
        the size of a regular chunk
    last_chunk : int
        the size of the last chunk
    nchunks : int
        the number of chunks
    max_app_chunks : int
        the total number of possible append chunks

    Notes
    -----
    See the README distributed for details on the header format.

    Raises
    ------
    ValueError
        if any of the arguments have an invalid value
    TypeError
        if any of the arguments have the wrong type
    """
    def __init__(self,
                 format_version=FORMAT_VERSION,
                 offsets=False,
                 metadata=False,
                 checksum='None',
                 typesize=0,
                 chunk_size=-1,
                 last_chunk=-1,
                 nchunks=-1,
                 max_app_chunks=0):

        check_range('format_version', format_version, 0, MAX_FORMAT_VERSION)
        _check_valid_checksum(checksum)
        check_range('typesize',   typesize,    0, blosc.BLOSC_MAX_TYPESIZE)
        check_range('chunk_size', chunk_size, -1, blosc.BLOSC_MAX_BUFFERSIZE)
        check_range('last_chunk', last_chunk, -1, blosc.BLOSC_MAX_BUFFERSIZE)
        check_range('nchunks',    nchunks,    -1, MAX_CHUNKS)
        check_range('max_app_chunks', max_app_chunks, 0, MAX_CHUNKS)
        if nchunks != -1:
            check_range('nchunks + max_app_chunks',
                nchunks + max_app_chunks, 0, MAX_CHUNKS)
        elif max_app_chunks != 0:
            raise ValueError("'max_app_chunks' can not be non '0' if 'nchunks' is '-1'")
        if chunk_size != -1 and last_chunk != -1 and last_chunk > chunk_size:
            raise ValueError("'last_chunk' (%d) is larger than 'chunk_size' (%d)"
                    % (last_chunk, chunk_size))

        self._attrs = ['format_version',
                       'offsets',
                       'metadata',
                       'checksum',
                       'typesize',
                       'chunk_size',
                       'last_chunk',
                       'nchunks',
                       'max_app_chunks']
        self._len = len(self._attrs)
        self._bytes_attrs = ['chunk_size',
                             'last_chunk']

        self.format_version  = format_version
        self.offsets         = offsets
        self.metadata        = metadata
        self.checksum        = checksum
        self.typesize        = typesize
        self.chunk_size      = chunk_size
        self.last_chunk      = last_chunk
        self.nchunks         = nchunks
        self.max_app_chunks  = max_app_chunks

    def __getitem__(self, key):
        if key not in self._attrs:
            raise KeyError('%s not in BloscPackHeader' % key)
        return getattr(self, key)

    def __setitem__(self, key, value):
        if key not in self._attrs:
            raise KeyError('%s not in BloscPackHeader' % key)
        setattr(self, key, value)

    def __delitem__(self, key):
        raise NotImplementedError(
            'BloscPackHeader does not support __delitem__ or derivatives')

    def __len__(self):
        return self._len

    def __iter__(self):
        return iter(self._attrs)

    def __str__(self):
        return pprint.pformat(dict(self))

    def __repr__(self):
        return "BloscPackHeader(%s)" % ", ".join((("%s=%s" % (arg, repr(value)))
                          for arg, value in self.iteritems()))

    def pformat(self, indent=4):
        indent = " " * indent
        # don't ask, was feeling functional
        return "bloscpack header: \n%s%s" % (indent, (",\n%s" % indent).join((("%s=%s" % 
            (key, (repr(value) if (key not in self._bytes_attrs or value == -1)
                         else double_pretty_size(value)))
             for key, value in self.iteritems()))))

    def copy(self):
        return copy.copy(self)

    @property
    def checksum_impl(self):
        return CHECKSUMS_LOOKUP[self.checksum]

    def encode(self):
        """ Encode the Bloscpack header.

        Returns
        -------

        raw_bloscpack_header : string
            the header as string of bytes
        """
        format_version = encode_uint8(self.format_version)
        options = encode_uint8(int(
            create_options(offsets=self.offsets, metadata=self.metadata),
            2))
        checksum = encode_uint8(CHECKSUMS_AVAIL.index(self.checksum))
        typesize = encode_uint8(self.typesize)
        chunk_size = encode_int32(self.chunk_size)
        last_chunk = encode_int32(self.last_chunk)
        nchunks = encode_int64(self.nchunks)
        max_app_chunks = encode_int64(self.max_app_chunks)

        raw_bloscpack_header = (MAGIC + format_version + options + checksum +
                                typesize + chunk_size + last_chunk + nchunks +
                                max_app_chunks)
        print_debug('raw_bloscpack_header: %s' % repr(raw_bloscpack_header))
        return raw_bloscpack_header

    @staticmethod
    def decode(buffer_):
        """ Decode an encoded Bloscpack header.

        Parameters
        ----------
        buffer_ : str of length BLOSCPACK_HEADER_LENGTH

        Returns
        -------
        bloscpack_header : BloscPackHeader
            the decoded Bloscpack header object

        Raises
        ------
        ValueError
            If the buffer_ is not equal to BLOSCPACK_HEADER_LENGTH or the the
            first four bytes are not the Bloscpack magic.

        """
        buffer_ = memoryview(buffer_)
        if len(buffer_) != BLOSCPACK_HEADER_LENGTH:
            raise ValueError(
                "attempting to decode a bloscpack header of length '%d', not '%d'"
                % (len(buffer_), BLOSCPACK_HEADER_LENGTH))
        elif buffer_[0:4] != MAGIC:
            try:
                rep = buffer_[0:4].tobytes()
            except AttributeError:
                rep = buffer_[0:4]
            raise ValueError(
                "the magic marker '%s' is missing from the bloscpack " % MAGIC +
                "header, instead we found: %s" % repr(rep))
        options = decode_options(decode_bitfield(buffer_[5]))
        return BloscPackHeader(
            format_version=decode_uint8(buffer_[4]),
            offsets=options['offsets'],
            metadata=options['metadata'],
            checksum=CHECKSUMS_AVAIL[decode_uint8(buffer_[6])],
            typesize=decode_uint8(buffer_[7]),
            chunk_size=decode_int32(buffer_[8:12]),
            last_chunk=decode_int32(buffer_[12:16]),
            nchunks=decode_int64(buffer_[16:24]),
            max_app_chunks=decode_int64(buffer_[24:32]))


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
    _check_valid_checksum(meta_checksum)
    _check_valid_codec(meta_codec)
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
    out_file = in_file + EXTENSION \
        if args.out_file is None else args.out_file
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
            out_file = in_file[:-len(EXTENSION)] \
                    if args.out_file is None else args.out_file
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


def check_files(in_file, out_file, args):
    """ Check files exist/don't exist.

    Parameters
    ----------
    in_file : str:
        the input file
    out_file : str
        the output file
    args : parser args
        any additional arguments from the parser

    Raises
    ------
    FileNotFound
        in case any of the files isn't found.

    """
    if not path.exists(in_file):
        raise FileNotFound("input file '%s' does not exist!" % in_file)
    if path.exists(out_file):
        if not args.force:
            raise FileNotFound("output file '%s' exists!" % out_file)
        else:
            print_verbose("overwriting existing file: '%s'" % out_file)
    print_verbose("input file is: '%s'" % in_file)
    print_verbose("output file is: '%s'" % out_file)


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
    print_verbose('metadata args are:', level=DEBUG)
    for arg, value in metadata_args.iteritems():
        print_verbose('\t%s: %s' % (arg, value), level=DEBUG)
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
            print_verbose('metadata compression requested, but it was not '
                    'beneficial, deactivating '
                    "(raw: '%s' vs. compressed: '%s') " %
                    (meta_size, meta_comp_size),
                    level=DEBUG)
            meta_comp_size = meta_size
        else:
            codec = codec_impl.name
            metadata = metadata_compressed
    else:
        meta_size = len(metadata)
        meta_comp_size = meta_size
    print_verbose("Raw %s metadata of size '%s': %s" %
            ('compressed' if metadata_args['meta_codec'] != 'None' else
                'uncompressed', meta_comp_size, repr(metadata)),
            level=DEBUG)
    if hasattr(metadata_args['max_meta_size'], '__call__'):
        max_meta_size = metadata_args['max_meta_size'](meta_size)
    elif isinstance(metadata_args['max_meta_size'], int):
        max_meta_size = metadata_args['max_meta_size']
    print_verbose('max meta size is deemed to be: %d' %
            max_meta_size,
            level=DEBUG)
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
    print_verbose('raw_metadata_header: %s' % repr(raw_metadata_header),
            level=DEBUG)
    output_fp.write(raw_metadata_header)
    output_fp.write(metadata)
    prealloc = max_meta_size - meta_comp_size
    for i in xrange(prealloc):
        output_fp.write('\x00')
    metadata_total += prealloc
    print_verbose("metadata has %d preallocated empty bytes" %
            prealloc, level=DEBUG)
    if metadata_args['meta_checksum'] != CHECKSUMS_AVAIL[0]:
        metadata_checksum_impl = CHECKSUMS_LOOKUP[metadata_args['meta_checksum']]
        metadata_digest = metadata_checksum_impl(metadata)
        metadata_total += metadata_checksum_impl.size
        output_fp.write(metadata_digest)
        print_verbose("metadata checksum (%s): %s" %
                (metadata_args['meta_checksum'], repr(metadata_digest)),
                level=DEBUG)
    print_verbose("metadata section occupies a total of %s" %
            double_pretty_size(metadata_total), level=DEBUG)
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

    _metaclass__ = abc.ABCMeta

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
        self.metadata = {'dtype': ndarray.dtype.descr,
                         'shape': ndarray.shape,
                         'order': 'F' if np.isfortran(ndarray) else 'C',
                         'container': 'numpy',
                         }
        # TODO only one dim for now
        self.size = ndarray.size * ndarray.itemsize
        # TODO check that the array is contiguous
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
            print_verbose('checksum (%s): %s ' %
                    (self.checksum_impl.name, repr(digest)),
                    level=DEBUG)
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
        print_verbose("decompressing chunk '%d'%s" %
                (self.i, ' (last)' if self.nchunks is not None
                                   and self.i == self.nchunks - 1 else ''),
                level=DEBUG)
        decompressed = blosc.decompress(compressed)
        print_verbose("chunk handled, in: %s out: %s" %
                (pretty_size(len(compressed)),
                    pretty_size(len(decompressed))), level=DEBUG)
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
                dtype=metadata['dtype'][0][1],
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
    """ Helper function for pack_file.

    Use file_pointers, which could potentially be cStringIO objects.

    """
    _check_blosc_args(blosc_args)
    print_verbose('blosc args are:', level=DEBUG)
    for arg, value in blosc_args.iteritems():
        print_verbose('\t%s: %s' % (arg, value), level=DEBUG)
    _check_bloscpack_args(bloscpack_args)
    print_verbose('bloscpack args are:', level=DEBUG)
    for arg, value in bloscpack_args.iteritems():
        print_verbose('\t%s: %s' % (arg, value), level=DEBUG)
    max_app_chunks = _handle_max_apps(bloscpack_args['offsets'],
            nchunks,
            bloscpack_args['max_app_chunks'])
    # create the bloscpack header
    bloscpack_header = BloscPackHeader(
            offsets=bloscpack_args['offsets'],
            metadata=True if metadata is not None else False,
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
        print_verbose('metadata_args will be silently ignored', level=DEBUG)
    sink.init_offsets()

    compress_func = source.compress_func
    # read-compress-write loop
    for i, chunk in enumerate(source()):
        print_verbose("Handle chunk '%d' %s" % (i,'(last)' if i == nchunks -1
            else ''), level=DEBUG)
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
    pack_ndarray(ndarray, sink)
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
    print_verbose('reading bloscpack header', level=DEBUG)
    bloscpack_header_raw = input_fp.read(BLOSCPACK_HEADER_LENGTH)
    print_verbose('bloscpack_header_raw: %s' %
            repr(bloscpack_header_raw), level=DEBUG)
    bloscpack_header = BloscPackHeader.decode(bloscpack_header_raw)
    print_verbose("bloscpack header: %s" % repr(bloscpack_header), level=DEBUG)
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
    print_verbose("raw metadata header: '%s'" % repr(raw_metadata_header),
            level=DEBUG)
    metadata_header = decode_metadata_header(raw_metadata_header)
    print_verbose("metadata header: ", level=DEBUG)
    for arg, value in metadata_header.iteritems():
        print_verbose('\t%s: %s' % (arg, value), level=DEBUG)
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
            print_verbose('metadata checksum OK (%s): %s ' %
                    (metadata_checksum_impl.name,
                        repr(metadata_received_digest)),
                    level=DEBUG)
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
        print_verbose('Read raw offsets: %s' % repr(offsets_raw),
                level=DEBUG)
        offsets = [decode_int64(offsets_raw[j - 8:j]) for j in
                xrange(8, bloscpack_header.nchunks * 8 + 1, 8)]
        print_verbose('Offsets: %s' % offsets, level=DEBUG)
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
    print_verbose("Writing '%d' offsets: '%s'" %
            (len(offsets), repr(offsets)), level=DEBUG)
    # write the offsets encoded into the reserved space in the file
    encoded_offsets = "".join([encode_int64(i) for i in offsets])
    print_verbose("Raw offsets: %s" % repr(encoded_offsets),
            level=DEBUG)
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
    if LEVEL == DEBUG:
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
            print_verbose('checksum OK (%s): %s ' %
                    (checksum_impl.name, repr(received_digest)),
                    level=DEBUG)
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
        _check_valid_serializer(magic_format)
        metadata_args['magic_format'] = magic_format
    if checksum is not None:
        _check_valid_checksum(checksum)
        old_impl = CHECKSUMS_LOOKUP[old_metadata_header['meta_checksum']]
        new_impl = CHECKSUMS_LOOKUP[checksum]
        if old_impl.size != new_impl.size:
            raise ChecksumLengthMismatch(
                    'checksums have a size mismatch')
        metadata_args['meta_checksum'] = checksum
    if codec is not None:
        _check_valid_codec(codec)
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
        print_verbose("Handle chunk '%d' %s" % (i,'(last)' if i == nchunks -1
            else ''), level=DEBUG)

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
        LEVEL = VERBOSE
    elif args.debug:
        LEVEL = DEBUG
    print_verbose('command line argument parsing complete', level=DEBUG)
    print_verbose('command line arguments are: ', level=DEBUG)
    for arg, val in vars(args).iteritems():
        print_verbose('\t%s: %s' % (arg, str(val)), level=DEBUG)
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
                print_verbose("Metadata is:\n'%s'" % metadata, level=NORMAL)
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
