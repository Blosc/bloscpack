#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

""" Command line interface to Blosc via python-blosc """

from __future__ import division

import sys
import os.path as path
import argparse
import struct
import math
import blosc

__version__ = '0.1.0-rc1'
__author__ = 'Valentin Haenel <valentin.haenel@gmx.de>'

EXTENSION = '.blp'
MAGIC = 'blpk'
MAX_CHUNKS = (2**32)-1
NORMAL  = 'NORMAL'
VERBOSE = 'VERBOSE'
DEBUG   = 'DEBUG'
LEVEL = NORMAL
VERBOSITY_LEVELS = [NORMAL, VERBOSE, DEBUG]
PREFIX = "bloscpack.py"
BLOSC_ARGS = ['typesize', 'clevel', 'shuffle']
SUFFIXES = { "B": 1,
             "K": 2**10,
             "M": 2**20,
             "G": 2**30,
             "T": 2**40}

def print_verbose(message, level=VERBOSE):
    """ Print message with desired verbosity level. """
    if level not in VERBOSITY_LEVELS:
        raise TypeError("Desired level '%s' is not one of %s" % (level,
            str(VERBOSITY_LEVELS)))
    if VERBOSITY_LEVELS.index(level) <= VERBOSITY_LEVELS.index(LEVEL):
        print('%s: %s' % (PREFIX, message))

def error(message, exit_code=1):
    """ Print message and exit with desired code. """
    for l in [l for l in message.split('\n') if l != '']:
        print('%s: error: %s' % (PREFIX, l))
    sys.exit(exit_code)

def pretty_size(size_in_bytes):
    """ Pretty print filesize.  """
    for suf, lim in reversed(sorted(SUFFIXES.items(),key=lambda x: x[1])):
        if size_in_bytes < lim:
            continue
        else:
            return str(round(size_in_bytes/lim, 2))+suf

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
            version='%(prog)s:\t' + ("'%s'\n" % __version__) + \
                    "python-blosc:\t'%s'\n"   % blosc.version.__version__ + \
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
            help='disable overwrite checks for existing files\n' + \
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

    class CheckNchunksOption(argparse.Action):
        def __call__(self, parser, namespace, value, option_string=None):
            if not 1 <= value <= MAX_CHUNKS:
                error('%s must be 1 <= n <= %d'
                        % (option_string, MAX_CHUNKS))
            setattr(namespace, self.dest, value)
    class CheckChunkSizeOption(argparse.Action):
        def __call__(self, parser, namespace, value, option_string=None):
            try:
                # try to get the value as bytes
                value = reverse_pretty(value)
            except ValueError as ve:
                error('%s error: %s' % (option_string, ve.message))
            if value < 0:
                error('%s must be > 0 ' % option_string)
            setattr(namespace, self.dest, value)
    for p in [compress_parser, c_parser]:
        blosc_group = p.add_argument_group(title='blosc settings')
        blosc_group.add_argument('-t', '--typesize',
                metavar='<size>',
                default=4,
                type=int,
                help='typesize for blosc')
        blosc_group.add_argument('-l', '--clevel',
                default=7,
                choices=range(10),
                metavar='[0, 9]',
                type=int,
                help='compression level')
        blosc_group.add_argument('-s', '--no-shuffle',
                action='store_false',
                default=True,
                dest='shuffle',
                help='deactivate shuffle')
        bloscpack_group = p.add_mutually_exclusive_group()
        bloscpack_group.add_argument('-c', '--nchunks',
                metavar='[1, 2**32-1]',
                action=CheckNchunksOption,
                type=int,
                default=None,
                help='set desired number of chunks')
        bloscpack_group.add_argument('-z', '--chunk-size',
                metavar='<size>',
                action=CheckChunkSizeOption,
                type=str,
                default=None,
                dest='chunk_size',
                help='set desired number of chunks')

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

    The Blosc 1.1.3 header is 16 bytes as follows:

    |-0-|-1-|-2-|-3-|-4-|-5-|-6-|-7-|-8-|-9-|-A-|-B-|-C-|-D-|-E-|-F-|
      ^   ^   ^   ^ |     nbytes    |   blocksize   |    ctbytes    |
      |   |   |   |
      |   |   |   +--typesize
      |   |   +------flags
      |   +----------versionlz
      +--------------version

    The first four are simply bytes, the last three are are each unsigned ints
    (uint32) each occupying 4 bytes. The header is always little-endian.
    'ctbytes' is the length of the buffer including header and nbytes is the
    length of the data when uncompressed.

    """
    def decode_byte(byte):
        return int(byte.encode('hex'), 16)
    def decode_uint32(fourbyte):
        return struct.unpack('<I', fourbyte)[0]
    return {'version': decode_byte(buffer_[0]),
            'versionlz': decode_byte(buffer_[1]),
            'flags': decode_byte(buffer_[2]),
            'typesize': decode_byte(buffer_[3]),
            'nbytes': decode_uint32(buffer_[4:8]),
            'blocksize': decode_uint32(buffer_[8:12]),
            'ctbytes': decode_uint32(buffer_[12:16])}

class ChunkingException(BaseException):
    pass

def calculate_nchunks(in_file_size, nchunks=None, chunk_size=None):
    """ Determine chunking for an input file.

    Parameters
    ----------

    in_file_size : int
        the size of the input file
    nchunks : int, default: None
        the number of chunks desired by the user
    chunk_size : int, default: None
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
        under various error conditions

    """
    if nchunks != None and chunk_size != None:
        raise ValueError(
                "either specify 'nchunks' or 'chunk_size', but not both")
    elif nchunks != None and chunk_size == None:
        print_verbose("'nchunks' proposed", level=DEBUG)
        if nchunks > in_file_size:
            raise ChunkingException(
                    "Your value of 'nchunks': %d is" % nchunks +
                    "greater than the 'in_file size': %d" % in_file_size)
        elif nchunks <= 0:
            raise ChunkingException(
                    "'nchunks' must be greate than zero, not '%d' " % nchunks)
        quotient, remainder = divmod(in_file_size, nchunks)
        if nchunks == 1:
            chunk_size = 0
            last_chunk_size = in_file_size
        elif remainder == 0:
            chunk_size = quotient
            last_chunk_size = chunk_size
        elif nchunks == 2:
            chunk_size = quotient
            last_chunk_size = in_file_size - chunk_size
        else:
            chunk_size = in_file_size//(nchunks-1)
            last_chunk_size = in_file_size - chunk_size * (nchunks-1)
    elif nchunks == None and chunk_size != None:
        print_verbose("'chunk_size' proposed", level=DEBUG)
        if chunk_size > in_file_size:
            raise ChunkingException(
                    "Your value of 'chunk_size': %d is" % chunk_size +
                    "greater than the 'in_file size': %d" % in_file_size)
        elif chunk_size <= 0:
            raise ChunkingException(
                    "'chunk_size' must be greate than zero, not '%d' " %
                    chunk_size)
        quotient, remainder = divmod(in_file_size, chunk_size)
        if chunk_size == in_file_size:
            nchunks = 1
            chunk_size = 0
            last_chunk_size = in_file_size
        elif remainder == 0:
            nchunks = quotient
            last_chunk_size = chunk_size
        else:
            nchunks = quotient + 1
            last_chunk_size = remainder
    elif nchunks == None and chunk_size == None:
        nchunks =  int(math.ceil(in_file_size/blosc.BLOSC_MAX_BUFFERSIZE))
        quotient, remainder = divmod(in_file_size, blosc.BLOSC_MAX_BUFFERSIZE)
        if in_file_size == blosc.BLOSC_MAX_BUFFERSIZE:
            nchunks = 1
            chunk_size = 0
            last_chunk_size = blosc.BLOSC_MAX_BUFFERSIZE
        elif quotient == 0:
            chunk_size = 0
            last_chunk_size = in_file_size
        else:
            chunk_size = blosc.BLOSC_MAX_BUFFERSIZE
            last_chunk_size = in_file_size % blosc.BLOSC_MAX_BUFFERSIZE
    if chunk_size > blosc.BLOSC_MAX_BUFFERSIZE \
            or last_chunk_size > blosc.BLOSC_MAX_BUFFERSIZE:
        raise ChunkingException(
            "Your value of 'nchunks' would lead to chunk sizes bigger than " +\
            "'BLOSC_MAX_BUFFERSIZE', please use something smaller.\n" +\
            "nchunks : %d\n" % nchunks +\
            "chunk_size : %d\n" % chunk_size +\
            "last_chunk_size : %d\n" % last_chunk_size +\
            "BLOSC_MAX_BUFFERSIZE : %d\n" % blosc.BLOSC_MAX_BUFFERSIZE)
    elif nchunks > MAX_CHUNKS:
        raise ChunkingException(
                "nchunks: '%d' is greate than the MAX_CHUNKS: '%d'" %
                (nchunks, MAX_CHUNKS))
    print_verbose('nchunks: %d' % nchunks, level=VERBOSE)
    print_verbose('chunk_size: %s' % pretty_size(chunk_size), level=VERBOSE)
    print_verbose('last_chunk_size: %s' % pretty_size(last_chunk_size),
            level=DEBUG)
    return nchunks, chunk_size, last_chunk_size

def create_bloscpack_header(nchunks):
    """ Create the bloscpack header string.

    Parameters
    ----------
    nchunks : int
        the number of chunks

    Returns
    -------
    bloscpack_header : string
        the header as string

    Notes
    -----

    The bloscpack header is 8 bytes as follows:

    |-0-|-1-|-2-|-3-|-4-|-5-|-6-|-7-|
    | b   l   p   k |    nchunks    |

    The first four are the magic string 'blpk' and the second four are an
    unsigned 32 bit little-endian integer.

    """
    # this will fail if nchunks is larger than the max of an unsigned int
    return (MAGIC + struct.pack('<I', nchunks))

def decode_bloscpack_header(buffer_):
    """ Check that the magic marker exists and return number of chunks. """
    if len(buffer_) != 8:
        raise ValueError(
            'attempting to decode a bloscpack header of length other than 8')
    elif buffer_[0:4] != MAGIC:
        raise ValueError(
            "the magic marker '%s' is missing from the bloscpack " % MAGIC +
            "header, instead we found: '%s'" % buffer_[0:4])
    return struct.unpack('<I', buffer_[4:])[0]

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
    blosc_args = dict((arg, args.__getattribute__(arg)) for arg in BLOSC_ARGS)
    return in_file, out_file, blosc_args

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

def check_files(in_file, out_file, args):
    """ Check files exist/don't exist.

    Warning: may call sys.exit()

    """
    if not path.exists(in_file):
        error("input file '%s' does not exist!" % in_file)
    if path.exists(out_file):
        if not args.force:
            error("output file '%s' exists!" % out_file)
        else:
            print_verbose("overwriting existing file: %s" % out_file)
    print_verbose('input file is: %s' % in_file)
    print_verbose('output file is: %s' % out_file)

def process_nthread_arg(args):
    """ Extract and set nthreads. """
    if args.nthreads != blosc.ncores:
        blosc.set_nthreads(args.nthreads)
    print_verbose('using %d thread%s' %
            (args.nthreads, 's' if args.nthreads > 1 else ''))

def pack_file(in_file, out_file, blosc_args, nchunks=None, chunk_size=None):
    """ Main function for compressing a file.

    Parameters
    ----------
    in_file : str
        the name of the input file
    out_file : str
        the name of the output file
    blosc_args : dict
        dictionary of blosc keyword args
    nchunks : int, default: None
        The desired number of chunks.
    chunk_size : int, default: None
        The desired chunk size in bytes.

    Notes
    -----
    The parameters 'nchunks' and 'chunk_size' are mutually exclusive. Will be
    determined automatically if not present.

    """
    # calculate chunk sizes
    in_file_size = path.getsize(in_file)
    print_verbose('input file size: %s' % pretty_size(in_file_size))
    nchunks, chunk_size, last_chunk_size = \
            calculate_nchunks(in_file_size, nchunks, chunk_size)
    # calculate header
    bloscpack_header = create_bloscpack_header(nchunks)
    print_verbose('bloscpack_header: %s' % repr(bloscpack_header), level=DEBUG)
    # write the chunks to the file
    with open(in_file, 'rb') as input_fp, \
         open(out_file, 'wb') as output_fp:
        output_fp.write(bloscpack_header)
        # if nchunks == 1 the last_chunk_size is the size of the single chunk
        for i, bytes_to_read in enumerate((
                [chunk_size] * (nchunks - 1)) + [last_chunk_size]):
            print_verbose("compressing chunk '%d'%s" %
                    (i, ' (last)' if i == nchunks-1 else ''), level=DEBUG)
            current_chunk = input_fp.read(bytes_to_read)
            compressed = blosc.compress(current_chunk, **blosc_args)
            output_fp.write(compressed)
            print_verbose("chunk written, in: %s out: %s" %
                    (pretty_size(len(current_chunk)),
                        pretty_size(len(compressed))), level=DEBUG)
    out_file_size = path.getsize(out_file)
    print_verbose('output file size: %s' % pretty_size(out_file_size))
    print_verbose('compression ratio: %f' % (out_file_size/in_file_size))

def unpack_file(in_file, out_file):
    """ Main function for decompressing a file.

    Parameters
    ----------
    in_file : str
        the name of the input file
    out_file : str
        the name of the output file
    """
    in_file_size = path.getsize(in_file)
    print_verbose('input file size: %s' % pretty_size(in_file_size))
    with open(in_file, 'rb') as input_fp, \
         open(out_file, 'wb') as output_fp:
        # read the bloscpack header
        print_verbose('reading bloscpack header', level=DEBUG)
        bloscpack_header = input_fp.read(8)
        nchunks = decode_bloscpack_header(bloscpack_header)
        print_verbose('nchunks: %d' % nchunks, level=DEBUG)
        for i in range(nchunks):
            print_verbose("decompressing chunk '%d'%s" %
                    (i, ' (last)' if i == nchunks-1 else ''), level=DEBUG)
            print_verbose('reading blosc header', level=DEBUG)
            blosc_header_raw = input_fp.read(16)
            blosc_header = decode_blosc_header(blosc_header_raw)
            ctbytes = blosc_header['ctbytes']
            print_verbose('ctbytes: %s' % pretty_size(ctbytes), level=DEBUG)
            # seek back 16 bytes in file relative to current position
            input_fp.seek(-16, 1)
            compressed = input_fp.read(ctbytes)
            decompressed = blosc.decompress(compressed)
            output_fp.write(decompressed)
            print_verbose("chunk written, in: %s out: %s" %
                    (pretty_size(len(compressed)),
                        pretty_size(len(decompressed))), level=DEBUG)
    out_file_size = path.getsize(out_file)
    print_verbose('output file size: %s' % pretty_size(out_file_size))
    print_verbose('decompression ratio: %f' % (out_file_size/in_file_size))

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

    # compression and decompression handled via subparsers
    if args.subcommand in ['compress', 'c']:
        print_verbose('getting ready for compression')
        in_file, out_file, blosc_args = process_compression_args(args)
        print_verbose('blosc args are:', level=DEBUG)
        for arg, value in blosc_args.iteritems():
            print_verbose('\t%s: %s' % (arg, value), level=DEBUG)
        check_files(in_file, out_file, args)
        process_nthread_arg(args)
        try:
            pack_file(in_file, out_file, blosc_args,
                    nchunks=args.nchunks, chunk_size=args.chunk_size)
        except ChunkingException as e:
            error(e.message)
    elif args.subcommand in ['decompress', 'd']:
        print_verbose('getting ready for decompression')
        in_file, out_file = process_decompression_args(args)
        check_files(in_file, out_file, args)
        process_nthread_arg(args)
        try:
            unpack_file(in_file, out_file)
        except ValueError as ve:
            error(ve.message)
    else:
        # we should never reach this
        error('You found the easter-egg, please contact the author')
    print_verbose('done')
