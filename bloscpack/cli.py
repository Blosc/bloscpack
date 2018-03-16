#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import argparse
from os import path
import json
import pprint


import blosc


from .args import (BloscArgs,
                   BloscpackArgs,
                   MetadataArgs,
                   )
from .append import (append,
                     _seek_to_metadata,
                     _rewrite_metadata_fp
                     )
from .checksums import (CHECKSUMS_AVAIL,
                        )
from .constants import (SUFFIXES,
                        CNAME_AVAIL,
                        EXTENSION,
                        MIN_CLEVEL,
                        MAX_CLEVEL,
                        )
from .defaults import (DEFAULT_TYPESIZE,
                       DEFAULT_CLEVEL,
                       DEFAULT_SHUFFLE,
                       DEFAULT_CNAME,
                       DEFAULT_CHUNK_SIZE,
                       DEFAULT_CHECKSUM,
                       DEFAULT_OFFSETS,
                       )
from .exceptions import (FileNotFound,
                         ChunkingException,
                         FormatVersionMismatch,
                         ChecksumMismatch,
                         )
from .file_io import (pack_file,
                      unpack_file,
                      _read_beginning,
                      _read_compressed_chunk_fp,
                      )
from .headers import (decode_blosc_flags,
                      )
from .pretty import (reverse_pretty,
                     join_with_eol,
                     )
from .version import __version__
from . import log


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
            log.verbose("overwriting existing file: '%s'" % out_file)
    log.verbose("input file is: '%s'" % in_file)
    log.verbose("output file is: '%s'" % out_file)


def _blosc_args_from_args(args):
    return BloscArgs(typesize=args.typesize,
                     clevel=args.clevel,
                     shuffle=args.shuffle,
                     cname=args.cname,
                     )


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
            log.error('--no-check-extension requires use of <out_file>')
    else:
        if in_file.endswith(EXTENSION):
            out_file = args.out_file or in_file[:-len(EXTENSION)]
        else:
            log.error("input file '%s' does not end with '%s'" %
                      (in_file, EXTENSION))
    return in_file, out_file


def process_append_args(args):
    original_file = args.original_file
    new_file = args.new_file
    if not args.no_check_extension and not original_file.endswith(EXTENSION):
        log.error("original file '%s' does not end with '%s'" %
                  (original_file, EXTENSION))

    return original_file, new_file


def process_metadata_args(args):
    if args.metadata is not None:
        try:
            with open(args.metadata, 'r') as metadata_file:
                return json.loads(metadata_file.read().strip())
        except IOError as ioe:
            log.error(ioe.message)


def process_nthread_arg(args):
    """ Extract and set nthreads. """
    if args.nthreads != blosc.ncores:
        blosc.set_nthreads(args.nthreads)
    log.verbose('using %d thread%s' %
                (args.nthreads, 's' if args.nthreads > 1 else ''))


def log_metadata(metadata):
    log.normal("Metadata:")
    log.normal(pprint.pformat(metadata, width=90))


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

    def _fill_text(self, text, width, indent):
        return ''.join([indent + line for line in text.splitlines(True)])


def _inject_blosc_group(parser):
    blosc_group = parser.add_argument_group(title='blosc settings')
    blosc_group.add_argument('-t', '--typesize',
                             metavar='<size>',
                             default=DEFAULT_TYPESIZE,
                             type=int,
                             help='typesize for blosc')
    blosc_group.add_argument('-l', '--clevel',
                             default=DEFAULT_CLEVEL,
                             choices=range(MIN_CLEVEL, MAX_CLEVEL+1),
                             metavar='[0, 9]',
                             type=int,
                             help='compression level')
    blosc_group.add_argument('-s', '--no-shuffle',
                             action='store_false',
                             default=DEFAULT_SHUFFLE,
                             dest='shuffle',
                             help='deactivate shuffle')
    blosc_group.add_argument('-c', '--codec',
                             metavar='<codec>',
                             type=str,
                             choices=CNAME_AVAIL,
                             default=DEFAULT_CNAME,
                             dest='cname',
                             help="codec to be used by Blosc: \n%s"
                                  % join_with_eol(CNAME_AVAIL))


def create_parser():
    """ Create and return the parser. """
    parser = argparse.ArgumentParser(
            #usage='%(prog)s [GLOBAL_OPTIONS] (compress | decompress)
            # [COMMAND_OPTIONS] <in_file> [<out_file>]',
            description='command line de/compression with blosc',
            formatter_class=BloscPackCustomFormatter,
            epilog="Additional help for subcommands is available:\n"+
            "  %(prog)s 'subcommand' [ -h | --help ]")

    ## print version of bloscpack, python-blosc and blosc itself
    version_str = "bloscpack: '%s' " % __version__ + \
                  "python-blosc: '%s' " % blosc.version.__version__ + \
                  "blosc: '%s'" % blosc.BLOSC_VERSION_STRING
    parser.add_argument('--version', action='version', version=version_str)
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
                log.error('%s must be 1 <= n <= %d'
                          % (option_string, blosc.BLOSC_MAX_THREADS))
            setattr(namespace, self.dest, value)

    global_group.add_argument('-n', '--nthreads',
                              metavar='[1, %d]' % blosc.BLOSC_MAX_THREADS,
                              action=CheckThreadOption,
                              default=blosc.ncores,
                              type=int,
                              dest='nthreads',
                              help='set number of threads, ' +
                                   '(default: %(default)s (ncores))')

    subparsers = parser.add_subparsers(title='subcommands',
                                       metavar='',
                                       dest='subcommand')
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
                    log.error('%s error: %s' % (option_string, str(ve)))
                if value < 0:
                    log.error('%s must be > 0' % option_string)
            setattr(namespace, self.dest, value)
    for p in [compress_parser, c_parser]:
        _inject_blosc_group(p)
        bloscpack_group = p.add_argument_group(title='bloscpack settings')
        bloscpack_group.add_argument('-z', '--chunk-size',
                                     metavar='<size>',
                                     action=CheckChunkSizeOption,
                                     type=str,
                                     default=DEFAULT_CHUNK_SIZE,
                                     dest='chunk_size',
                                     help="set desired chunk size or 'max'")
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
                                  'file to be compressed',
                                  'file to compress to'),
                                 (c_parser,
                                  'file to be compressed',
                                  'file to compress to'),
                                 (decompress_parser,
                                  'file to be decompressed',
                                  'file to decompress to'),
                                 (d_parser,
                                  'file to be decompressed',
                                  'file to decompress to'),
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


def main():
    parser = create_parser()
    log.set_prefix(parser.prog)
    args = parser.parse_args()
    if args.verbose:
        log.LEVEL = log.VERBOSE
    elif args.debug:
        log.LEVEL = log.DEBUG
    log.debug('command line argument parsing complete')
    log.debug('command line arguments are: ')
    for arg, val in sorted(vars(args).items()):
        log.debug('    %s: %s' % (arg, str(val)))
    process_nthread_arg(args)

    # compression and decompression handled via subparsers
    if args.subcommand in ['compress', 'c']:
        log.verbose('getting ready for compression')
        in_file, out_file, blosc_args = process_compression_args(args)
        try:
            check_files(in_file, out_file, args)
        except FileNotFound as fnf:
            log.error(str(fnf))
        metadata = process_metadata_args(args)
        bloscpack_args = BloscpackArgs(offsets=args.offsets,
                                       checksum=args.checksum)
        try:
            pack_file(in_file, out_file, chunk_size=args.chunk_size,
                      metadata=metadata,
                      blosc_args=blosc_args,
                      bloscpack_args=bloscpack_args,
                      metadata_args=MetadataArgs())
        except ChunkingException as ce:
            log.error(str(ce))
    elif args.subcommand in ['decompress', 'd']:
        log.verbose('getting ready for decompression')
        in_file, out_file = process_decompression_args(args)
        try:
            check_files(in_file, out_file, args)
        except FileNotFound as fnf:
            log.error(str(fnf))
        try:
            metadata = unpack_file(in_file, out_file)
            if metadata:
                log_metadata(metadata)
        except FormatVersionMismatch as fvm:
            log.error(fvm.message)
        except ChecksumMismatch as csm:
            log.error(csm.message)
    elif args.subcommand in ['append', 'a']:
        log.verbose('getting ready for append')
        original_file, new_file = process_append_args(args)
        try:
            if not path.exists(original_file):
                raise FileNotFound("original file '%s' does not exist!" %
                                   original_file)
            if not path.exists(new_file):
                raise FileNotFound("new file '%s' does not exist!" %
                                   new_file)
        except FileNotFound as fnf:
            log.error(str(fnf))
        log.verbose("original file is: '%s'" % original_file)
        log.verbose("new file is: '%s'" % new_file)
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
            log.error(str(fnf))
        try:
            with open(args.file_, 'rb') as fp:
                bloscpack_header, metadata, metadata_header, offsets = \
                    _read_beginning(fp)
                checksum_impl = bloscpack_header.checksum_impl
                # get the header of the first chunk
                _, blosc_header, _ = _read_compressed_chunk_fp(
                    fp, checksum_impl)
        except ValueError as ve:
            log.error(str(ve) + "\n" +
                      "This might not be a bloscpack compressed file.")
        log.normal(bloscpack_header.pformat())
        if offsets:
            log.normal("'offsets':")
            log.normal("[%s,...]" % (",".join(str(o) for o in offsets[:5])))
        if metadata is not None:
            log_metadata(metadata)
            log.normal(metadata_header.pformat())
        log.normal("First chunk blosc header:")
        log.normal(str(blosc_header))
        log.normal("First chunk blosc flags: ")
        log.normal(str(decode_blosc_flags(blosc_header['flags'])))
    else:  # pragma: no cover
        # in Python 3 subcommands are not mandatory by default
        parser.print_usage()
        log.error('too few arguments', 2)
    log.verbose('done')
