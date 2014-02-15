#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


import argparse
from os import path


import blosc


import log
from .checksums import CHECKSUMS_AVAIL
from .defaults import (DEFAULT_TYPESIZE,
                        DEFAULT_CLEVEL,
                        DEFAULT_SHUFFLE,
                        DEFAULT_CNAME,
                        DEFAULT_CHUNK_SIZE,
                        DEFAULT_CHECKSUM,
                        DEFAULT_OFFSETS,
                        )
from .constants import (SUFFIXES,
                        CNAME_AVAIL,
                        )
from .exceptions import FileNotFound
from .pretty import (reverse_pretty,
                     join_with_eol,
                     )
from .version import __version__


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
            log.print_verbose("overwriting existing file: '%s'" % out_file)
    log.print_verbose("input file is: '%s'" % in_file)
    log.print_verbose("output file is: '%s'" % out_file)


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
                log.error('%s must be 1 <= n <= %d'
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
                    log.error('%s error: %s' % (option_string, str(ve) +
                        " or 'max'"))
                if value < 0:
                    log.error('%s must be > 0 ' % option_string)
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


