# bloscpack

Command line interface to Blosc via python-blosc

## Description

This script provides a command line interface to
[Blosc](http://blosc.pytables.org/trac) a high performance, multi-threaded,
blocking and shuffeling compressor. The interface is realized by using the
[argparse](http://docs.python.org/dev/library/argparse.html) library
and [python-blosc](https://github.com/FrancescAlted/python-blosc) bindings.

## Dependencies

* Python 2.7
* [python-blosc](https://github.com/FrancescAlted/python-blosc) (provides Blosc)

## Installation

Place the file ``bloscpack`` somewhere in your ``$PATH``.

## Usage

    zsh» ./bloscpack --help
    usage: bloscpack [-h] [--version] [--verbose] [--force] [--typesize <size>]
                     [--clevel {0,1,2,3,4,5,6,7,8,9}] [--no-shuffle]
                     [--no-check-extension] (-c | -d)
                     <in_file> [[<out_file>]]

    command line de/compression with blosc

    positional arguments:
      <in_file>             file to be de/compressed
      [<out_file>]          file to de/compress to

    optional arguments:
      -h, --help            show this help message and exit
      --version             show program's version number and exit
      --verbose             be verbose about actions
      --force               disable overwrite checks for existing files
                            (use with caution)
      -c, --compress        perform compression on <in_file>
      -d, --decompress      perform decompression on <in_file>

    compression only:
      --typesize <size>     typesize for blosc, default = 4
      --clevel {0,1,2,3,4,5,6,7,8,9}
                            compression level, default = 7
      --no-shuffle          deactivate shuffle

    decompression only:
      --no-check-extension  disable checking input file for extension (*.blp)
                            (requires use of <out_file>)

## Website

Repository is at: https://github.com/esc/bloscpack

## TODO

* files larger than memory, maybe chunking
* input-pipe and --raw for output
* magic file header

## Author, Copyright and License

(C) 2012 Valentin Haenel <valentin.haenel@gmx.de>

bloscpack is licensed under the terms of the MIT License.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FR
