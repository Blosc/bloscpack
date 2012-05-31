# bloscpack

Command line interface to Blosc via python-blosc

## Description

This script provides a command line interface to
[Blosc](http://blosc.pytables.org/trac), a high performance, multi-threaded,
blocking and shuffeling compressor. The interface is realized by using the
[argparse](http://docs.python.org/dev/library/argparse.html) library
and [python-blosc](https://github.com/FrancescAlted/python-blosc) bindings.

## Website

Repository is at: https://github.com/esc/bloscpack

## Dependencies

* Python 2.7
* [python-blosc](https://github.com/FrancescAlted/python-blosc) (provides Blosc)

## Installation

Add the ``blpk`` file to your ``$PATH`` somehow. For example by copying using
dereferencing (``-L``), since ``blpk`` is a sym-link to ``bloscpack.py``::

    zsh» cp -L blpk ~/bin

Or, of course, use the standard ``setup.py``::

    zsh» python setup.py install

... which may require superuser privileges.

## Usage

Bloscpack has a number of global options and two subcommands: ``[c |
compress]`` and ``[d | decompress]`` which each have their own options.


Help for global options and subcommands:

    zsh» ./blpk --help
    [...]

Help for each one of the subcommands:

    zsh» ./blpk compress --help
    [...]
    zsh» ./blpk decompress --help
    [...]

## Examples

Basic compression:

    zsh» ./blpk c data.dat

... will compress the file ``data.dat`` to ``data.dat.blp``

Basic decompression:

    zsh» ./blpk d data.dat.blp data.dcmp

... will decompress the file ``data.dat.blp`` to the file ``data.dcmp``. If you
leave out the ``[<out_file>]`` argument, bloscpack will complain that the file
``data.dat`` exists already and refuse to overwrite it:

    zsh» ./blpk d data.dat.blp
    blpk: error: output file 'data.dat' exists!

If you know what you are doing, you can use the global option ``[-f |
--force]`` to override the overwrite checks:

    zsh» ./blpk -f d data.dat.blp

Incidentally this works for compression too:

    zsh» ./blpk c data.dat
    blpk: error: output file 'data.dat.blp' exists!
    zsh» ./blpk -f c data.dat

By default, the number of threads that Blosc uses is determined by the number
of cores detected on your system. You can change this using the ``[-n |
--nthreads]`` option:

    zsh» ./blpk -n 1 c data.dat

There are some useful additional options for compression, that are passed
directly to Blosc:

* ``[-t | --typesize]``
  Typesize used by Blosc (default: 4):
  ``zsh» ./blpk c -t 8 data.dat``
* ``[-l | --level]``
  Compression level (default: 7):
  ``zsh» ./blpk c -l 3 data.dat``
* ``[-s | --no-shuffle]``
  Deactivate shuffle:
  ``zsh» ./blpk c -s data.dat``

In addition, there are two mutually exclusive options for bloscpack itself,
that govern how the file is split into chunks:

* ``[-z | --chunk-size]``
  Desired approximate size of the chunks, where you can use human readable
  strings like ``8M`` or ``128K``: ``zsh» ./blpk -d c -z 500000 data.dat``
* ``[-c | --nchunks]``
  Desired number of chunks:
  ``zsh» ./blpk -d c -c 2 data.dat``

Lastly there are two options to control how much output is produced,

The first causes basic info to be printed, ``[-v | --verbose]``:

    zsh» ./blpk -v c data.dat
    blpk: getting ready for compression
    blpk: input file is: data.dat
    blpk: output file is: data.dat.blp
    blpk: using 8 threads
    blpk: input file size: 1.53M
    blpk: output file size: 999.85K
    blpk: compression ratio: 0.639903
    blpk: done

... and ``[-d | --debug]`` prints a detailed account of what is going on:

    zsh» ./blpk -d c data.dat
    blpk: command line argument parsing complete
    blpk: command line arguments are:
    blpk:   nchunks: None
    blpk:   force: False
    blpk:   verbose: False
    blpk:   out_file: None
    blpk:   subcommand: c
    blpk:   in_file: data.dat
    blpk:   chunk_size: None
    blpk:   debug: True
    blpk:   shuffle: True
    blpk:   typesize: 4
    blpk:   clevel: 7
    blpk:   nthreads: 8
    blpk: getting ready for compression
    blpk: blosc args are:
    blpk:   typesize: 4
    blpk:   shuffle: True
    blpk:   clevel: 7
    blpk: input file is: data.dat
    blpk: output file is: data.dat.blp
    blpk: using 8 threads
    blpk: input file size: 1.53M
    blpk: nchunks: 1
    blpk: chunk_size: 1.53M
    blpk: last_chunk_size: 1.53M
    blpk: bloscpack_header: 'blpk\x01\x00\x00\x00'
    blpk: compressing chunk '0' (last)
    blpk: chunk written, in: 1.53M out: 999.84K
    blpk: output file size: 999.85K
    blpk: compression ratio: 0.639903
    blpk: done

## Testing

Basic tests, runs quickly:

    zsh» nosetests
    [...]

Extended tests:

    zsh» nosetests test_bloscpack.py:pack_unpack_extended
    [...]

# Benchmark

Using the provided ``benchmark`` script on a ``Intel(R) Core(TM) i7 CPU
960  @ 3.20GHz`` cpu with 4 cores and active hyperthreading yields the
following results:

    zsh» ./benchmark
    create the test data
    testfile is: 153M
    enlarge the testfile......... done.
    testfile is: 1.5G
    do compression with bloscpack, chunk-size: 128MB
    real 8.79
    user 6.64
    sys 1.29
    testfile.blp is: 639M
    do compression with gzip
    real 117.18
    user 113.26
    sys 1.01
    testfile.gz is: 960M


As was expected from previous benchmarks of Blosc using the python-blosc
bindings, Blosc is both much faster and has a better compression ratio for this
kind of structured data.

## Implementation Details

This section describes various details regarding the implementation.

Since Blosc has a buffer limit of 2GB (May 2012) we split the file into chunks
no larger than 2GB where the last chunk may be a little larger than the other
ones since it contains the remainder. To facilitate this, Bloscpack adds an 8
byte header containing a 4 byte magic string, 'blpk', and a 4 byte
little-endian unsigned integer which designates how many chunks there are.
Effectively, this limits the number of chunks to ``2**32-1 = 4294967295``, but
this should not be relevant in practice. In terms of overhead, this means that
for a file which can not be compressed, bloscpack will add a total of 8 bytes
for itself and  16 bytes for each chunk compressed by Blosc. Regarding memory,
bloscpack is quite memory hungry compared to other compressors. It will need
enough memory to keep both the uncompressed and compressed data in
memory---with a chunk size of 2GB, more than 4GB of memory is recommended.

## TODO

* library usage

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
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
