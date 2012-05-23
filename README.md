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

Place the file ``blpk`` somewhere in your ``$PATH``.

## Usage

    zsh» ./blpk --help
    [...]
    zsh» ./blpk compress --help
    [...]
    zsh» ./blpk decompress --help
    [...]

## Testing

    zsh» nosetests
    [...]

## Benchmark

Using the provided ``benchmark`` script on a ``Intel(R) Core(TM) i7 CPU
960  @ 3.20GHz`` cpu with 4 cores and active hyperthreading yields the
following results:

    zsh» ./benchmark
    create the test data
    testfile is: 1.5G
    do compression with bloscpack
    real 6.69
    user 7.24
    sys 1.68
    testfile.blp is: 589M
    do compression with gzip
    real 146.69
    user 144.24
    sys 0.91
    testfile.gz is: 919M

As was expected from previous benchmarks of Blosc using the python-blosc
bindings, Blosc is both much faster and has a better compression ratio for this
kind of data (``a = numpy.linspace(0, 100, 2e8)``).

## Implementation Details

This section describes various details regarding the implementation.

Since Blosc has a buffer limit of 2GB (May 2012) we split the file into chunks
no larger than 2GB where the last chunk may be a little larger than the other
ones since it contains the remainder. To facilitate this, Bloscpack adds an 8
byte header containing a 4 byte magic string, 'blpk', and a 4 byte
little-endian unsigned integer which designates how many chunks there are.
Effectively this limits the number of chunks to 2\*\*32-1 = 4294967295, but
this should not be relevant in practice. In terms of overhead, this means that
for a file which can not be compressed, bloscpack will add a total of 8 bytes
for itself and  16 bytes for each chunk compressed by Blosc overhead. Regarding
memory considerations, bloscpack is quite memory hungry compared to other
compressors. It will need enough memory to keep both the uncompressed and
compressed data in memory---with a chunk size of 2GB, more than 4GB of memory
is required.

## TODO

* ``setup.py``
* examples of use
* library usage
* make pylint happier
* more unit testing

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
