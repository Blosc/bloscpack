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

    zsh» ./bloscpack --help
    [...]
    zsh» ./bloscpack compress --help
    [...]
    zsh» ./bloscpack decompress --help
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

## TODO

* 'c' and 'd' as aliases for compress and decompress
* allow to adjust the chunk size
* progress bar (maybe) and timing (maybe)
* multiple verbosity levels
* input-pipe and --raw for output
* python-blosc as submodule?

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
