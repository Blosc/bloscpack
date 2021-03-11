Bloscpack
=========

:Author: Valentin Haenel
:Contact: valentin@haenel.co
:List: http://groups.google.com/group/blosc
:Github: https://github.com/Blosc/bloscpack
:PyPi: https://pypi.python.org/pypi/bloscpack
:Conda-Forge: https://github.com/conda-forge/bloscpack-feedstock
:Anaconda: https://anaconda.org/pypi/bloscpack
:Ohloh: https://www.ohloh.net/p/bloscpack
:Version: |version|
:Travis CI: |travis|
:GitHub Actions: |gha|
:Coveralls: |coveralls|
:Python Versions: |pyversions|
:License: |license|
:And...: |powered|

.. |version| image::    https://img.shields.io/pypi/v/bloscpack.svg
        :target: https://pypi.python.org/pypi/bloscpack

.. |travis| image:: https://img.shields.io/travis/Blosc/bloscpack/master.svg
        :target: https://travis-ci.org/Blosc/bloscpack

.. |gha| image:: https://github.com/Blosc/bloscpack/actions/workflows/testbloscpack.yml/badge.svg
        :target: https://github.com/Blosc/bloscpack/actions/workflows/testbloscpack.yml

.. |coveralls| image:: https://coveralls.io/repos/Blosc/bloscpack/badge.svg?branch=master&service=github
        :target: https://coveralls.io/github/Blosc/bloscpack?branch=master

.. |license| image:: https://img.shields.io/pypi/l/bloscpack.svg
        :target: https://pypi.python.org/pypi/bloscpack

.. |powered| image:: https://img.shields.io/badge/Powerd--By-Blosc-blue.svg
        :target: https://blosc.org

.. |pyversions| image:: https://img.shields.io/pypi/pyversions/bloscpack.svg
        :target: https://pypi.python.org/pypi/bloscpack

.. contents:: Table of Contents
   :depth: 1

Description
-----------

Command line interface to and serialization format for `Blosc
<http://blosc.org/>`_, a high performance, multi-threaded, blocking and
shuffling compressor. Uses `python-blosc
<https://github.com/Blosc/python-blosc>`_ bindings to interface with Blosc.
Also comes with native support for efficiently serializing and deserializing
Numpy arrays.

Code of Conduct
---------------

The Blosc community has adopted a `Code of Conduct
<https://github.com/Blosc/CodeOfConduct>`_ that we expect project participants
to adhere to. Please read the full text so that you can understand what actions
will and will not be tolerated.

Dependencies
------------

* Python 3.6 3.7 3.8 and 3.9
* `python-blosc  <https://github.com/Blosc/python-blosc>`_ (provides Blosc) and
  `Numpy <http://www.numpy.org/>`_ (as listed in ``requirements.txt``) for
  running the code
* The Python packages listed in ``test_requirements.txt`` for testing and
  releasing

Stability of File Format
------------------------

The tool is considered alpha-stage, experimental, research software. It is not
unlikely that **the internal storage format for the compressed files will
change in future**. Please **do not depend critically on the files generated
(unless you know what you are doing)** by Bloscpack. See the warranty disclaimer
in the licence at the end of this file.

Installation
------------

Disclaimer: There are a myriad ways of installing Python packages (and their
dependencies) these days and it is a futile endeavour to explain the procedures
in great detail again and again. Below are three methods that are known to
work. Depending on the method you choose and the system your are using you may
require any or all of: super user privileges, a C++ compiler and/or a virtual
environment. If you do run into problems or are unsure, it's best to send an
email to the aforementioned mailing list asking for help.

The package is available on PyPi, so you may use `pip` to install the
dependencies and bloscpack itself:

.. code-block:: console

    $ pip install bloscpack

The package is also available on anaconda.org via conda-forge. You may use
`conda` to install it:

.. code-block:: console

    $ conda install -c conda-forge bloscpack

If you want to install straight from GitHub, use pip's VCS support:

.. code-block:: console

    $ pip install git+https://github.com/Blosc/bloscpack

Or, of course, download the source code or clone the repository and then use
the standard ``setup.py``:

.. code-block:: console

    $ git clone https://github.com/Blosc/bloscpack
    $ cd bloscpack
    $ python setup.py install

Usage
-----

Bloscpack is accessible from the command line using the ``blpk`` executable
this has a number of global options and four subcommands: ``[c | compress]``,
``[d | decompress]``, ``[a | append]`` and ``[i | info]`` most of which each
have their own options.

Help for global options and subcommands:

.. code-block:: console

    $ blpk --help
    [...]

Help for each one of the subcommands:

.. code-block:: console

    $ blpk compress --help
    [...]
    $ blpk decompress --help
    [...]
    $ blpk info --help
    [...]
    $ blpk append --help
    [...]

Examples
--------

Basics
~~~~~~

Basic compression:

.. code-block:: console

    $ blpk compress data.dat

Or:

.. code-block:: console

    $ blpk c data.dat

... will compress the file ``data.dat`` to ``data.dat.blp``

Basic decompression:

.. code-block:: console

    $ blpk decompress data.dat.blp data.dcmp

Or:

.. code-block:: console

    $ blpk d data.dat.blp data.dcmp

... will decompress the file ``data.dat.blp`` to the file ``data.dcmp``. If you
leave out the ``[<out_file>]`` argument, Bloscpack will complain that the file
``data.dat`` exists already and refuse to overwrite it:

.. code-block:: console

    $ blpk decompress data.dat.blp
    blpk: error: output file 'data.dat' exists!

If you know what you are doing, you can use the global option ``[-f |
--force]`` to override the overwrite checks:

.. code-block:: console

    $ blpk --force decompress data.dat.blp

Incidentally this works for compression too:

.. code-block:: console

    $ blpk compress data.dat
    blpk: error: output file 'data.dat.blp' exists!
    $ blpk --force compress data.dat

Lastly, if you want a different filename:

.. code-block:: console

    $ blpk compress data.dat custom.filename.blp

... will compress the file ``data.dat`` to ``custom.filename.blp``

Settings
~~~~~~~~

By default, the number of threads that Blosc uses during compression and
decompression is determined by the number of cores detected on your system.
You can change this using the ``[-n | --nthreads]`` option:

.. code-block:: console

    $ blpk --nthreads 1 compress data.dat

Compression with Blosc is controlled with the following options:

* ``[-t | --typesize]``
  Typesize used by Blosc (default: 8):
  ``$ blpk compress --typesize 8 data.dat``
* ``[-l | --level]``
  Compression level (default: 7):
  ``$ blpk compress --level 3 data.dat``
* ``[-s | --no-shuffle]``
  Deactivate shuffle:
  ``$ blpk compress --no-shuffle data.dat``
* ``[-c | --codec]``
  Use alternative codec:
  ``$ blpk compress --codec lz4 data.dat``

In addition, there are the following options that control the Bloscpack file:

* ``[-z | --chunk-size]``
  Desired approximate size of the chunks, where you can use human readable
  strings like ``8M`` or ``128K`` or ``max`` to use the maximum chunk size of
  apprx. ``2GB`` (default: ``1MB``):
  ``$ blpk compress --chunk-size 128K data.dat`` or
  ``$ blpk c -z max data.dat``
* ``[-k | --checksum <checksum>]``
  Chose which checksum to use. The following values are permissible:
  ``None``, ``adler32``, ``crc32``, ``md5``,
  ``sha1``, ``sha224``, ``sha256``, ``sha384``,
  ``sha512``, (default: ``adler32``). As described in the header format, each
  compressed chunk can be stored with a checksum, which aids corruption
  detection on decompression:
  ``$ blpk compress --checksum crc32 data.dat``
* ``[-o | --no-offsets]``
  By default, offsets to the individual chunks are stored. These are included
  to allow for partial decompression in the future. This option disables that
  feature. Also, a certain number of offsets (default: 10 * 'nchunks') are
  preallocated to allow for appending data to the file:
  ``$ blpk compress --no-offsets data.dat``

Info Subcommand
~~~~~~~~~~~~~~~

If you just need some info on how the file was compressed ``[i | info]``:

.. code-block:: console

    $ blpk info data.dat.blp
    blpk: BloscpackHeader:
    blpk:     format_version: 3
    blpk:     offsets: True
    blpk:     metadata: False
    blpk:     checksum: 'adler32'
    blpk:     typesize: 8
    blpk:     chunk_size: 1.0M (1048576B)
    blpk:     last_chunk: 900.0K (921600B)
    blpk:     nchunks: 1526
    blpk:     max_app_chunks: 15260
    blpk: 'offsets':
    blpk: [134320,459218,735869,986505,1237646,...]
    blpk: First chunk blosc header:
    blpk: OrderedDict([('version', 2), ('versionlz', 1), ('flags', 1), ('typesize', 8), ('nbytes', 1048576), ('blocksize', 131072), ('ctbytes', 324894)])
    blpk: First chunk blosc flags:
    blpk: OrderedDict([('byte_shuffle', True), ('pure_memcpy', False), ('bit_shuffle', False), ('split_blocks', False), ('codec', 'blosclz')])

Importantly, the header and flag information are for the first chunk only.
Usually this isn't a problem because bloscpack compressed files do tend to have
homogeneous settings like codec used, typesize etc... However, there is nothing
that will stop you from appending to an existing bloscpack file using different
settings. For example, half the file might be compressed using 'blosclz'
whereas the other half of the file might be compressed with 'lz4'. In any case,
just be aware that the output is to be seen as an indication that is likely to
be correct for all chunks but must not be so necessarily.

Adding Metdata
~~~~~~~~~~~~~~

Using the ``[-m | --metadata]`` option you can include JSON from a file:

.. code-block:: console

   $ cat meta.json
   {"dtype": "float64", "shape": [200000000], "container": "numpy"}
   $ blpk compress --chunk-size=512M --metadata meta.json data.dat
   $ blpk info data.dat.blp
   blpk: BloscpackHeader:
   blpk:     format_version: 3
   blpk:     offsets: True
   blpk:     metadata: True
   blpk:     checksum: 'adler32'
   blpk:     typesize: 8
   blpk:     chunk_size: 512.0M (536870912B)
   blpk:     last_chunk: 501.88M (526258176B)
   blpk:     nchunks: 3
   blpk:     max_app_chunks: 30
   blpk: 'offsets':
   blpk: [922,78074943,140783242,...]
   blpk: 'metadata':
   blpk: {   u'container': u'numpy', u'dtype': u'float64', u'shape': [200000000]}
   blpk: MetadataHeader:
   blpk:     magic_format: 'JSON'
   blpk:     meta_options: '00000000'
   blpk:     meta_checksum: 'adler32'
   blpk:     meta_codec: 'zlib'
   blpk:     meta_level: 6
   blpk:     meta_size: 59.0B (59B)
   blpk:     max_meta_size: 590.0B (590B)
   blpk:     meta_comp_size: 58.0B (58B)
   blpk:     user_codec: ''

It will be printed when decompressing:

.. code-block:: console

    $ blpk decompress data.dat.blp
    blpk: Metadata is:
    blpk: '{u'dtype': u'float64', u'shape': [200000000], u'container': u'numpy'}'

Appending
~~~~~~~~~

You can also append data to an existing bloscpack compressed file:

.. code-block:: console

   $ blpk append data.dat.blp data.dat

However there are certain limitations on the amount of data can be appended.
For example, if there is an offsets section, there must be enough room to store
the offsets for the appended chunks. If no offsets exists, you may append as
much data as possible given the limitations governed by the maximum number of
chunks and the chunk-size. Additionally, there are limitations on the
compression options. For example, one cannot change the checksum used. It is
however possible to change the compression level, the typesize and the shuffle
option for the appended chunks.

Also note that appending is still considered experimental as of ``v0.5.0``.

Verbose and Debug mode
~~~~~~~~~~~~~~~~~~~~~~

Lastly there are two mutually exclusive options to control how much output is
produced.

The first causes basic info to be printed, ``[-v | --verbose]``:

.. code-block:: console

    $ blpk --verbose compress --chunk-size 0.5G data.dat
    blpk: using 4 threads
    blpk: getting ready for compression
    blpk: input file is: 'data.dat'
    blpk: output file is: 'data.dat.blp'
    blpk: input file size: 1.49G (1600000000B)
    blpk: nchunks: 3
    blpk: chunk_size: 512.0M (536870912B)
    blpk: last_chunk_size: 501.88M (526258176B)
    blpk: output file size: 198.39M (208028617B)
    blpk: compression ratio: 7.691250
    blpk: done

... and ``[-d | --debug]`` prints a detailed account of what is going on:

.. code-block:: console

    $ blpk --debug compress --chunk-size 0.5G data.dat
    blpk: command line argument parsing complete
    blpk: command line arguments are:
    blpk:     force: False
    blpk:     verbose: False
    blpk:     offsets: True
    blpk:     checksum: adler32
    blpk:     subcommand: compress
    blpk:     out_file: None
    blpk:     metadata: None
    blpk:     cname: blosclz
    blpk:     in_file: data.dat
    blpk:     chunk_size: 536870912
    blpk:     debug: True
    blpk:     shuffle: True
    blpk:     typesize: 8
    blpk:     clevel: 7
    blpk:     nthreads: 4
    blpk: using 4 threads
    blpk: getting ready for compression
    blpk: input file is: 'data.dat'
    blpk: output file is: 'data.dat.blp'
    blpk: input file size: 1.49G (1600000000B)
    blpk: nchunks: 3
    blpk: chunk_size: 512.0M (536870912B)
    blpk: last_chunk_size: 501.88M (526258176B)
    blpk: BloscArgs:
    blpk:     typesize: 8
    blpk:     clevel: 7
    blpk:     shuffle: True
    blpk:     cname: 'blosclz'
    blpk: BloscpackArgs:
    blpk:     offsets: True
    blpk:     checksum: 'adler32'
    blpk:     max_app_chunks: <function <lambda> at 0x1182de8>
    blpk: metadata_args will be silently ignored
    blpk: max_app_chunks is a callable
    blpk: max_app_chunks was set to: 30
    blpk: BloscpackHeader:
    blpk:     format_version: 3
    blpk:     offsets: True
    blpk:     metadata: False
    blpk:     checksum: 'adler32'
    blpk:     typesize: 8
    blpk:     chunk_size: 512.0M (536870912B)
    blpk:     last_chunk: 501.88M (526258176B)
    blpk:     nchunks: 3
    blpk:     max_app_chunks: 30
    blpk: raw_bloscpack_header: 'blpk\x03\x01\x01\x08\x00\x00\x00 \x00\x10^\x1f\x03\x00\x00\x00\x00\x00\x00\x00\x1e\x00\x00\x00\x00\x00\x00\x00'
    blpk: Handle chunk '0'
    blpk: checksum (adler32): '\x1f\xed\x1e\xf4'
    blpk: chunk handled, in: 512.0M (536870912B) out: 74.46M (78074017B)
    blpk: Handle chunk '1'
    blpk: checksum (adler32): ')\x1e\x08\x88'
    blpk: chunk handled, in: 512.0M (536870912B) out: 59.8M (62708295B)
    blpk: Handle chunk '2' (last)
    blpk: checksum (adler32): '\xe8\x18\xa4\xac'
    blpk: chunk handled, in: 501.88M (526258176B) out: 64.13M (67245997B)
    blpk: Writing '3' offsets: '[296, 78074317, 140782616]'
    blpk: Raw offsets: '(\x01\x00\x00\x00\x00\x00\x00\xcdQ\xa7\x04\x00\x00\x00\x00\x18,d\x08\x00\x00\x00\x00'
    blpk: output file size: 198.39M (208028617B)
    blpk: compression ratio: 7.691250
    blpk: done


Python API
----------

Bloscpack has a versatile yet simple API consisting of a series of 'arguments'
objects and high-level functions that can be invoked dependding on your input
and output needs.

Nomenclature wise, Python 3 has done a lot for Bloscpack, because we always
need to represent compressed data as bytes deliberatey. This makes it easier
and more natural to distinguish between text, such a filenames and binary and
bytes objects such as compressed data.

Arguments
~~~~~~~~~

The three argument types are:

* ``BloscArgs``
* ``BloscpackArgs``
* ``MetadataArgs``

as defined in ``bloscpack/args.py``.  Instantiating any of them will create an
object with the defaults setup. The defaults are defined in
``bloscpack/defaults.py``. You can use these in the high-level functions listed
below.

You can override any and all defaults by passing in the respective
keyword-arguments, for example:


.. code-block:: pycon

   >>> b = BloscArgs()               # will create a default args object
   >>> b = BloscArgs(clevel=4)       # change compression level to 4
   >>> b = BloscArgs(typesize=4,     # change the typesize to 4
   >>> ...           clevel=9,       # change the compression level to 9
   >>> ...           shuffle=False,  # deactivate the shuffle filter
   >>> ...           cname='lz4')    # let lz4 be the internal codec


.. code-block:: python

    class BloscArgs(MutableMappingObject):
        """ Object to hold Blosc arguments.

        Parameters
        ----------
        typesize : int
            The typesize used
        clevel : int
            Compression level
        shuffle : boolean
            Whether or not to activate the shuffle filter
        cname: str
            Name of the internal code to use

        """

.. code-block:: python

    class BloscpackArgs(MutableMappingObject):
        """ Object to hold BloscPack arguments.

        Parameters
        ----------
        offsets : boolean
            Whether to include space for offsets
        checksum : str
            Name of the checksum to use or None/'None'
        max_app_chunks : int or callable on number of chunks
            How much space to reserve in the offsets for chunks to be appended.

        """

.. code-block:: python

    class MetadataArgs(MutableMappingObject):
        """ Object to hold the metadata arguments.

        Parameters
        ----------
        magic_format : 8 bytes
            Format identifier for the metadata
        meta_checksum : str
            Checksum to be used for the metadata
        meta_codec : str
            Codec to be used to compress the metadata
        meta_level : int
            Compression level for metadata
        max_meta_size : int or callable on metadata size
            How much space to reserve for additional metadata

        """

File / Bytes
~~~~~~~~~~~~

The following high-level functions exist for compressing and decompressing to
and from files and byte objects:


* ``pack_file_to_file``
* ``unpack_file_from_file``
* ``pack_bytes_to_file``
* ``unpack_bytes_from_file``
* ``pack_bytes_to_bytes``
* ``unpack_bytes_from_bytes``

Beyond the target arguments such as the files and the bytes, each ``pack_*``
function takes the following arguments:

.. code-block::

    chunk_size : int
        the desired chunk size in bytes
    metadata : dict
        the metadata dict
    blosc_args : BloscArgs
        blosc args
    bloscpack_args : BloscpackArgs
        bloscpack args
    metadata_args : MetadataArgs
        metadata args

Below are their sigantures:

.. code-block:: python

    def pack_file_to_file(in_file, out_file,
                          chunk_size=DEFAULT_CHUNK_SIZE,
                          metadata=None,
                          blosc_args=None,
                          bloscpack_args=None,
                          metadata_args=None):

    def unpack_file_from_file(in_file, out_file):


    def pack_bytes_to_file(bytes_, out_file,
                           chunk_size=DEFAULT_CHUNK_SIZE,
                           metadata=None,
                           blosc_args=None,
                           bloscpack_args=None,
                           metadata_args=None):

    def unpack_bytes_from_file(compressed_file):

    def pack_bytes_to_bytes(bytes_,
                            chunk_size=DEFAULT_CHUNK_SIZE,
                            metadata=None,
                            blosc_args=None,
                            bloscpack_args=None,
                            metadata_args=None,
                            ):


    def unpack_bytes_from_bytes(bytes_):

Numpy
~~~~~

Numpy arrays can be serialized as Bloscpack files, here is a very brief example:

.. code-block:: pycon

    >>> a = np.linspace(0, 1, 3e8)
    >>> print a.size, a.dtype
    300000000 float64
    >>> bp.pack_ndarray_to_file(a, 'a.blp')
    >>> b = bp.unpack_ndarray_from_file('a.blp')
    >>> (a == b).all()
    True

Looking at the generated file, we can see the Numpy metadata being saved:

.. code-block:: console

    $ lh a.blp
    -rw------- 1 esc esc 266M Aug 13 23:21 a.blp

    $ blpk info a.blp
    blpk: BloscpackHeader:
    blpk:     format_version: 3
    blpk:     offsets: True
    blpk:     metadata: True
    blpk:     checksum: 'adler32'
    blpk:     typesize: 8
    blpk:     chunk_size: 1.0M (1048576B)
    blpk:     last_chunk: 838.0K (858112B)
    blpk:     nchunks: 2289
    blpk:     max_app_chunks: 22890
    blpk: 'offsets':
    blpk: [202170,408064,554912,690452,819679,...]
    blpk: 'metadata':
    blpk: {   u'container': u'numpy',
    blpk:     u'dtype': u'<f8',
    blpk:     u'order': u'C',
    blpk:     u'shape': [300000000]}
    blpk: MetadataHeader:
    blpk:     magic_format: 'JSON'
    blpk:     meta_options: '00000000'
    blpk:     meta_checksum: 'adler32'
    blpk:     meta_codec: 'zlib'
    blpk:     meta_level: 6
    blpk:     meta_size: 67.0B (67B)
    blpk:     max_meta_size: 670.0B (670B)
    blpk:     meta_comp_size: 62.0B (62B)
    blpk:     user_codec: ''

Alternatively, we can also use a string as storage:

.. code-block:: pycon

    >>> a = np.linspace(0, 1, 3e8)
    >>> c = pack_ndarray_to_bytes(a)
    >>> b = unpack_ndarray_from_bytes(c)
    >>> (a == b).all()
    True

Or use alternate compressors:

.. code-block:: pycon

    >>> a = np.linspace(0, 1, 3e8)
    >>> c = pack_ndarray_to_bytes(a, blosc_args=BloscArgs(cname='lz4'))
    >>> b = unpack_ndarray_from_bytes(c)
    >>> (a == b).all()
    True


.. code-block:: python

    def pack_ndarray_to_file(ndarray, filename,
                             chunk_size=DEFAULT_CHUNK_SIZE,
                             blosc_args=None,
                             bloscpack_args=None,
                             metadata_args=None):

    def pack_ndarray_to_bytes(ndarray,
                              chunk_size=DEFAULT_CHUNK_SIZE,
                              blosc_args=None,
                              bloscpack_args=None,
                              metadata_args=None):

    def unpack_ndarray_from_file(filename):

    def unpack_ndarray_from_bytes(str_):

If you are interested in the performance of Bloscpack compared to other
serialization formats for Numpy arrays, please look at the benchmarks presented
in `the Bloscpack paper from the EuroScipy 2013 conference proceedings
<http://arxiv.org/abs/1404.6383>`_.

Testing
-------

Installing Dependencies
~~~~~~~~~~~~~~~~~~~~~~~

Testing requires some additional libraries, which you can install from PyPi
with:

.. code-block:: console

    $ pip install -r test_requirements.txt
    [...]


Basic Tests
~~~~~~~~~~~

Basic tests, runs quickly:

.. code-block:: console

    $ PYTHONPATH=. pytest -m "not slow"
    [...]


Heavier Tests
~~~~~~~~~~~~~

Extended tests using a larger file, may take some time, but will be nice to
memory:

.. code-block:: console

    $ PYTHONPATH=. pytest test/test_file_io.py::test_pack_unpack_hard
    [...]

Extended tests using a huge file. This one take forever and needs loads (5G-6G)
of memory and loads of disk-space (10G). Use ``-s`` to print progress:

.. code-block:: console

    $ PYTHONPATH=. test/test_file_io.py::test_pack_unpack_extreme
    [...]

Note that, some compression/decompression tests create temporary files (on
UNIXoid systems this is under ``/tmp/blpk*``) which are deleted upon completion
of the respective test, both successful and unsuccessful, or when the test is
aborted with e.g. ``ctrl-c`` (using ``atexit`` magic).

Under rare circumstances, for example when aborting the deletion which is
triggered on abort you may be left with large files polluting your temporary
space.  Depending on your partitioning scheme etc.. doing this repeatedly, may
lead to you running out of space on the file-system.

Command Line Interface Tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The command line interface is tested with `cram <https://bitheap.org/cram/>`_:

.. code-block:: console

   $ cram --verbose test_cmdline/*.cram
   [...]


Coverage
~~~~~~~~

To determine coverage you can pool together the coverage from the cram tests and
the unit tests:

.. code-block:: console

    $ COVERAGE=1 cram --verbose test_cmdline/*.cram
    [...]
    $ PYTHONPATH=. pytest --cov=bloscpack --cov-append -m "not slow" test
    [...]

Test Runner
~~~~~~~~~~~

To run the command line interface tests and the unit tests and analyse
coverage, use the convenience ``test.sh`` runner:

.. code-block:: console

   $ ./test.sh
   [...]

Benchmark
---------

Using the provided ``bench/blpk_vs_gzip.py`` script on a ``Intel(R) Core(TM)
i7-3667U CPU @ 2.00GHz`` CPU with 2 cores and 4 threads (active
hyperthreading), cpu frequency scaling activated but set to the ``performance``
governor (all cores scaled to ``2.0 GHz``), 8GB of DDR3 memory and a Luks encrypted
SSD, we get:

.. code-block:: console

    $ PYTHONPATH=. ./bench/blpk_vs_gzip.py
    create the test data..........done

    Input file size: 1.49G
    Will now run bloscpack...
    Time: 2.06 seconds
    Output file size: 198.55M
    Ratio: 7.69
    Will now run gzip...
    Time: 134.20 seconds
    Output file size: 924.05M
    Ratio: 1.65

As was expected from previous benchmarks of Blosc using the python-blosc
bindings, Blosc is both much faster and has a better compression ratio for this
kind of structured data. One thing to note here, is that we are not dropping
the system file cache after every step, so the file to read will be cached in
memory. To get a more accurate picture we can use the ``--drop-caches`` switch
of the benchmark which requires you however, to run the benchmark as root,
since dropping the caches requires root privileges:

.. code-block:: console

    $ PYTHONPATH=. ./bench/blpk_vs_gzip.py --drop-caches
    will drop caches
    create the test data..........done

    Input file size: 1.49G
    Will now run bloscpack...
    Time: 13.49 seconds
    Output file size: 198.55M
    Ratio: 7.69
    Will now run gzip...
    Time: 137.49 seconds
    Output file size: 924.05M
    Ratio: 1.65

Optimizing Chunk Size
---------------------

You can use the provided ``bench/compression_time_vs_chunk_size.py`` file
to optimize the chunk-size for a given machine. For example:

.. code-block:: console

    $ sudo env PATH=$PATH PYTHONPATH=.  bench/compression_time_vs_chunk_size.py
    create the test data..........done
    chunk_size    comp-time       decomp-time      ratio
    512.0K        8.106235        10.243908        7.679094
    724.08K       4.424007        12.284307        7.092846
    1.0M          6.243544        11.978932        7.685173
    1.41M         4.715511        10.780901        7.596981
    2.0M          4.548568        10.676304        7.688216
    2.83M         4.851359        11.668394        7.572480
    4.0M          4.557665        10.127647        7.689736
    5.66M         4.589349        9.579627         7.667467
    8.0M          5.290080        10.525652        7.690499

Running the script requires super user privileges, since you need to
synchronize disk writes and drop the file system caches for less noisy results.
Also, you should probably run this script a couple of times and inspect the
variability of the results.


Bloscpack Format
----------------

The input is split into chunks since a) we wish to put less stress on main
memory and b) because Blosc has a buffer limit of ``2GB`` (Version ``1.0.0`` and
above). By default the chunk-size is a moderate ``1MB`` which should be fine,
even for less powerful machines.

In addition to the chunks some additional information must be added to the file
for housekeeping:

:header:
    a 32 byte header containing various pieces of information
:meta:
    a variable length metadata section, may contain user data
:offsets:
    a variable length section containing chunk offsets
:chunk:
    the blosc chunk(s)
:checksum:
    a checksum following each chunk, if desired

The layout of the file is then::

    |-header-|-meta-|-offsets-|-chunk-|-checksum-|-chunk-|-checksum-|...|

Description of the header
~~~~~~~~~~~~~~~~~~~~~~~~~
The following 32 byte header is used for Bloscpack as of version ``0.3.0``.  The
design goals of the header format are to contain as much information as
possible to achieve interesting things in the future and to be as general as
possible such that the persistence layer of `Blaze
<https://github.com/ContinuumIO/blaze>`_/`BLZ
<https://github.com/ContinuumIO/blz/tree/master>`_ can be implemented without
modification of the header format.

The following ASCII representation shows the layout of the header::

    |-0-|-1-|-2-|-3-|-4-|-5-|-6-|-7-|-8-|-9-|-A-|-B-|-C-|-D-|-E-|-F-|
    | b   l   p   k | ^ | ^ | ^ | ^ |   chunk-size  |  last-chunk   |
                      |   |   |   |
          version ----+   |   |   |
          options --------+   |   |
         checksum ------------+   |
         typesize ----------------+

    |-0-|-1-|-2-|-3-|-4-|-5-|-6-|-7-|-8-|-9-|-A-|-B-|-C-|-D-|-E-|-F-|
    |            nchunks            |        max-app-chunks         |

The first 4 bytes are the magic string ``blpk``. Then there are 4 bytes which
hold information about the activated features in this file.  This is followed
by 4 bytes for the ``chunk-size``, another 4 bytes for the ``last-chunk-size``,
8 bytes for the number of chunks, ``nchunks`` and lastly 8 bytes for the total
number of chunks that can be appended to this file, ``max-app-chunks``.

Effectively, storing the number of chunks as a signed 8 byte integer, limits
the number of chunks to ``2**63-1 = 9223372036854775807``, but this should not
be relevant in practice, since, even with the moderate default value of ``1MB``
for chunk-size, we can still store files as large as ``8ZB`` (!) Given that
in 2012 the maximum size of a single file in the Zettabye File System (zfs) is
``16EB``, Bloscpack should be safe for a few more years.

Description of the header entries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All entries are little-endian.

:version:
    (``uint8``)
    format version of the Bloscpack header, to ensure exceptions in case of
    forward incompatibilities.
:options:
    (``bitfield``)
    A bitfield which allows for setting certain options in this file.

    :``bit 0 (0x01)``:
        If the offsets to the chunks are present in this file.
    :``bit 1 (0x02)``:
        If metadata is present in this file.

:checksum:
    (``uint8``)
    The checksum used. The following checksums, available in the python
    standard library should be supported. The checksum is always computed on
    the compressed data and placed after the chunk.

    :``0``:
        ``no checksum``
    :``1``:
        ``zlib.adler32``
    :``2``:
        ``zlib.crc32``
    :``3``:
        ``hashlib.md5``
    :``4``:
        ``hashlib.sha1``
    :``5``:
        ``hashlib.sha224``
    :``6``:
        ``hashlib.sha256``
    :``7``:
        ``hashlib.sha384``
    :``8``:
        ``hashlib.sha512``
:typesize:
    (``uint8``)
    The typesize of the data in the chunks. Currently, assume that the typesize
    is uniform. The space allocated is the same as in the Blosc header.
:chunk-size:
    (``int32``)
    Denotes the chunk-size. Since the maximum buffer size of Blosc is 2GB
    having a signed 32 bit int is enough (``2GB = 2**31 bytes``). The special
    value of ``-1`` denotes that the chunk-size is unknown or possibly
    non-uniform.
:last-chunk:
    (``int32``)
    Denotes the size of the last chunk. As with the ``chunk-size`` an ``int32``
    is enough. Again, ``-1`` denotes that this value is unknown.
:nchunks:
    (``int64``)
    The total number of chunks used in the file. Given a chunk-size of one
    byte, the total number of chunks is ``2**63``. This amounts to a maximum
    file-size of 8EB (``8EB = 2*63 bytes``) which should be enough for the next
    couple of years. Again, ``-1`` denotes that the number of is unknown.
:max-app-chunks:
    (``int64``)
    The maximum number of chunks that can be appended to this file, excluding
    ``nchunks``. This is only useful if there is an offsets section and if
    nchunks is known (not ``-1``), if either of these conditions do not apply
    this should be ``0``.

The overall file-size can be computed as ``chunk-size * (nchunks - 1) +
last-chunk-size``. In a streaming scenario ``-1`` can be used as a placeholder.
For example if the total number of chunks, or the size of the last chunk is not
known at the time the header is created.

The following constraints exist on the header entries:

* ``last-chunk`` must be less than or equal to ``chunk-size``.
* ``nchunks + max_app_chunks`` must be less than or equal to the maximum value
  of an ``int64``.


Description of the metadata section
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This section goes after the header. It consists of a metadata-section header
followed by a serialized and potentially compressed data section, followed by
preallocated space to resize the data section, possibly followed by a checksum.

The layout of the section is thus::

    |-metadata-header-|-data-|-prealloc-|-checksum-|

The header has the following layout::

   |-0-|-1-|-2-|-3-|-4-|-5-|-6-|-7-|-8-|-9-|-A-|-B-|-C-|-D-|-E-|-F-|
   |         magic-format          | ^ | ^ | ^ | ^ |   meta-size   |
                                     |   |   |   |
                 meta-options -------+   |   |   |
                 meta-checksum ----------+   |   |
                 meta-codec -----------------+   |
                 meta-level ---------------------+

   |-0-|-1-|-2-|-3-|-4-|-5-|-6-|-7-|-8-|-9-|-A-|-B-|-C-|-D-|-E-|-F-|
   | max-meta-size |meta-comp-size |            user-codec         |

:magic-format:
    (``8 byte ASCII string``)
    The data will usually be some kind of binary serialized string data, for
    example ``JSON``, ``BSON``, ``YAML`` or Protocol-Buffers. The format
    identifier is to be placed in this field.
:meta-options:
    (``bitfield``)
    A bitfield which allows for setting certain options in this metadata
    section. Currently unused
:meta-checksum:
    The checksum used for the metadata. The same checksums as for the data are
    available.
:meta-codec:
    (``unit8``)
    The codec used for compressing the metadata. As of Bloscpack version
    ``0.3.0`` the following codecs are supported.

    :``0``:
        no codec
    :``1``:
        ``zlib`` (DEFLATE)

:meta-level:
    (``unit8``)
    The compression level used for the codec. If ``codec`` is ``0`` i.e. the
    metadata is not compressed, this must be ``0`` too.
:meta-size:
    (``uint32``)
    The size of the uncompressed metadata.
:max-meta-size:
    (``uint32``)
    The total allocated space for the data section.
:meta-comp-size:
    (``uint32``)
    If the metadata is compressed, this gives the total space the metadata
    occupies. If the data is not compressed this is the same as ``meta-size``.
    In a sense this is the true amount of space in the metadata section that is
    used.
:user-codec:
    Space reserved for usage of additional codecs. E.g. 4 byte magic string for
    codec identification and 4 bytes for encoding of codec parameters.

The total space left for enlarging the metadata section is simply:
``max-meta-size - meta-comp-size``.

JSON Example of serialized metadata::

  '{"dtype": "float64", "shape": [1024], "others": []}'

If compression is requested, but not beneficial, because the compressed size
would be larger than the uncompressed size, compression of the metadata is
automatically deactivated.

As of Bloscpack version ``0.3.0`` only the JSON serializer is supported and
used the string ``JSON`` followed by four whitespace bytes as identifier.
Since JSON and any other of the suggested serializers has limitations, only a
subset of Python structures can be stored, so probably some additional object
handling must be done prior to serialize certain kinds of metadata.

Description of the offsets entries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Following the metadata section, comes a variable length section of chunk
offsets. Offsets of the chunks into the file are to be used for accelerated
seeking. The offsets (if activated) follow the header. Each offset is a 64 bit
signed little-endian integer (``int64``). A value of ``-1`` denotes an unknown
offset. Initially, all offsets should be initialized to ``-1`` and filled in
after writing all chunks. Thus, If the compression of the file fails
prematurely or is aborted, all offsets should have the value ``-1``.  Also, any
unused offset entries preallocated to allow the file to grow should be set to
``-1``. Each offset denotes the exact position of the chunk in the file such
that seeking to the offset, will position the file pointer such that, reading
the next 16 bytes gives the Blosc header, which is at the start of the desired
chunk.

Description of the chunk format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As mentioned previously, each chunk is just a Blosc compressed string including
header. The Blosc header (as of ``v1.0.0``) is 16 bytes as follows::

    |-0-|-1-|-2-|-3-|-4-|-5-|-6-|-7-|-8-|-9-|-A-|-B-|-C-|-D-|-E-|-F-|
      ^   ^   ^   ^ |     nbytes    |   blocksize   |    ctbytes    |
      |   |   |   |
      |   |   |   +--typesize
      |   |   +------flags
      |   +----------versionlz
      +--------------version

The first four are simply bytes, the last three are are each unsigned ints
(``uint32``) each occupying 4 bytes. The header is always little-endian.
``ctbytes`` is the length of the buffer including header and ``nbytes`` is the
length of the data when uncompressed. A more detailed description of the Blosc
header can be found in the `README_HEADER.rst of the Blosc repository
<https://github.com/FrancescAlted/blosc/blob/master/README_HEADER.rst>`_

Overhead
~~~~~~~~

Depending on which configuration for the file is used a constant, or linear
overhead may be added to the file. The Bloscpack header adds 32 bytes in any
case. If the data is non-compressible, Blosc will add 16 bytes of header to
each chunk. The metadata section obviously adds a constant overhead, and if
used, both the checksum and the offsets will add overhead to the file. The
offsets add 8 bytes per chunk and the checksum adds a fixed constant value
which depends on the checksum to each chunk. For example, 32 bytes for the
``adler32`` checksum.

Coding Conventions
------------------

* Numpy rst style docstrings
* README cli examples should use long options
* testing: expected before received ``nt.assert_equal(expected, received)``
* Debug messages: as close to where the data was generated
* Single quotes around ambiguities in messages ``overwriting existing file: 'testfile'``
* Exceptions instead of exit
* Use the Wikipedia definition of compression ratio:
  http://en.wikipedia.org/wiki/Data_compression_ratio

How to Optimize Logging
-----------------------

Some care must be taken when logging in the inner loop. For example consider the
following two commits:

* https://github.com/Blosc/bloscpack/commit/0854930514eebaf7dbc6c4dcf3589dbcb9f2fdc9

* https://github.com/Blosc/bloscpack/commit/355bf90a8c13a2a1f792d43228c2a68c61476621

If there are a larger number of chunks, calls to ``double_pretty_size`` will be
executed (and may be costly) *even* if no logging is needed.

Consider the following script, ``loop-bench.py``:

.. code-block:: python

    import numpy as np
    import bloscpack as bp
    import blosc

    shuffle = True
    clevel = 9
    cname = 'lz4'

    a = np.arange(2.5e8)

    bargs = bp.args.BloscArgs(clevel=clevel, shuffle=shuffle, cname=cname)
    bpargs = bp.BloscpackArgs(offsets=False, checksum='None', max_app_chunks=0)

Timing with ``v0.7.0``:

.. code-block:: pycon

    In [1]: %run loop-bench.py

    In [2]: %timeit bpc = bp.pack_ndarray_str(a, blosc_args=bargs, bloscpack_args=bpargs)
    1 loops, best of 3: 423 ms per loop

    In [3]: %timeit bpc = bp.pack_ndarray_str(a, blosc_args=bargs, bloscpack_args=bpargs)
    1 loops, best of 3: 421 ms per loop

    In [4]: bpc = bp.pack_ndarray_str(a, blosc_args=bargs, bloscpack_args=bpargs)

    In [5]: %timeit a3 = bp.unpack_ndarray_str(bpc)
    1 loops, best of 3: 727 ms per loop

    In [6]: %timeit a3 = bp.unpack_ndarray_str(bpc)
    1 loops, best of 3: 725 ms per loop

And then using a development version that contains the two optimization commits:

.. code-block:: pycon

    In [1]: %run loop-bench.py

    In [2]: %timeit bpc = bp.pack_ndarray_str(a, blosc_args=bargs, bloscpack_args=bpargs)
    1 loops, best of 3: 357 ms per loop

    In [3]: %timeit bpc = bp.pack_ndarray_str(a, blosc_args=bargs, bloscpack_args=bpargs)
    1 loops, best of 3: 357 ms per loop

    In [4]: bpc = bp.pack_ndarray_str(a, blosc_args=bargs, bloscpack_args=bpargs)

    In [5]: %timeit a3 = bp.unpack_ndarray_str(bpc)
    1 loops, best of 3: 658 ms per loop

    In [6]: %timeit a3 = bp.unpack_ndarray_str(bpc)
    1 loops, best of 3: 655 ms per loop

Comparison to HDF5/PyTables
---------------------------

Since Blosc has already been supported for use in HDF5 files from within
PyTables, one might be tempted to question why yet another file format has to
be invented. This section aims to differentiate between HDF5/PyTables and
effectively argues that they are not competitors.

* Lightweight vs. Heavyweight. Bloscpack is a lightweight format. The format
  specification can easily be digested within a day and the dependencies are
  minimal. PyTables is a complex piece of software and the HDF5 file format
  specification is a large document.

* Persistence vs. Database. Bloscpack is designed to allow for fast
  serialization and deserialization of in-memory data. PyTables is more of a
  database which for example allows complex queries to be computed on the
  data.

Additionally there are two network uses cases which Bloscpack is suited for
(but does not have support for as of yet):

#. Streaming: Since bloscpack without offsets can be written in a single
   pass it is ideally suited for streaming over a network, where you can
   compress send and decompress individual chunks in a streaming fashion.

#. Expose a file over HTTP and do partial reads from it, for example when
   storing a compressed file in S3. You can easily just store a file on a
   web server and then use the header information to read and decompress
   individual chunks.

Prior Art
---------

The following is a  list of important resources that were read during the
conception and initial stages of Bloscpack.

* The `6pack utility included with FastLZ
  <https://github.com/ariya/FastLZ/blob/master/6pack.c>`_ (the codec that
  BloscLZ was derived from) was the initial inspiration for writing a command
  line interface to Blosc.

* The `Wikipedia article on the PNG format
  <http://en.wikipedia.org/wiki/Portable_Network_Graphics>`_ contains some
  interesting details about the PNG header and file headers in general.

* The `XZ File Format Specification
  <http://tukaani.org/xz/xz-file-format.txt>`_ gave rise to some ideas and
  techniques about writing file format specifications and using checksums for
  data integrity. Although the format and the document itself was a bit to
  heavyweight for my tastes.

* The `Snappy framing format
  <http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt>`_
  and the `file container format for LZ4
  <http://fastcompression.blogspot.de/2012/04/file-container-format-for-lz4.html>`_
  were also consulted, but I can't remember if and what inspiration they gave
  rise to.

* The homepages of `zlib <http://www.zlib.net/>`_ and `gzip
  <http://www.gzip.org/>`_ were also consulted at some point. The command line
  interface of `gzip/gunzip` was deemed to be from a different era and as a
  result git-style subcommands are used in Bloscpack.

Resources and Related Publications
----------------------------------

* `Main Blosc website <http://www.blosc.org>`_
* `Francesc Alted. *The Data Access Problem* EuroScipy 2009 Keynote Presentation <http://www.blosc.org/docs/StarvingCPUs.pdf>`_
* `Francesc Alted. *Why modern CPUs are starving and what can be done about it*, Computing in Science & Engineering, Vol. 12, No. 2. (March 2010), pp. 68-71 <http://www.blosc.org/docs/StarvingCPUs-CISE-2010.pdf>`_
* Francesc Alted: Sending Data from Memory to CPU (and back) faster than memcpy(). PyData London 2014 `slides0 <http://www.slideshare.net/PyData/blosc-py-data-2014>`_ `video0 <http://www.youtube.com/watch?v=IzqlWUTndTo>`_
* `The Blosc Github organization <https://github.com/Blosc>`_
* `Valentin Haenel. *Introducing Bloscpack* EuroScipy 2013 Presentation <https://github.com/esc/euroscipy2013-talk-bloscpack>`_
* `Valentin Haenel. *Bloscpack: a compressed lightweight serialization format for numerical data*. Proceedings of the 6th European Conference on Python in Science (EuroSciPy 2013) <http://arxiv.org/abs/1404.6383>`_.
* Valentin Haenel. *Fast Serialization of Numpy Arrays with Bloscpack*. PyData Berlin 2014 `slides1 <http://slides.zetatech.org/haenel-bloscpack-talk-2014-PyDataBerlin.pdf>`_, `video1 <https://www.youtube.com/watch?v=TZdqeEd7iTM>`_

Maintainers Notes on Cutting a Release
--------------------------------------

#. Set the version as environment variable ``VERSION=vX.X.X``
#. Update the changelog and ``ANNOUNCE.rst``
#. Commit using ``git commit -m "$VERSION changelog and ANNOUNCE.rst"``
#. Set the version number in ``bloscpack/version.py``
#. Commit with ``git commit -m "$VERSION"``
#. Make the tag using ``git tag -s -m "Bloscpack $VERSION" $VERSION``
#. Push commits to Blosc github ``git push blosc master``
#. Push commits to own github ``git push esc master``
#. Push the tag to Blosc github ``git push blosc $VERSION``
#. Push the tag to own github ``git push esc $VERSION``
#. Make a source distribution using ``python setup.py sdist bdist_wheel``
#. Upload to PyPi using ``twine upload dist/bloscpack-$VERSION*``
#. Bump version number to next dev version and reset ``ANNOUNCE.rst``
#. Announce release on the Blosc list
#. Announce release via Twitter

TODO
----

Documentation
~~~~~~~~~~~~~

* Refactor monolithic readme into Sphinx and publish
* Cleanup and double check the docstrings for the public API classes

Command Line
~~~~~~~~~~~~

* quiet verbosity level
* Expose the ability to set 'max_app_chunks' from the command line
* Allow to save metadata to a file during decompression
* subcommand e or estimate to estimate the size of the uncompressed data.
* subcommand v or verify to verify the integrity of the data
* add --raw-input and --raw-output switches to allow stuff like:
  cat file | blpk --raw-input --raw-output compress > file.blp
* Establish and document proper exit codes
* Document the metadata saved during Numpy serialization

Profiling and Optimization
~~~~~~~~~~~~~~~~~~~~~~~~~~

* Use the faster version of struct where you have a single string
* Memory profiler, might be able to reduce memory used by reusing the buffer
  during compression and decompression
* Benchmark different codecs
* Use line profiler to check code
* Select different defaults for Numpy arrays, no offsets? no pre-alloc?

Library Features
~~~~~~~~~~~~~~~~

* possibly provide a BloscPackFile abstraction, like GzipFile
* Allow to not-prealloc additional space for metadata
* Refactor certain collections of functions that operate on data into objects

  * Offsets (maybe)

* partial decompression?
* since we now have potentially small chunks, the progressbar becomes relevant
  again
* configuration file to store commonly used options on a given machine
* print the compression time, either as verbose or debug
* Investigate if we can use a StringIO object that returns memoryviews on read.
* Implement a memoryview Compressed/PlainSource
* Use a bytearray to read chunks from a file. Then re-use that bytearray
  during every read to avoid allocating deallocating strings the whole time.
* The keyword arguments to many functions are global dicts, this is a bad idea,
  Make the immutable with a forzendict.
* Check that the checksum is really being checked for all PlainSinks
* Bunch of NetworkSource/Sinks
* HTTPSource/Sink

Miscellaneous
~~~~~~~~~~~~~

* Announce on scipy/numpy lists, comp.compression, freshmeat, ohloh ...

Packaging and Infrastructure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Debian packages (for python-blosc and bloscpack)
* Conda recipes (for python-blosc and bloscpack)
* Use tox for testing multiple python versions
* Build on travis and drone.io using pre-compiled


Changelog
---------

* v0.16.0     - Thu 27 Dec 2018

  * Update of Python API and docs
  * various minor fixes

* v0.15.0     - Wed 31 Oct 2018

  * Halloween Release!
  * Adding the Blosc code of conduct (#79 by @esc)
  * Two new high-level functions: 'pack_bytes_to_bytes' and
    'unpack_bytes_from_bytes' (#83 by @esc)
  * Fix incorrect check for typesize-chunksize mismatch (#81 by @esc)
  * Fix test to append without shuffle (#82 by @esc)
  * Fix tests to respect snappy not being available by default (#85 by @esc)
  * Fix tests to account for new default blocksize (#86 by @esc)
  * Enable testing on Python 3.7 via Travis (#84 by @esc)

* v0.14.0     - Thu Oct 18 2018

  * Remove official support for Python 2.6 (#77 by @esc)

* v0.13.0     - Thu May 24 2018

  * Add license file and include in sdist packages (#75 by @toddrme2178)
  * Print codec on info (#73 by @esc)
  * Decode Blosc flags (#72 by @esc)
  * Fix an embarrassing typo (#71 by @esc)
  * Test zstd (#70 by @esc)
  * Document args object (#69 by @esc)
  * Various pep8 fixes by @esc
  * Support for uploading wheels and using twine by @esc
  * Fix use of coverage by @esc
  * Better support for Python 2.6 by @esc

* v0.12.0     - Fri Mar 09 2018

  * Allow Pythonic None as checksum (#60 by @esc)
  * Fix failing tests to comply with latest Blosc (#63 and #64 by FrancescElies)
  * Support testing with Python 3.6 via Travis (#65 by @esc)
  * Unpinn Blosc in conda recipe (who uses this?) (#61 by @esc)
  * Cleanup README (#66 by @esc)
  * Fix Trove classifiers (#67 by @esc)
  * Random pep8 fixes by @esc

* v0.11.0     - Mon Aug 22 2016

  * Unpinn python-blosc and fix unit-tests (#51 and #57 fixed by @oogali)
  * Improve the computation of the chunksize when it is not divisible by
    typesize (#52 by FrancescAlted)

* v0.10.0     - Thu Dec 10 2015

  * Fix for compressing sliced arrays (#43 reported by @mistycheney)
  * Fix ``un/pack_bytes_file`` to be available from toplevel
  * Fix the badges to come (mostly) from https://img.shields.io
  * Fixes for travis-ci, test Python 3.5 too
  * Pin Blosc version to 1.2.7 via `requirements.txt` and `setup.py` due to
    breakage with Blosc 1.2.8.

* v0.9.0     - Tue Aug 18 2015

  * Use ``ast.literal_eval`` instead of ``np.safe_eval`` which is much faster (#39 @cpcloud)
  * Support for packing/unpacking bytes to/from file (#41)

* v0.8.0     - Sun Jul 12 2015

  * Python 3.x compatibility (#14)

* v0.7.3     - Sat Jul 11 2015

  * Fix deserialization of numpy arrays with nested dtypes that were created
    with versions v0.7.1 and before. (#37)

* v0.7.2     - Wed Mar 25 2015

  * Fix support for zero length arrays (and input in general) (#17 reported by @dmbelov)
  * Catch when ``typesize`` doesn't divide ``chunk_size`` (#18 reported by @dmbelov)
  * Fix serialization of object arrays (#16 reported by @dmbelov)
  * Reject Object dtype arrays since they cannot be compressed with Bloscpack
  * Provide backwards compatibility for older Numpy serializations
  * Fix win32 compatibility of tests (#27 fixed by @mindw)
  * Fix using setuptools for scripts and dependencies (#28 fixed by @mindw)
  * Various misc fixes

* v0.7.1     - Sun Jun 29 2014

  * Fix a bug related to setting the correct typesize when compressing Numpy
    arrays
  * Optimization of debug statements in the inner loops

* v0.7.0     - Wed May 28 2014

  * Modularize cram tests, even has something akin to a harness
  * Refactored, tweaked and simplified Source/Sink code and semantics
  * Various documentation improvements: listing prior art, comparison to HDF5
  * Improve benchmarking scripts
  * Introduce a BloscArgs object for saner handling of the BloscArgs
  * Introduce a BloscpackArgs object for saner handling of the BloscpackArgs
  * Introduce MetadataHeader and MetdataArgs objects too
  * Fix all (hopefully) incorrect uses of the term 'compression ratio'
  * Various miscellaneous fixes and improvements

* v0.6.0     - Fri Mar 28 2014

  * Complete refactor of Bloscpack codebase to support modularization
  * Support for `drone.io <https://drone.io/>`_ CI service
  * Improved dependency specification for Python 2.6
  * Improved installation instructions

* v0.5.2     - Fri Mar 07 2014

  * Fix project url in setup.py

* v0.5.1     - Sat Feb 22 2014

  * Documentation fixes and improvements

* v0.5.0     - Sun Feb 02 2014

  * Moved project to the `Blosc organization on Github <https://github.com/Blosc>`_

* v0.5.0-rc1 - Thu Jan 30 2014

  * Support for Blosc 1.3.x (alternative codecs)

* v0.4.1     - Fri Sep 27 2013

  * Fixed the `pack_unpack_hard` test suite
  * Fixed handling Numpy record and nested record arrays

* v0.4.0     - Sun Sep 15 2013

  * Fix a bug when serializing numpy arrays to strings

* v0.4.0-rc2 - Tue Sep 03 2013

  * Package available via PyPi (since 0.4.0-rc1)
  * Support for packing/unpacking numpy arrays to/from string
  * Check that string and record arrays work
  * Fix installation problems with PyPi package (Thanks to Olivier Grisel)

* v0.4.0-rc1 - Sun Aug 18 2013

  * BloscpackHeader class introduced
  * The info subcommand shows human readable sizes when printing the header
  * Now using Travis-CI for testing and Coveralls for coverage
  * Further work on the Plain/Compressed-Source/Sink abstractions
  * Start using memoryview in places
  * Learned to serialize Numpy arrays

* v0.3.0     - Sun Aug 04 2013

  * Minor readme fixes
  * Increase number of cram tests

* v0.3.0-rc1 - Thu Aug 01 2013

  * Bloscpack format changes (format version 3)

    * Variable length metadata section with it's own header
    * Ability to preallocate offsets for appending data (``max_app_chunks``)

  * Refactor compression and decompression to use file pointers instead of
    file name strings, allows using StringIO/cStringIO.
  * Sanitize calculation of nchunks and chunk-size
  * Special keyword ``max`` for use with chunk-size in the CLI
  * Support appending to a file and ``append`` subcommand
    (including the ability to preallocate offsets)
  * Support rudimentary ``info`` subcommand
  * Add tests of the command line interface using ``cram``
  * Minor bugfixes and corrections as usual

* v0.2.1     - Mon Nov 26 2012

  * Backport to Python 2.6
  * Typo fixes in documentation

* v0.2.0     - Fri Sep 21 2012

  * Use ``atexit`` magic to remove test data on abort
  * Change prefix of temp directory to ``/tmp/blpk*``
  * Merge header RFC into monolithic readme

* v0.2.0-rc2 - Tue Sep 18 2012

  * Don't bail out if the file is smaller than default chunk
  * Set the default ``typesize`` to ``8`` bytes
  * Upgrade dependencies to python-blosc ``v1.0.5`` and fix tests
  * Make extreme test less resource intensive
  * Minor bugfixes and corrections

* v0.2.0-rc1 - Thu Sep 13 2012

  * Implement new header format as described in RFC
  * Implement checksumming compressed chunks with various checksums
  * Implement offsets of the chunks into the file
  * Efforts to make the library re-entrant, better control of side-effects
  * README is now rst not md (flirting with sphinx)
  * Tons of trivial fixes, typos, wording, refactoring, renaming, pep8 etc..

* v0.1.1     - Sun Jul 15 2012

  * Fix the memory issue with the tests
  * Two new suites: ``hard`` and ``extreme``
  * Minor typo fixes and corrections

* v0.1.0     - Thu Jun 14 2012

  * Freeze the first 8 bytes of the header (hopefully for ever)
  * Fail to decompress on non-matching format version
  * Minor typo fixes and corrections

* v0.1.0-rc3 - Tue Jun 12 2012

  * Limit the chunk-size benchmark to a narrower range
  * After more careful experiments, a default chunk-size of ``1MB`` was
    deemed most appropriate

  * Fixed a terrible bug, where during testing and benchmarking, temporary
    files were not removed, oups...

  * Adapted the header to have space for more chunks, include special marker
    for unknown chunk number (``-1``) and format version of the compressed
    file
  * Added a note in the README about instability of the file format
  * Various minor fixes and enhancements

* v0.1.0-rc2 - Sat Jun 09 2012

  * Default chunk-size now ``4MB``
  * Human readable chunk-size argument
  * Last chunk now contains remainder
  * Pure python benchmark to compare against gzip
  * Benchmark to measure the effect of chunk-size
  * Various minor fixes and enhancements

* v0.1.0-rc1 - Sun May 27 2012

  * Initial version
  * Compression/decompression
  * Command line argument parser
  * README, setup.py, tests and benchmark

Thanks
------

* Francesc Alted for writing Blosc in the first place, for providing continual
  code-review and feedback on Bloscpack and for co-authoring the Bloscpack
  file-format specification.

Author, Copyright and License
-----------------------------

© 2012-2020 Valentin Haenel <valentin@haenel.co>

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
