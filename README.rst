Bloscpack
=========

.. image:: https://badge.fury.io/py/bloscpack.png
    :target: https://crate.io/packages/bloscpack

.. image:: https://travis-ci.org/esc/bloscpack.png?branch=master
        :target: https://travis-ci.org/esc/bloscpack

.. image:: https://coveralls.io/repos/esc/bloscpack/badge.png
        :target: https://coveralls.io/r/esc/bloscpack

.. image:: https://pypip.in/d/bloscpack/badge.png
        :target: https://crate.io/packages/bloscpack


Description
-----------

Command line interface to and serialization format for `Blosc
<http://blosc.pytables.org/trac>`_, a high performance, multi-threaded,
blocking and shuffling compressor. Uses `python-blosc
<https://github.com/FrancescAlted/python-blosc>`_ bindings to interface with
blosc.

Website(s)
----------

* `Repository at Github <https://github.com/esc/bloscpack>`_

* `Crate.io <https://crate.io/packages/bloscpack/>`_
* `PyPi <https://pypi.python.org/pypi/bloscpack>`_
* `Ohloh <https://www.ohloh.net/p/bloscpack>`_
* `Masterbranch <https://masterbranch.com/bloscpack-project/1751103>`_


Contact
-------

There is an official Blosc mailing list at: http://groups.google.com/group/blosc

Dependencies
------------

* Python 2.6 (requires ``ordereddict`` and ``argparse``) or Python 2.7
* `python-blosc  <https://github.com/FrancescAlted/python-blosc>`_
  `(at least v1.0.5) <https://github.com/FrancescAlted/python-blosc/tree/v1.0.5>`_ (provides Blosc)
* The Python packages ``numpy``, ``nose`` and ``cram`` for testing

Stability of File Format
------------------------

The tool is considered alpha-stage, experimental, research software. It is not
unlikely that **the internal storage format for the compressed files will
change in future**. Please **do not depend critically on the files generated
(unless you know what you are doing)** by Bloscpack. See the warranty disclaimer
in the licence at the end of this file.

Installation
------------

The package is available on PyPi, so you may use pip to install it:

.. code-block:: console

    $ pip install bloscpack

If you want to install straight from GitHub, use pip's VCS support:

.. code-block:: console

    $ pip install git+https://github.com/esc/bloscpack

Or, of course, download the source code or clone the repository and then use
the standard ``setup.py``:

.. code-block:: console

    $ python setup.py install

All of the above may or may not -- depending on the incantation used -- require
superuser privileges.

Alternatively, if you just need the command line interface, add the ``blpk``
file to your ``$PATH`` somehow. For example by copying using dereferencing
(``-L``), since ``blpk`` is a sym-link to ``bloscpack.py``:

.. code-block:: console

    $ cp -L blpk ~/bin

Usage
-----

Bloscpack has a number of global options and four subcommands: ``[c |
compress]``, ``[d | decompress]``, ``[a | append]`` and ``[i | info]`` most of
which each have their own options.

Help for global options and subcommands:

.. code-block:: console

    $ ./blpk --help
    [...]

Help for each one of the subcommands:

.. code-block:: console

    $ ./blpk compress --help
    [...]
    $ ./blpk decompress --help
    [...]
    $ ./blpk info --help
    [...]
    $ ./blpk append --help
    [...]

Examples
--------

Basics
~~~~~~

Basic compression:

.. code-block:: console

    $ ./blpk c data.dat

... will compress the file ``data.dat`` to ``data.dat.blp``

Basic decompression:

.. code-block:: console

    $ ./blpk d data.dat.blp data.dcmp

... will decompress the file ``data.dat.blp`` to the file ``data.dcmp``. If you
leave out the ``[<out_file>]`` argument, Bloscpack will complain that the file
``data.dat`` exists already and refuse to overwrite it:

.. code-block:: console

    $ ./blpk d data.dat.blp
    blpk: error: output file 'data.dat' exists!

If you know what you are doing, you can use the global option ``[-f |
--force]`` to override the overwrite checks:

.. code-block:: console

    $ ./blpk -f d data.dat.blp

Incidentally this works for compression too:

.. code-block:: console

    $ ./blpk c data.dat
    blpk: error: output file 'data.dat.blp' exists!
    $ ./blpk -f c data.dat

Settings
~~~~~~~~

By default, the number of threads that Blosc uses is determined by the number
of cores detected on your system. You can change this using the ``[-n |
--nthreads]`` option:

.. code-block:: console

    $ ./blpk -n 1 c data.dat

There are some useful additional options for compression, that are passed
directly to Blosc:

* ``[-t | --typesize]``
  Typesize used by Blosc (default: 8):
  ``$ ./blpk c -t 8 data.dat``
* ``[-l | --level]``
  Compression level (default: 7):
  ``$ ./blpk c -l 3 data.dat``
* ``[-s | --no-shuffle]``
  Deactivate shuffle:
  ``$ ./blpk c -s data.dat``

In addition, the desired size of the chunks may be specified.

* ``[-z | --chunk-size]``
  Desired approximate size of the chunks, where you can use human readable
  strings like ``8M`` or ``128K`` or ``max`` to use the maximum chunk size of
  apprx. ``2GB`` (default: ``1MB``):
  ``$ ./blpk -d c -z 128K data.dat``
  ``$ ./blpk -d c -z max data.dat``

There are two options that influence how the data is stored:

* ``[-k | --checksum <checksum>]``
  Chose which checksum to use. The following values are permissible:
  ``None``, ``adler32``, ``crc32``, ``md5``,
  ``sha1``, ``sha224``, ``sha256``, ``sha384``,
  ``sha512``, (default: ``adler32``). As described in the header format, each
  compressed chunk can be stored with a checksum, which aids corruption
  detection on decompression.

* ``[-o | --no-offsets]``
  By default, offsets to the individual chunks are stored. These are included
  to allow for partial decompression in the future. This option disables that
  feature. Also, a certain number of offsets (default: 10 * 'nchunks') are
  preallocated to allow for appending data to the file.

Info Subcommand
~~~~~~~~~~~~~~~

If you just need some info on how the file was compressed ``[i | info]``:

.. code-block:: console

   $ ./blpk info testfile.blp
   blpk: 'bloscpack_header':
   blpk: {   'checksum': 'adler32',
   blpk:     'chunk_size': 1048576,
   blpk:     'format_version': 3,
   blpk:     'last_chunk': 921600,
   blpk:     'max_app_chunks': 15260,
   blpk:     'metadata': False,
   blpk:     'nchunks': 1526,
   blpk:     'offsets': True,
   blpk:     'typesize': 8}
   blpk: 'offsets':
   blpk: [134320,354002,552182,709597,870494,...]

Adding Metdata
~~~~~~~~~~~~~~

Using the ``[-m | --metadata]`` option you can include JSON from a file:

.. code-block:: console

   $ cat meta.json
   {"dtype": "float64", "shape": [200000000], "container": "numpy"}
   $ ./blpk compress --metadata meta.json data.dat
   $ ./blpk info data.dat.blp
   blpk: 'bloscpack_header':
   blpk: {   'checksum': 'adler32',
   blpk:     'chunk_size': 1048576,
   blpk:     'format_version': 3,
   blpk:     'last_chunk': 921600,
   blpk:     'max_app_chunks': 15260,
   blpk:     'metadata': True,
   blpk:     'nchunks': 1526,
   blpk:     'offsets': True,
   blpk:     'typesize': 8}
   blpk: 'metadata':
   blpk: {   u'container': u'numpy', u'dtype': u'float64', u'shape': [200000000]}
   blpk: 'metadata_header':
   blpk: {   'magic_format': 'JSON',
   blpk:     'max_meta_size': 590,
   blpk:     'meta_checksum': 'adler32',
   blpk:     'meta_codec': 'zlib',
   blpk:     'meta_comp_size': 58,
   blpk:     'meta_level': 6,
   blpk:     'meta_options': '00000000',
   blpk:     'meta_size': 59,
   blpk:     'user_codec': ''}
   blpk: 'offsets':
   blpk: [134946,354628,552808,710223,871120,...]

It will be printed when decompressing:

.. code-block:: console

    $ ./blpk d data.dat.blp
    blpk: Metadata is:
    blpk: '{u'dtype': u'float64', u'shape': [200000000], u'container': u'numpy'}'

Appending
~~~~~~~~~

You can also append data to an existing bloscpack compressed file:

.. code-block:: console

   $ ./blpk append data.dat.blp data.dat

However there are certain limitations on the amount of data can be appended.
For example, if there is an offsets section, there must be enough room to store
the offsets for the appended chunks. If no offsets exists, you may append as
much data as possible given the limitations governed by the maximum number of
chunks and the chunk-size. Additionally, there are limitations on the
compression options. For example, one cannot change the checksum used. It is
however possible to change the compression level, the typesize and the shuffle
option for the appended chunks.

Verbose and Debug mode
~~~~~~~~~~~~~~~~~~~~~~

Lastly there are two options to control how much output is produced,

The first causes basic info to be printed, ``[-v | --verbose]``:

.. code-block:: console

    $ ./blpk --verbose compress --chunk-size 0.5G data.dat
    blpk: getting ready for compression
    blpk: input file is: data.dat
    blpk: output file is: data.dat.blp
    blpk: using 8 threads
    blpk: input file size: 1.49G (1600000000B)
    blpk: nchunks: 3
    blpk: chunk_size: 512.0M (536870912B)
    blpk: output file size: 161.9M (169759818B)
    blpk: compression ratio: 0.106100
    blpk: done

... and ``[-d | --debug]`` prints a detailed account of what is going on:

.. code-block:: console

    $ ./blpk --debug compress --chunk-size 0.5G data.dat
    blpk: command line argument parsing complete
    blpk: command line arguments are:
    blpk:   nchunks: None
    blpk:   force: False
    blpk:   verbose: False
    blpk:   offsets: True
    blpk:   checksum: adler32
    blpk:   subcommand: compress
    blpk:   out_file: None
    blpk:   in_file: data.dat
    blpk:   chunk_size: 512.0M (536870912B)
    blpk:   debug: True
    blpk:   shuffle: True
    blpk:   typesize: 8
    blpk:   clevel: 7
    blpk:   nthreads: 8
    blpk: getting ready for compression
    blpk: blosc args are:
    blpk:   typesize: 8
    blpk:   shuffle: True
    blpk:   clevel: 7
    blpk: input file is: data.dat
    blpk: output file is: data.dat.blp
    blpk: using 8 threads
    blpk: input file size: 1.49G (1600000000B)
    blpk: 'chunk_size' proposed
    blpk: nchunks: 3
    blpk: chunk_size: 512.0M (536870912B)
    blpk: last_chunk_size: 501.88M (526258176B)
    blpk: raw_bloscpack_header: 'blpk\x02\x01\x01\x08\x00\x00\x00 \x00\x10^\x1f\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    blpk: chunk '0' written, in: 512.0M (536870912B) out: 55.69M (58399001B)
    blpk: checksum (adler32): '\xf7\xaa\xa3\xdf' offset: '56'
    blpk: chunk '1' written, in: 512.0M (536870912B) out: 53.85M (56463343B)
    blpk: checksum (adler32): '\xafo\xfe\xfd' offset: '58399061'
    blpk: chunk '2' (last) written, in: 501.88M (526258176B) out: 52.35M (54897406B)
    blpk: checksum (adler32): '\x91v\x07\\' offset: '114862408'
    blpk: Writing '3' offsets: '[56, 58399061, 114862408]'
    blpk: Raw offsets: '8\x00\x00\x00\x00\x00\x00\x00U\x19{\x03\x00\x00\x00\x00H\xa9\xd8\x06\x00\x00\x00\x00'
    blpk: output file size: 161.9M (169759818B)
    blpk: compression ratio: 0.106100
    blpk: done

Python API
----------

The Python API is still in flux, so this section is deliberately sparse.

Numpy
~~~~~

Numpy arrays can be serialized as bloscpack files, here is a very brief example:

.. code-block:: pycon

    >>> a = np.linspace(0, 1, 3e8)
    >>> print a.size, a.dtype
    300000000 float64
    >>> bp.pack_ndarray_file(a, 'a.blp')
    >>> b = bp.unpack_ndarray_file('a.blp')
    >>> (a == b).all()
    True

Looking at the generated file, we can see the Numpy metadata being saved:

.. code-block:: console

    $ lh a.blp
    -rw------- 1 esc esc 266M Aug 13 23:21 a.blp
    anaconda ~ esc@toolbox 
    $ blpk info a.blp
    blpk: bloscpack header: 
    blpk:     format_version=3,
    blpk:     offsets=True,
    blpk:     metadata=True,
    blpk:     checksum='adler32',
    blpk:     typesize=8,
    blpk:     chunk_size=1.0M (1048576B),
    blpk:     last_chunk=838.0K (858112B),
    blpk:     nchunks=2289,
    blpk:     max_app_chunks=22890
    blpk: 'metadata':
    blpk: {   u'container': u'numpy',
    blpk:     u'dtype': [[u'', u'<f8']],
    blpk:     u'order': u'C',
    blpk:     u'shape': [300000000]}
    blpk: 'metadata_header':
    blpk: {   'magic_format': 'JSON',
    blpk:     'max_meta_size': 740,
    blpk:     'meta_checksum': 'adler32',
    blpk:     'meta_codec': 'zlib',
    blpk:     'meta_comp_size': 68,
    blpk:     'meta_level': 6,
    blpk:     'meta_options': '00000000',
    blpk:     'meta_size': 74,
    blpk:     'user_codec': ''}
    blpk: 'offsets':
    blpk: [202240,408134,554982,690522,819749,...]

Alternatively, we can also use a string as storage:

.. code-block::

    >>> a = np.linspace(0, 1, 3e8)
    >>> c = pack_ndarray_str(a)
    >>> b = unpack_ndarray_str(c)
    >>> (a == b).all()
    True


Testing
-------

Basic Tests
~~~~~~~~~~~

Basic tests, runs quickly:

.. code-block:: console

    $ nosetests
    [...]

Heavier Tests
~~~~~~~~~~~~~

Extended tests using a larger file, may take some time, but will be nice to
memory:

.. code-block:: console

    $ nosetests test_bloscpack.py:pack_unpack_hard
    [...]

Extended tests using a huge file. This one take forever and needs loads (5G-6G)
of memory and loads of disk-space (10G). Use ``-s`` to print progress:

.. code-block:: console

    $ nosetests -s test_bloscpack.py:pack_unpack_extreme
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

   $ ./test_bloscpack.cram
   [...]

Coverage
~~~~~~~~

To determine coverage you can pool togeher the coverage from the cram tests and
the unit tests:

.. code-block:: console

    $ COVERAGE=1 ./test_bloscpack.cram
    [...]
    $nosetests test_bloscpack.py --with-coverage --cover-package=bloscpack
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
    Time: 1.72 seconds
    Output file size: 198.55M
    Ratio: 0.13
    Will now run gzip...
    Time: 131.63 seconds
    Output file size: 924.05M
    Ratio: 0.61

As was expected from previous benchmarks of Blosc using the python-blosc
bindings, Blosc is both much faster and has a better compression ratio for this
kind of structured data. One thing to note here, is that we are not dropping
the system file cache after every step, so the file to read will be cached in
memory. To get a more accurate picture we can use the ``--drop-caches`` switch
of the benchmark which requires you however, to run the benchmark as root,
since dropping the caches requires root privileges:

.. code-block:: console

    $ PYTHONPATH=. bench/blpk_vs_gzip.py --drop-caches
    create the test data..........done

    Input file size: 1.49G
    Will now run bloscpack...
    Time: 4.30 seconds
    Output file size: 198.55M
    Ratio: 0.13
    Will now run gzip...
    Time: 135.15 seconds
    Output file size: 924.05M
    Ratio: 0.61

While the absolute improvement for `gzip` when using the file system cache is
higher, when looking at the relative improvement `bloscpack` runs twice as fast
when the input file comes from the file cache.

Bloscpack Format
----------------

The input is split into chunks since a) we wish to put less stress on main
memory and b) because Blosc has a buffer limit of ``2GB`` (Version ``1.0.0`` and
above). By default the chunk-size is a moderate ``1MB`` which should be fine,
even for less powerful machines.

In addition to the chunks some additional information must be added to the file
for housekeeping:

:header:
    a 32 bit header containing various pieces of information
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
The following 32 bit header is used for Bloscpack as of version ``0.3.0``.  The
design goals of the header format are to contain as much information as
possible to achieve interesting things in the future and to be as general as
possible such that the persistence layer of tools such as `CArray
<https://github.com/FrancescAlted/carray>`_ and `Blaze
<https://github.com/ContinuumIO/blaze>`_ can be implemented without modifcation
of the header format.

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

TODO
----

* list prior art
* quiet verbosity level
* possibly provide a BloscPackFile abstraction, like GzipFile
* document library usage
* Expose the ability to set 'max_app_chunks' from the command line
* Allow to save metadata to a file during decompression
* Refactor certain collections of functions that operate on data into objects

  * BloscHeader
  * MetadataHeader
  * Offsets (maybe)

* subcommand e or estimate to estimate the size of the uncompressed data.
* subcommand v or verify to verify the integrity of the data
* partial decompression?
* add --raw-input and --raw-output switches to allow stuff like:
  cat file | blpk --raw-input --raw-output compress > file.blp
* since we now have potentially small chunks, the progressbar becomes relevant
  again
* configuration file to store commonly used options on a given machine
* check Python 3.x compatibility
* make a note in the README that the chunk-size benchmark can be used to tune
* print the compression time, either as verbose or debug
* Announcement RST
* Announce on scipy/numpy lists, comp.compression, freshmeat, ohloh ...
* Debian packages (for python-blosc and bloscpack)
* Establish and document proper exit codes
* Use tox for testing multiple python versions
* Investigate if we can use a StringIO object that returns memoryviews on read.
* Implement a memoryview Compressed/PlainSource
* Use a bytearray to read chunks from a file. Then re-use that bytearray
  during every read to avoid allocating deallocating strings the whole time.
* Document the metadata saved during Numpy serialization
* The keyword arguments to many functions are global dicts, this is a bad idea,
  Make the immutable with a forzendict.
* Check that source and sink are of the correct type


Changelog
---------

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

* Fracesc Alted for writing Blosc in the first place, for providing continual
  code-review and feedback on Bloscpack and for co-authoring the Bloscpack
  file-format specification.

Author, Copyright and License
-----------------------------

© 2012-2013 Valentin Haenel <valentin.haenel@gmx.de>

Bloscpack is licensed under the terms of the MIT License.

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
