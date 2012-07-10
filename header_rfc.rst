RFC for the new Bloscpack Header
================================

:Author: Valentin Haenel
:Contact: valentin.haenel@gmx.de

The following 32 bit header is proposed for bloscpack as of version ``0.2.0``.
The design goals of the new header format are to contain as much information as
possible to achieve interesting things in the future and to be as general as
possible such that the new persistence layer of CArray is compatible with
bloscpack.

The following ascii representation shows the layout of the header::

    |-0-|-1-|-2-|-3-|-4-|-5-|-6-|-7-|-8-|-9-|-A-|-B-|-C-|-D-|-E-|-F-|
    | b   l   p   k | ^ | ^ | ^ | ^ |   chunk-size  |  last-chunk   |
                      |   |   |   |
          version ----+   |   |   |
          options --------+   |   |
         checksum ------------+   |
         typesize ----------------+

    |-0-|-1-|-2-|-3-|-4-|-5-|-6-|-7-|-8-|-9-|-A-|-B-|-C-|-D-|-E-|-F-|
    |            nchunks            |            RESERVED           |

The first 4 bytes are the magic string ``blpk``. Then there are 4 bytes, the
first three are described below and the last one is reserved. This is followed
by 4 bytes for the ``chunk-size``, another 4 bytes for the ``last-chunk-size``
and 8 bytes for the number of chunks. The last 8 bytes are reserved for use in
future versions of the format.

Description of the header entries
---------------------------------

All entries are little-endian.

:version:
    (``uint8``)
    format version of the bloscpack header, to ensure exceptions in case of
    forward incompatibilities.
:options:
    (``bitfield``)
    A bitfield which allows for setting certain options in this file.

    :``bit 0 (0x01)``:
        If the offsets to the chunks are present in this file.

:checksum:
    (``uint8``)
    The checksum used. The following checksums, available in the python
    standard library should be supported and the checksum should be
    placed after the chunk.

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
    couple of years. Again, ``-1`` denotes that the number of chunks was not
    known at the time of creation.

The overall file-size can be computed as ``chunk-size * (nchunks - 1) +
last-chunk-size``.

Description of the offsets entries
--------------------------------

Offsets of the chunks into the file are to be used for accelerated seeking. The
offsets (if activated) follow the header. Each offset is a 64 bit signed
little-endian integer (``int64``). A value of ``-1`` denotes an unknown offset.
Each offset denotes the exact position of the chunk in the file such that
seeking to the offset, will position the file pointer such that, reading the
next 16 bytes gives the blosc header, which is at the start of the desired
chunk. The layout of the file is then::

    |-bloscpack-header-|-offset-|-offset-|...|-chunk-|-chunk-|...|

