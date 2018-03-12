#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import struct

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict


import blosc
from six import PY3, integer_types, binary_type

from .abstract_objects import (MutableMappingObject,
                               )
from .checksums import (CHECKSUMS_AVAIL,
                        CHECKSUMS_LOOKUP,
                        check_valid_checksum,
                        )
from .constants import (MAGIC,
                        FORMAT_VERSION,
                        MAX_FORMAT_VERSION,
                        MAX_CHUNKS,
                        MAX_CLEVEL,
                        BLOSCPACK_HEADER_LENGTH,
                        MAX_META_SIZE,
                        CNAME_MAPPING,
                        )
from .defaults import (DEFAULT_OFFSETS,
                       )
from .metacodecs import (CODECS_AVAIL,
                         check_valid_codec,
                         )
from .util import (memoryview,
                   )
from . import log


def check_range(name, value, min_, max_):
    """ Check that a variable is in range. """
    if not isinstance(value, integer_types):
        raise TypeError("'%s' must be of type 'int'" % name)
    elif not min_ <= value <= max_:
        raise ValueError(
            "'%s' must be in the range %s <= n <= %s, not '%s'" %
            tuple(map(str, (name, min_, max_, value))))


def _check_str(name, value, max_len):
    if not isinstance(value, binary_type):
        raise TypeError("'%s' must be of type 'str'/'bytes'" % name)
    elif len(value) > max_len:
        raise ValueError("'%s' can be of max length '%i' but is: '%s'" %
                         (name, max_len, len(value)))


def _pad_with_nulls(data, len_):
    """ Pad string with null bytes.

    Parameters
    ----------
    data : str/bytes
        the string/bytes to pad
    len_ : int
        the final desired length
    """
    return data + (b'\x00' * (len_ - len(data)))


def check_options(options):
    """ Check the options bitfield.

    Parameters
    ----------
    options : str

    Raises
    ------
    TypeError
        if options is not a string
    ValueError
        either if any character in option is not a zero or a one, or if options
        is not of length 8
    """

    if not isinstance(options, str):
        raise TypeError("'options' must be of type 'str', not '%s'" %
                        type(options))
    elif (not len(options) == 8 or
            not all(map(lambda x: x in ['0', '1'], iter(options)))):
        raise ValueError(
            "'options' must be string of 0s and 1s of length 8, not '%s'" %
            options)


def check_options_zero(options, indices):
    for i in indices:
        if options[i] != '0':
            raise ValueError(
                'Element %i was non-zero when attempting to decode options')


def decode_uint8(byte):
    if PY3:
        return byte
    else:
        return struct.unpack('<B', byte)[0]


def decode_uint32(fourbyte):
    return struct.unpack('<I', fourbyte)[0]


def decode_int32(fourbyte):
    return struct.unpack('<i', fourbyte)[0]


def decode_int64(eightbyte):
    return struct.unpack('<q', eightbyte)[0]


def decode_bitfield(byte):
    return bin(decode_uint8(byte))[2:].rjust(8, '0')


def decode_magic_string(str_):
    if PY3:
        return str_.strip(b'\x00')
    else:
        return str_.strip('\x00')


def encode_uint8(byte):
    return struct.pack('<B', byte)


def encode_uint32(byte):
    return struct.pack('<I', byte)


def encode_int32(fourbyte):
    return struct.pack('<i', fourbyte)


def encode_int64(eightbyte):
    return struct.pack('<q', eightbyte)


def create_options(offsets=DEFAULT_OFFSETS, metadata=False):
    """ Create the options bitfield.

    Parameters
    ----------
    offsets : bool
    metadata : bool
    """
    return "".join([str(int(i)) for i in
        [False, False, False, False, False, False, metadata, offsets]])


def decode_options(options):
    """ Parse the options bitfield.

    Parameters
    ----------
    options : str
        the options bitfield

    Returns
    -------
    options : dict mapping str -> bool
    """

    check_options(options)
    check_options_zero(options, range(6))
    return {'offsets': bool(int(options[7])),
            'metadata': bool(int(options[6])),
            }


def create_metadata_options():
    """ Create the metadata options bitfield. """
    return "00000000"


def decode_metadata_options(options):
    check_options(options)
    check_options_zero(options, range(8))
    return {}


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
    Please see the readme for a precise descripttion of the blosc header
    format.

    """
    buffer_ = memoryview(buffer_)
    return OrderedDict((('version', decode_uint8(buffer_[0])),
                        ('versionlz', decode_uint8(buffer_[1])),
                        ('flags', decode_uint8(buffer_[2])),
                        ('typesize', decode_uint8(buffer_[3])),
                        ('nbytes', decode_uint32(buffer_[4:8])),
                        ('blocksize', decode_uint32(buffer_[8:12])),
                        ('ctbytes', decode_uint32(buffer_[12:16]))))


def decode_blosc_flags(byte_):
    return OrderedDict((('byte_shuffle',  bool(byte_ & 1)),
                        ('pure_memcpy',   bool(byte_ >> 1 & 1)),
                        ('bit_shuffle',   bool(byte_ >> 2 & 1)),
                        ('split_blocks',  bool(byte_ >> 4 & 1)),
                        ('codec',         CNAME_MAPPING[byte_ >> 5 & 7]),
                        ))


class BloscpackHeader(MutableMappingObject):
    """ The Bloscpack header.

    Parameters
    ----------
    format_version : int
        the version format for the compressed file
    offsets: bool
        if the offsets to the chunks are present
    metadata: bool
        if the metadata is present
    checksum : str
        the checksum to be used
    typesize : int
        the typesize used for blosc in the chunks
    chunk_size : int
        the size of a regular chunk
    last_chunk : int
        the size of the last chunk
    nchunks : int
        the number of chunks
    max_app_chunks : int
        the total number of possible append chunks

    Notes
    -----
    See the README distributed for details on the header format.

    Raises
    ------
    ValueError
        if any of the arguments have an invalid value
    TypeError
        if any of the arguments have the wrong type
    """
    def __init__(self,
                 format_version=FORMAT_VERSION,
                 offsets=False,
                 metadata=False,
                 checksum='None',
                 typesize=0,
                 chunk_size=-1,
                 last_chunk=-1,
                 nchunks=-1,
                 max_app_chunks=0):

        check_range('format_version', format_version, 0, MAX_FORMAT_VERSION)
        check_valid_checksum(checksum)
        check_range('typesize',   typesize,    0, blosc.BLOSC_MAX_TYPESIZE)
        check_range('chunk_size', chunk_size, -1, blosc.BLOSC_MAX_BUFFERSIZE)
        check_range('last_chunk', last_chunk, -1, blosc.BLOSC_MAX_BUFFERSIZE)
        check_range('nchunks',    nchunks,    -1, MAX_CHUNKS)
        check_range('max_app_chunks', max_app_chunks, 0, MAX_CHUNKS)
        if nchunks != -1:
            check_range('nchunks + max_app_chunks',
                        nchunks + max_app_chunks, 0, MAX_CHUNKS)
        elif max_app_chunks != 0:
            raise ValueError(
                "'max_app_chunks' can not be non '0' if 'nchunks' is '-1'")
        if chunk_size != -1 and last_chunk != -1 and last_chunk > chunk_size:
            raise ValueError(
                "'last_chunk' (%d) is larger than 'chunk_size' (%d)"
                % (last_chunk, chunk_size))

        self._attrs = ['format_version',
                       'offsets',
                       'metadata',
                       'checksum',
                       'typesize',
                       'chunk_size',
                       'last_chunk',
                       'nchunks',
                       'max_app_chunks']
        self._bytes_attrs = ['chunk_size',
                             'last_chunk']

        self.format_version  = format_version
        self.offsets         = offsets
        self.metadata        = metadata
        self.checksum        = checksum
        self.typesize        = typesize
        self.chunk_size      = chunk_size
        self.last_chunk      = last_chunk
        self.nchunks         = nchunks
        self.max_app_chunks  = max_app_chunks

    @property
    def attributes(self):
        return self._attrs

    @property
    def bytes_attributes(self):
        return self._bytes_attrs

    @property
    def checksum_impl(self):
        return CHECKSUMS_LOOKUP[self.checksum]

    @property
    def total_prospective_chunks(self):
        return self.nchunks + self.max_app_chunks \
            if self.nchunks >= 0 else None

    def encode(self):
        """ Encode the Bloscpack header.

        Returns
        -------

        raw_bloscpack_header : string
            the header as string of bytes
        """
        format_version = encode_uint8(self.format_version)
        options = encode_uint8(int(
            create_options(offsets=self.offsets, metadata=self.metadata),
            2))
        checksum = encode_uint8(CHECKSUMS_AVAIL.index(self.checksum))
        typesize = encode_uint8(self.typesize)
        chunk_size = encode_int32(self.chunk_size)
        last_chunk = encode_int32(self.last_chunk)
        nchunks = encode_int64(self.nchunks)
        max_app_chunks = encode_int64(self.max_app_chunks)

        raw_bloscpack_header = (MAGIC + format_version + options + checksum +
                                typesize + chunk_size + last_chunk + nchunks +
                                max_app_chunks)
        log.debug('raw_bloscpack_header: %s' % repr(raw_bloscpack_header))
        return raw_bloscpack_header

    @staticmethod
    def decode(buffer_):
        """ Decode an encoded Bloscpack header.

        Parameters
        ----------
        buffer_ : str of length BLOSCPACK_HEADER_LENGTH

        Returns
        -------
        bloscpack_header : BloscPackHeader
            the decoded Bloscpack header object

        Raises
        ------
        ValueError
            If the buffer_ is not equal to BLOSCPACK_HEADER_LENGTH or the the
            first four bytes are not the Bloscpack magic.

        """
        if len(buffer_) != BLOSCPACK_HEADER_LENGTH:
            raise ValueError(
                "attempting to decode a bloscpack header of length '%d', not '%d'"
                % (len(buffer_), BLOSCPACK_HEADER_LENGTH))
        elif buffer_[0:4] != MAGIC:
            raise ValueError(
                "the magic marker %r is missing from the bloscpack " % MAGIC +
                "header, instead we found: %r" % buffer_[0:4])
        options = decode_options(decode_bitfield(buffer_[5]))
        return BloscpackHeader(
            format_version=decode_uint8(buffer_[4]),
            offsets=options['offsets'],
            metadata=options['metadata'],
            checksum=CHECKSUMS_AVAIL[decode_uint8(buffer_[6])],
            typesize=decode_uint8(buffer_[7]),
            chunk_size=decode_int32(buffer_[8:12]),
            last_chunk=decode_int32(buffer_[12:16]),
            nchunks=decode_int64(buffer_[16:24]),
            max_app_chunks=decode_int64(buffer_[24:32]))


class MetadataHeader(MutableMappingObject):

    def __init__(self,
                 magic_format=b'',
                 meta_options="00000000",
                 meta_checksum='None',
                 meta_codec='None',
                 meta_level=0,
                 meta_size=0,
                 max_meta_size=0,
                 meta_comp_size=0,
                 user_codec=b'',
                 ):
        _check_str('magic-format',     magic_format,  8)
        check_options(meta_options)
        check_valid_checksum(meta_checksum)
        check_valid_codec(meta_codec)
        check_range('meta_level',      meta_level,     0, MAX_CLEVEL)
        check_range('meta_size',       meta_size,      0, MAX_META_SIZE)
        check_range('max_meta_size',   max_meta_size,  0, MAX_META_SIZE)
        check_range('meta_comp_size',  meta_comp_size, 0, MAX_META_SIZE)
        _check_str('user_codec',       user_codec,     8)

        self.magic_format = magic_format
        self.meta_options = meta_options
        self.meta_checksum = meta_checksum
        self.meta_codec = meta_codec
        self.meta_level = meta_level
        self.meta_size = meta_size
        self.max_meta_size = max_meta_size
        self.meta_comp_size = meta_comp_size
        self.user_codec = user_codec

        self._attrs = ['magic_format',
                       'meta_options',
                       'meta_checksum',
                       'meta_codec',
                       'meta_level',
                       'meta_size',
                       'max_meta_size',
                       'meta_comp_size',
                       'user_codec',
                       ]

        self._bytes_attrs = ['meta_size',
                             'max_meta_size',
                             'meta_comp_size',
                             ]

    @property
    def attributes(self):
        return self._attrs

    @property
    def bytes_attributes(self):
        return self._bytes_attrs

    def encode(self):

        magic_format = _pad_with_nulls(self.magic_format, 8)
        meta_options = encode_uint8(int(self.meta_options, 2))
        meta_checksum = encode_uint8(CHECKSUMS_AVAIL.index(self.meta_checksum))
        meta_codec = encode_uint8(CODECS_AVAIL.index(self.meta_codec))
        meta_level = encode_uint8(self.meta_level)
        meta_size = encode_uint32(self.meta_size)
        max_meta_size = encode_uint32(self.max_meta_size)
        meta_comp_size = encode_uint32(self.meta_comp_size)
        user_codec = _pad_with_nulls(self.user_codec, 8)

        return magic_format + meta_options + meta_checksum + meta_codec + \
                meta_level + meta_size + max_meta_size + meta_comp_size + \
                user_codec

    @staticmethod
    def decode(buffer_):
        if len(buffer_) != 32:
            raise ValueError(
                "attempting to decode a bloscpack metadata header of length '%d', not '32'"
                % len(buffer_))
        decoded= {'magic_format':        decode_magic_string(buffer_[:8]),
                  'meta_options':        decode_bitfield(buffer_[8]),
                  'meta_checksum':       CHECKSUMS_AVAIL[decode_uint8(buffer_[9])],
                  'meta_codec':          CODECS_AVAIL[decode_uint8(buffer_[10])],
                  'meta_level':          decode_uint8(buffer_[11]),
                  'meta_size':           decode_uint32(buffer_[12:16]),
                  'max_meta_size':       decode_uint32(buffer_[16:20]),
                  'meta_comp_size':      decode_uint32(buffer_[20:24]),
                  'user_codec':          decode_magic_string(buffer_[24:32])
                  }
        return MetadataHeader(**decoded)
