#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import collections
import copy
import pprint


import blosc


from .constants import (MAGIC,
                        FORMAT_VERSION,
                        MAX_FORMAT_VERSION,
                        MAX_CHUNKS,
                        BLOSCPACK_HEADER_LENGTH,
                        )

from bloscpack import (
               check_valid_checksum,
               double_pretty_size,
               check_range,
               encode_uint8,
               encode_int32,
               encode_int64,
               decode_uint8,
               decode_int32,
               decode_int64,
               decode_bitfield,
               decode_options,
               create_options,
               print_debug,
               )

from . import checksums


class BloscPackHeader(collections.MutableMapping):
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
            raise ValueError("'max_app_chunks' can not be non '0' if 'nchunks' is '-1'")
        if chunk_size != -1 and last_chunk != -1 and last_chunk > chunk_size:
            raise ValueError("'last_chunk' (%d) is larger than 'chunk_size' (%d)"
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
        self._len = len(self._attrs)
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

    def __getitem__(self, key):
        if key not in self._attrs:
            raise KeyError('%s not in BloscPackHeader' % key)
        return getattr(self, key)

    def __setitem__(self, key, value):
        if key not in self._attrs:
            raise KeyError('%s not in BloscPackHeader' % key)
        setattr(self, key, value)

    def __delitem__(self, key):
        raise NotImplementedError(
            'BloscPackHeader does not support __delitem__ or derivatives')

    def __len__(self):
        return self._len

    def __iter__(self):
        return iter(self._attrs)

    def __str__(self):
        return pprint.pformat(dict(self))

    def __repr__(self):
        return "BloscPackHeader(%s)" % ", ".join((("%s=%s" % (arg, repr(value)))
                          for arg, value in self.iteritems()))

    def pformat(self, indent=4):
        indent = " " * indent
        # don't ask, was feeling functional
        return "bloscpack header: \n%s%s" % (indent, (",\n%s" % indent).join((("%s=%s" % 
            (key, (repr(value) if (key not in self._bytes_attrs or value == -1)
                         else double_pretty_size(value)))
             for key, value in self.iteritems()))))

    def copy(self):
        return copy.copy(self)

    @property
    def checksum_impl(self):
        return checksums.CHECKSUMS_LOOKUP[self.checksum]

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
        checksum = encode_uint8(checksums.CHECKSUMS_AVAIL.index(self.checksum))
        typesize = encode_uint8(self.typesize)
        chunk_size = encode_int32(self.chunk_size)
        last_chunk = encode_int32(self.last_chunk)
        nchunks = encode_int64(self.nchunks)
        max_app_chunks = encode_int64(self.max_app_chunks)

        raw_bloscpack_header = (MAGIC + format_version + options + checksum +
                                typesize + chunk_size + last_chunk + nchunks +
                                max_app_chunks)
        print_debug('raw_bloscpack_header: %s' % repr(raw_bloscpack_header))
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
            try:
                rep = buffer_[0:4].tobytes()
            except AttributeError:
                rep = buffer_[0:4]
            raise ValueError(
                "the magic marker '%s' is missing from the bloscpack " % MAGIC +
                "header, instead we found: %s" % repr(rep))
        options = decode_options(decode_bitfield(buffer_[5]))
        return BloscPackHeader(
            format_version=decode_uint8(buffer_[4]),
            offsets=options['offsets'],
            metadata=options['metadata'],
            checksum=checksums.CHECKSUMS_AVAIL[decode_uint8(buffer_[6])],
            typesize=decode_uint8(buffer_[7]),
            chunk_size=decode_int32(buffer_[8:12]),
            last_chunk=decode_int32(buffer_[12:16]),
            nchunks=decode_int64(buffer_[16:24]),
            max_app_chunks=decode_int64(buffer_[24:32]))


