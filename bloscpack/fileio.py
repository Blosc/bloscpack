#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


from .args import (_check_metadata_arguments,
                   METADATA_ARGS,
                   )
from .codecs import (CODECS_AVAIL,
                     CODECS_LOOKUP,
                     )
from .contents import (METADATA_HEADER_LENGTH,
                       BLOSCPACK_HEADER_LENGTH,
                       BLOSC_HEADER_LENGTH,
                       FORMAT_VERSION,
                       )
from .checksums import (CHECKSUMS_AVAIL,
                        CHECKSUMS_LOOKUP,
                        )
from .defaults import (DEFAULT_META_CODEC,
                       DEFAULT_META_LEVEL,
                       )
from .exceptions import (MetadataSectionTooSmall,
                         FormatVersionMismatch,
                         ChecksumMismatch,
                         NoMetadataFound,
                         NoChangeInMetadata,
                         )
from .headers import (create_metadata_header,
                      decode_metadata_header,
                      decode_blosc_header,
                      BloscPackHeader,
                      decode_int64,
                      encode_int64,
                      )
from .pretty import (double_pretty_size,
                     )
from .serializers import (SERIALIZERS_LOOKUP
                          check_valid_serializer,
                          )
import log


def _write_metadata(output_fp, metadata, metadata_args):
    """ Write the metadata to a file pointer.

    Parameters
    ----------
    output_fp : file like
        the file pointer to write to
    metadata : dict
        the metadata to write
    metadata_args : dict
        the metadata args

    Returns
    -------
    metadata_total : int
        the total number of bytes occupied by metadata header, metadata plus
        preallocation and checksum

    Notes
    -----
    The 'output_fp' should point to the position in the file where the metadata
    should be written.

    """
    _check_metadata_arguments(metadata_args)
    metadata_total = 0
    log.debug('metadata args are:')
    for arg, value in metadata_args.iteritems():
        log.debug('\t%s: %s' % (arg, value))
    metadata_total += METADATA_HEADER_LENGTH
    serializer_impl = SERIALIZERS_LOOKUP[metadata_args['magic_format']]
    metadata = serializer_impl.dumps(metadata)
    codec = 'None'
    if metadata_args['meta_codec'] != CODECS_AVAIL[0]:
        codec_impl = CODECS_LOOKUP[metadata_args['meta_codec']]
        metadata_compressed = codec_impl.compress(metadata,
                metadata_args['meta_level'])
        meta_size = len(metadata)
        meta_comp_size = len(metadata_compressed)
        # be opportunistic, avoid compression if not beneficial
        if meta_size < meta_comp_size:
            log.debug('metadata compression requested, but it was not '
                    'beneficial, deactivating '
                    "(raw: '%s' vs. compressed: '%s') " %
                    (meta_size, meta_comp_size))
            meta_comp_size = meta_size
        else:
            codec = codec_impl.name
            metadata = metadata_compressed
    else:
        meta_size = len(metadata)
        meta_comp_size = meta_size
    log.debug("Raw %s metadata of size '%s': %s" %
            ('compressed' if metadata_args['meta_codec'] != 'None' else
                'uncompressed', meta_comp_size, repr(metadata)))
    if hasattr(metadata_args['max_meta_size'], '__call__'):
        max_meta_size = metadata_args['max_meta_size'](meta_size)
    elif isinstance(metadata_args['max_meta_size'], int):
        max_meta_size = metadata_args['max_meta_size']
    log.debug('max meta size is deemed to be: %d' %
            max_meta_size)
    if meta_comp_size > max_meta_size:
        raise MetadataSectionTooSmall(
                'metadata section is too small to contain the metadata '
                'required: %d allocated: %d' %
                (meta_comp_size, max_meta_size))
    metadata_total += meta_comp_size
    # create metadata header
    raw_metadata_header = create_metadata_header(
            magic_format=metadata_args['magic_format'],
            meta_checksum=metadata_args['meta_checksum'],
            meta_codec=codec,
            meta_level=metadata_args['meta_level'],
            meta_size=meta_size,
            max_meta_size=max_meta_size,
            meta_comp_size=meta_comp_size)
    log.debug('raw_metadata_header: %s' % repr(raw_metadata_header))
    output_fp.write(raw_metadata_header)
    output_fp.write(metadata)
    prealloc = max_meta_size - meta_comp_size
    for i in xrange(prealloc):
        output_fp.write('\x00')
    metadata_total += prealloc
    log.debug("metadata has %d preallocated empty bytes" % prealloc)
    if metadata_args['meta_checksum'] != CHECKSUMS_AVAIL[0]:
        metadata_checksum_impl = CHECKSUMS_LOOKUP[metadata_args['meta_checksum']]
        metadata_digest = metadata_checksum_impl(metadata)
        metadata_total += metadata_checksum_impl.size
        output_fp.write(metadata_digest)
        log.debug("metadata checksum (%s): %s" %
                (metadata_args['meta_checksum'], repr(metadata_digest)))
    log.debug("metadata section occupies a total of %s" %
            double_pretty_size(metadata_total))
    return metadata_total


def _read_bloscpack_header(input_fp):
    """ Read the bloscpack header.

    Parameters
    ----------
    input_fp : file like
        a file pointer to read from

    Returns
    -------
    bloscpack_header : BloscPackHeader
        the decoded bloscpack header

    Raises
    ------
    FormatVersionMismatch
        if the received format version is not equal to the one this module can
        decode

    """
    log.debug('reading bloscpack header')
    bloscpack_header_raw = input_fp.read(BLOSCPACK_HEADER_LENGTH)
    log.debug('bloscpack_header_raw: %s' %
              repr(bloscpack_header_raw))
    bloscpack_header = BloscPackHeader.decode(bloscpack_header_raw)
    log.debug("bloscpack header: %s" % repr(bloscpack_header))
    if FORMAT_VERSION != bloscpack_header.format_version:
        raise FormatVersionMismatch(
                "format version of file was not '%s' as expected, but '%d'" %
                (FORMAT_VERSION, bloscpack_header.format_version))
    return bloscpack_header


def _read_metadata(input_fp):
    """ Read the metadata and header from a file pointer.

    Parameters
    ----------
    input_fp : file like
        a file pointer to read from

    Returns
    -------
    metadata : dict
        the metadata
    metadata_header : dict
        the metadata contents as dict

    Notes
    -----
    The 'input_fp' should point to the position where the metadata starts. The
    number of bytes to read will be determined from the metadata header.

    """
    raw_metadata_header = input_fp.read(METADATA_HEADER_LENGTH)
    log.debug("raw metadata header: '%s'" % repr(raw_metadata_header))
    metadata_header = decode_metadata_header(raw_metadata_header)
    log.debug("metadata header: ")
    for arg, value in metadata_header.iteritems():
        log.debug('\t%s: %s' % (arg, value))
    metadata = input_fp.read(metadata_header['meta_comp_size'])
    prealloc = metadata_header['max_meta_size'] - metadata_header['meta_comp_size']
    input_fp.seek(prealloc, 1)
    if metadata_header['meta_checksum'] != 'None':
        metadata_checksum_impl = CHECKSUMS_LOOKUP[metadata_header['meta_checksum']]
        metadata_expected_digest = input_fp.read(metadata_checksum_impl.size)
        metadata_received_digest = metadata_checksum_impl(metadata)
        if metadata_received_digest != metadata_expected_digest:
            raise ChecksumMismatch(
                    "Checksum mismatch detected in metadata "
                    "expected: '%s', received: '%s'" %
                    (repr(metadata_expected_digest),
                        repr(metadata_received_digest)))
        else:
            log.debug('metadata checksum OK (%s): %s ' %
                    (metadata_checksum_impl.name,
                        repr(metadata_received_digest)))
    if metadata_header['meta_codec'] != 'None':
        metadata_codec_impl = CODECS_LOOKUP[metadata_header['meta_codec']]
        metadata = metadata_codec_impl.decompress(metadata)
    log.verbose("read %s metadata of size: '%s'" %
            # FIXME meta_codec?
            ('compressed' if metadata_header['meta_codec'] != 'None' else
                'uncompressed', metadata_header['meta_comp_size']))
    serializer_impl = SERIALIZERS_LOOKUP[metadata_header['magic_format']]
    metadata = serializer_impl.loads(metadata)
    return metadata, metadata_header


def _read_offsets(input_fp, bloscpack_header):
    """ Read the offsets from a file pointer.

    Parameters
    ----------
    input_fp : file like
        a file pointer to read from

    Returns
    -------
    offsets : list of int
        the offsets

    Notes
    -----
    The 'input_fp' should point to the position where the offsets start. Any
    unused offsets will not be returned.

    """
    if bloscpack_header.offsets:
        total_entries = bloscpack_header.nchunks + \
                bloscpack_header.max_app_chunks
        offsets_raw = input_fp.read(8 * total_entries)
        log.debug('Read raw offsets: %s' % repr(offsets_raw))
        offsets = [decode_int64(offsets_raw[j - 8:j]) for j in
                   xrange(8, bloscpack_header.nchunks * 8 + 1, 8)]
        log.debug('Offsets: %s' % offsets)
        return offsets
    else:
        return []


def _read_beginning(input_fp):
    """ Read the bloscpack_header, metadata, metadata_header and offsets.

    Parameters
    ----------
    input_fp : file like
        input file pointer

    Returns
    -------
    bloscpack_header : dict
    metadata : object
    metadata_header : dict
    offsets : list of ints

    """
    bloscpack_header = _read_bloscpack_header(input_fp)
    metadata, metadata_header = _read_metadata(input_fp) \
            if bloscpack_header.metadata\
            else (None, None)
    offsets = _read_offsets(input_fp, bloscpack_header)
    return bloscpack_header, metadata, metadata_header, offsets


def _write_offsets(output_fp, offsets):
    log.debug("Writing '%d' offsets: '%s'" %
              (len(offsets), repr(offsets)))
    # write the offsets encoded into the reserved space in the file
    encoded_offsets = "".join([encode_int64(i) for i in offsets])
    log.debug("Raw offsets: %s" % repr(encoded_offsets))
    output_fp.write(encoded_offsets)


def _read_compressed_chunk_fp(input_fp, checksum_impl):
    """ Read a compressed chunk from a file pointer.

    Parameters
    ----------
    input_fp : file like
        the file pointer to read the chunk from
    checksum_impl : Checksum
        the checksum that has been used

    Returns
    -------
    compressed : str
        the compressed data
    blosc_header : dict
        the blosc header from the chunk
    """
    # read blosc header
    blosc_header_raw = input_fp.read(BLOSC_HEADER_LENGTH)
    blosc_header = decode_blosc_header(blosc_header_raw)
    if log.LEVEL == log.DEBUG:
        log.debug('blosc_header: %s' % repr(blosc_header))
    ctbytes = blosc_header['ctbytes']
    # Seek back BLOSC_HEADER_LENGTH bytes in file relative to current
    # position. Blosc needs the header too and presumably this is
    # better than to read the whole buffer and then concatenate it...
    input_fp.seek(-BLOSC_HEADER_LENGTH, 1)
    # read chunk
    compressed = input_fp.read(ctbytes)
    if checksum_impl.size > 0:
        # do checksum
        expected_digest = input_fp.read(checksum_impl.size)
        received_digest = checksum_impl(compressed)
        if received_digest != expected_digest:
            raise ChecksumMismatch(
                    "Checksum mismatch detected in chunk, "
                    "expected: '%s', received: '%s'" %
                    (repr(expected_digest), repr(received_digest)))
        else:
            log.debug('checksum OK (%s): %s ' %
                      (checksum_impl.name, repr(received_digest)))
    return compressed, blosc_header


def _seek_to_metadata(target_fp):
    """ Given a target file pointer, seek to the metadata section.

    Parameters
    ----------

    target_fp : file like
        the target file pointer

    Returns
    -------
    metadata_position : int

    Raises
    ------
    NoMetadataFound
        if there is no metadata section in this file

    """
    bloscpack_header = _read_bloscpack_header(target_fp)
    if not bloscpack_header.metadata:
        raise NoMetadataFound("unable to seek to metadata if it does not exist")
    else:
        return target_fp.tell()


def _rewrite_metadata_fp(target_fp, new_metadata,
                         magic_format=None, checksum=None,
                         codec=DEFAULT_META_CODEC, level=DEFAULT_META_LEVEL):
    """ Rewrite the metadata section in a file pointer.

    Parameters
    ----------
    target_fp : file like
        the target file pointer to rewrite in
    new_metadata: dict
        the new metadata to save

    See the notes in ``_recreate_metadata`` for a description of the keyword
    arguments.

    """
    # cache the current position
    current_pos = target_fp.tell()
    # read the metadata section
    old_metadata, old_metadata_header = _read_metadata(target_fp)
    if old_metadata == new_metadata:
        raise NoChangeInMetadata(
                'you requested to update metadata, but this has not changed')
    new_metadata_args = _recreate_metadata(old_metadata_header, new_metadata,
            magic_format=magic_format, checksum=checksum,
            codec=codec, level=level)
    # seek back to where the metadata begins...
    target_fp.seek(current_pos, 0)
    # and re-write it
    _write_metadata(target_fp, new_metadata, new_metadata_args)


def _recreate_metadata(old_metadata_header, new_metadata,
                       magic_format=None, checksum=None,
                       codec=DEFAULT_META_CODEC, level=DEFAULT_META_LEVEL):
    """ Update the metadata section.

    Parameters
    ----------
    old_metadata_header: dict
        the header of the old metadata
    new_metadata: dict
        the new metadata to save

    See the notes below for a description of the keyword arguments.

    Returns
    -------
    new_metadata_args: dict
        the new arguments for ``_write_metadata``

    Raises
    ------
    ChecksumLengthMismatch
        if the new checksum has a different length than the old one
    NoChangeInMetadata
        if the metadata has not changed

    Notes
    -----
    This create new ``metadata_args`` based on an old metadata_header, Since
    the space has already been allocated, only certain metadata arguments can
    be overridden. The keyword arguments specify which ones these are. If a
    keyword argument value is 'None' the existing argument which is obtained
    from the header is used.  Otherwise the value from the keyword argument
    takes precedence. Due to a policy of opportunistic compression, the 'codec'
    and 'level' arguments are not 'None' by default, to ensure that previously
    uncompressed metadata, which might be favourably compressible as a result
    of the enlargement process, will actually be compressed. As for the
    'checksum' only a checksum with the same digest size can be used.

    The ``metadata_args`` returned by this function are suitable to be passed
    on to ``_write_metadata``.

    """
    # get the settings from the metadata header
    metadata_args = dict((k, old_metadata_header[k]) for k in METADATA_ARGS)
    # handle and check validity of overrides
    if magic_format is not None:
        check_valid_serializer(magic_format)
        metadata_args['magic_format'] = magic_format
    if checksum is not None:
        check_valid_checksum(checksum)
        old_impl = CHECKSUMS_LOOKUP[old_metadata_header['meta_checksum']]
        new_impl = CHECKSUMS_LOOKUP[checksum]
        if old_impl.size != new_impl.size:
            raise ChecksumLengthMismatch(
                    'checksums have a size mismatch')
        metadata_args['meta_checksum'] = checksum
    if codec is not None:
        check_valid_codec(codec)
        metadata_args['meta_codec'] = codec
    if level is not None:
        check_range('meta_level', level, 0, MAX_CLEVEL)
        metadata_args['meta_level'] = level
    return metadata_args
