#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import zlib


from .exceptions import NoSuchCodec


class MetaCodec(object):
    """ Uniform codec object.

    Parameters
    ----------
    name : str
        the name of the codec
    compress : callable
        a compression function taking data and level as args
    decompress : callable
        a decompression function taking data as arg

    """

    def __init__(self, name, compress, decompress):
        self.name = name
        self._compress = compress
        self._decompress = decompress

    def compress(self, data, level):
        return self._compress(data, level)

    def decompress(self, data):
        return self._decompress(data)

CODECS = [MetaCodec('None', lambda data, level: data, lambda data: data),
          MetaCodec('zlib', zlib.compress, zlib.decompress)]
CODECS_AVAIL = [c.name for c in CODECS]
CODECS_LOOKUP = dict(((c.name, c) for c in CODECS))


def check_valid_codec(codec):
    """ Check the validity of a codec.

    Parameters
    ----------
    codec : str
        the string descriptor of the codec

    Raises
    ------
    NoSuchCodec
        if no such checksum exists.
    """
    if codec not in CODECS_AVAIL:
        raise NoSuchCodec("codec '%s' does not exist" % codec)
