#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

import json


from .exceptions import NoSuchSerializer


class Serializer(object):
    """ Uniform serializer object.

    Parameters
    ----------
    name : str
        the name of the serializer
    compress : callable
        a compression function taking a dict as arg
    decompress : callable
        a decompression function taking serialized data as arg

    """
    def __init__(self, name, dumps, loads):
        self.name = name
        self._loads = loads
        self._dumps = dumps

    def dumps(self, dict_):
        return self._dumps(dict_)

    def loads(self, data):
        return self._loads(data)


SERIALIZERS = [Serializer(b'JSON',
                  lambda x: json.dumps(x, separators=(',', ':')),
                  lambda x: json.loads(x))]
SERIALIZERS_AVAIL = [s.name for s in SERIALIZERS]
SERIALIZERS_LOOKUP = dict(((s.name, s) for s in SERIALIZERS))


def check_valid_serializer(serializer):
    """ Check the validity of a serializer.

    Parameters
    ----------
    serializer : str
        the magic format of the serializer

    Raises
    ------
    NoSuchSerializer
        if no such serializer exists.
    """
    if serializer not in SERIALIZERS_AVAIL:
        raise NoSuchSerializer("serializer '%s' does not exist" % serializer)

