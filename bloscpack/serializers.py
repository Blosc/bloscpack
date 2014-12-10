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

class JSONSerializer(object):
    """ Specialized JSON Serializer.

    Includes a special hack to deserialize Numpy 

    """

    name = 'JSON'

    def dumps(self, dict):
        return json.dumps(dict, separators=(',', ':'))

    def loads(self, data):
        x = json.loads(data)
        print x
        if isinstance(x, dict)  and u'container' in x and x[u'container'] == u'numpy':
            x['dtype'] =self._fix_numpy_dtype(x[u'dtype'])
        print x
        return x

    @staticmethod
    def _fix_numpy_dtype(metadata):
        """
        Fix numpy metadata when it goes through JSON.

        JSON converts tuples into lists, on the way down but doesn't convert them
        back on the way up. Also all strings are converted to unicode but not
        convrted back. This is a problem during the initialization of Numpy arrays
        because the resulting datatype isn't understood. This function converts
        nested list of lists into list of tuples and unicode into strings.

        Examples
        --------
        >>> _fix_numpy_dtype([[u'a', u'f8']])
        [('a', 'f8')]
        >>> _fix_numpy_dtype([[u'a', u'f8', 2]])
        [('a', 'f8', 2)]
        >>> _fix_numpy_dtype([[u'a', [[u'b', 'f8']]]])
        [('a', [('b', 'f8')])]
        """
        if isinstance(metadata, list):
            if isinstance(metadata[0], list):
                metadata = map(JSONSerializer._fix_numpy_dtype, metadata)
            else:
                metadata = tuple(map(JSONSerializer._fix_numpy_dtype, metadata))
        elif isinstance(metadata, unicode):
            metadata = str(metadata)
        else:
            # keep metadata as is
            pass
        return metadata

SERIALIZERS = [JSONSerializer()]
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

