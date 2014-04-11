#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import abc
import collections
import copy
import pprint


from .pretty import (double_pretty_size,
                     )


class MutableMappaingObject(collections.MutableMapping):

    _metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def attributes(self):
        pass

    @abc.abstractproperty
    def bytes_attributes(self):
        pass

    def __getitem__(self, key):
        if key not in self.attributes:
            raise KeyError('%s not in %s' % (key, type(self).__name__))
        return getattr(self, key)

    def __setitem__(self, key, value):
        if key not in self.attributes:
            raise KeyError('%s not in %s' % (key, type(self).__name__))
        setattr(self, key, value)

    def __delitem__(self, key):
        raise NotImplementedError(
            '%s does not support __delitem__ or derivatives'
            % type(self).__name__)

    def __len__(self):
        return len(self.attributes)

    def __iter__(self):
        return iter(self.attributes)

    def __str__(self):
        return pprint.pformat(dict(self))

    def __repr__(self):
        return "%s(%s)" % (type(self).__name__,
                          ", ".join((("%s=%s" % (arg, repr(value)))
                          for arg, value in self.iteritems())))

    def pformat(self, indent=4):
        indent = " " * indent
        # don't ask, was feeling functional
        return "bloscpack header: \n%s%s" % (indent, (",\n%s" % indent).join((("%s=%s" % 
            (key, (repr(value) if (key not in self._bytes_attrs or value == -1)
                         else double_pretty_size(value)))
             for key, value in self.iteritems()))))

    def copy(self):
        return copy.copy(self)

