#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import abc
import collections
import copy
import pprint


from .pretty import (double_pretty_size,
                     )


class MutableMappingObject(collections.MutableMapping):

    _metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def attributes(self):
        pass

    @abc.abstractproperty
    def bytes_attributes(self):
        pass

    @property
    def _class_name(self):
        return type(self).__name__

    def __getitem__(self, key):
        if key not in self.attributes:
            raise KeyError('%s not in %s' % (key, self._class_name))
        return getattr(self, key)

    def __setitem__(self, key, value):
        if key not in self.attributes:
            raise KeyError('%s not in %s' % (key, self._class_name))
        setattr(self, key, value)

    def __delitem__(self, key):
        raise NotImplementedError(
            '%s does not support __delitem__ or derivatives'
            % self._class_name)

    def __len__(self):
        return len(self.attributes)

    def __iter__(self):
        return iter(self.attributes)

    def __str__(self):
        return pprint.pformat(dict(self))

    def __repr__(self):
        args = (("%s=%s" % (arg, repr(value))) for arg, value in self.items())
        return "%s(%s)" % (self._class_name, ", ".join(args))

    def pformat(self, indent=4):
        indent = " " * indent
        # don't ask, was feeling functional
        return "%s:\n%s%s" % (self._class_name, indent, ("\n%s" % indent).join((("%s: %s" %
            (key, (repr(value) if (key not in self.bytes_attributes or value == -1)
                               else double_pretty_size(value)))
             for key, value in self.items()))))

    def copy(self):
        return copy.copy(self)
