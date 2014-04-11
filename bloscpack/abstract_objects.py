#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import abc
import collections


class MutableMappaingObject(collections.MutableMapping):

    def __getitem__(self, key):
        if key not in self._attrs:
            raise KeyError('%s not in %s' % (key, type(self).__name__))
        return getattr(self, key)

    def __setitem__(self, key, value):
        if key not in self._attrs:
            raise KeyError('%s not in %s' % (key, type(self).__name__))
        setattr(self, key, value)

    def __delitem__(self, key):
        raise NotImplementedError(
            '%s does not support __delitem__ or derivatives'
            % type(self).__name__)

    def __len__(self):
        return len(self._attrs)

    def __iter__(self):
        return iter(self._attrs)
