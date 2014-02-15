#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

from __future__ import division


from .constants import SUFFIXES


def pretty_size(size_in_bytes):
    """ Pretty print filesize.  """
    if size_in_bytes == 0:
        return "0B"
    for suf, lim in reversed(sorted(SUFFIXES.items(), key=lambda x: x[1])):
        if size_in_bytes < lim:
            continue
        else:
            return str(round(size_in_bytes/lim, 2))+suf


def double_pretty_size(size_in_bytes):
    """ Pretty print filesize including size in bytes. """
    return ("%s (%dB)" % (pretty_size(size_in_bytes), size_in_bytes))


def reverse_pretty(readable):
    """ Reverse pretty printed file size. """
    # otherwise we assume it has a suffix
    suffix = readable[-1]
    if suffix not in SUFFIXES.keys():
        raise ValueError(
                "'%s' is not a valid prefix multiplier, use one of: '%s'" %
                (suffix, SUFFIXES.keys()))
    else:
        return int(float(readable[:-1]) * SUFFIXES[suffix])


def join_with_eol(items):
    return ', '.join(items) + '\n'
