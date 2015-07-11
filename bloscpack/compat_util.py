#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

try:
    from cStringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO

try:
    from collections import OrderedDict
except ImportError:  # pragma: no cover
    from ordereddict import OrderedDict
