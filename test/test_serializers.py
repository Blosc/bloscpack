#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

try:
    from collections import OrderedDict
except ImportError:  # pragma: no cover
    from ordereddict import OrderedDict


from bloscpack.serializers import (SERIALIZERS,
                                   SERIALIZERS_AVAIL,
                                   )


def test_serializers():
    assert SERIALIZERS_AVAIL == [b'JSON']
    output = '{"dtype":"float64","shape":[1024],"others":[]}'
    input_ = OrderedDict([('dtype', "float64"),
                         ('shape', [1024]),
                         ('others', [])])
    for s in SERIALIZERS:
        assert output == s.dumps(input_)
        assert input_ == s.loads(output)
