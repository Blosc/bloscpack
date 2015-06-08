#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

import nose.tools as nt


try:
    from collections import OrderedDict
except ImportError:  # pragma: no cover
    from ordereddict import OrderedDict


from bloscpack.serializers import (SERIALIZERS,
                                   SERIALIZERS_AVAIL,
                                   )


def test_serializers():
    nt.assert_equal(SERIALIZERS_AVAIL, [b'JSON'])
    output = '{"dtype":"float64","shape":[1024],"others":[]}'
    input_ = OrderedDict([('dtype', "float64"),
                         ('shape', [1024]),
                         ('others', [])])
    for s in SERIALIZERS:
        yield nt.assert_equal, output, s.dumps(input_)
        yield nt.assert_equal, input_, s.loads(output)
