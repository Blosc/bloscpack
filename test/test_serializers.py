#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

import nose.tools as nt


from bloscpack.serializers import (SERIALIZERS,
                                   SERIALIZERS_AVAIL,
                                   )


def test_serializers():
    nt.assert_equal(SERIALIZERS_AVAIL, ['JSON'])
    output = '{"dtype":"float64","shape":[1024],"others":[]}'
    input_ = eval(output)
    for s in SERIALIZERS:
        nt.assert_equal(output, s.dumps(input_))
        nt.assert_equal(input_, s.loads(output))
