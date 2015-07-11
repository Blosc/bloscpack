#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


import nose.tools as nt


from bloscpack.args import (DEFAULT_META_LEVEL,
                            )
from bloscpack.metacodecs import (CODECS,
                                  CODECS_AVAIL
                                  )


def test_codecs():
    nt.assert_equal(CODECS_AVAIL, ['None', 'zlib'])
    random_str = b"4KzGCl7SxTsYLaerommsMWyZg1TXbV6wsR9Xk"
    for i, c in enumerate(CODECS):
        nt.assert_equal(random_str, c.decompress(
            c.compress(random_str, DEFAULT_META_LEVEL)))
