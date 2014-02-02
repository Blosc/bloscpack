#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


import nose.tools as nt


from bloscpack.metacodec import CODECS, CODECS_AVAIL

from bloscpack import DEFAULT_META_LEVEL


def test_codecs():
    nt.assert_equal(CODECS_AVAIL, ['None', 'zlib'])
    random_str = "4KzGCl7SxTsYLaerommsMWyZg1TXbV6wsR9Xk"
    for i, c in enumerate(CODECS):
        nt.assert_equal(random_str, c.decompress(
            c.compress(random_str, DEFAULT_META_LEVEL)))
