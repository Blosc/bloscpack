# -*- coding: utf-8 -*-
# vim :set ft=py:

from bloscpack.args import (DEFAULT_META_LEVEL,
                            )
from bloscpack.metacodecs import (CODECS,
                                  CODECS_AVAIL
                                  )


def test_codecs():
    assert CODECS_AVAIL == ['None', 'zlib']
    random_str = b"4KzGCl7SxTsYLaerommsMWyZg1TXbV6wsR9Xk"
    for i, c in enumerate(CODECS):
        assert random_str == c.decompress(
            c.compress(random_str, DEFAULT_META_LEVEL))
