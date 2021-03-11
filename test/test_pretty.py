# -*- coding: utf-8 -*-
# vim :set ft=py:


import pytest


from bloscpack.pretty import (pretty_size,
                              reverse_pretty,
                              )


def test_pretty_filesieze():

    assert '0B' == pretty_size(0)
    assert '9.0T' == pretty_size(9898989898879)
    assert '4.78G' == pretty_size(5129898234)
    assert '12.3M' == pretty_size(12898234)
    assert '966.7K' == pretty_size(989898)
    assert '128.0B' == pretty_size(128)
    assert 0 == reverse_pretty('0B')
    assert 8 == reverse_pretty('8B')
    assert 8192 == reverse_pretty('8K')
    assert 134217728 == reverse_pretty('128M')
    assert 2147483648 == reverse_pretty('2G')
    assert 2199023255552 == reverse_pretty('2T')
    # can't handle Petabytes, yet
    with pytest.raises(ValueError):
        reverse_pretty('2P')
