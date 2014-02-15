#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


import nose.tools as nt


from bloscpack.pretty import (pretty_size,
                              reverse_pretty,
                              )


def test_pretty_filesieze():

    nt.assert_equal('0B', pretty_size(0))
    nt.assert_equal('9.0T', pretty_size(9898989898879))
    nt.assert_equal('4.78G', pretty_size(5129898234))
    nt.assert_equal('12.3M', pretty_size(12898234))
    nt.assert_equal('966.7K', pretty_size(989898))
    nt.assert_equal('128.0B', pretty_size(128))
    nt.assert_equal(0, reverse_pretty('0B'))
    nt.assert_equal(8, reverse_pretty('8B'))
    nt.assert_equal(8192, reverse_pretty('8K'))
    nt.assert_equal(134217728, reverse_pretty('128M'))
    nt.assert_equal(2147483648, reverse_pretty('2G'))
    nt.assert_equal(2199023255552, reverse_pretty('2T'))
    # can't handle Petabytes, yet
    nt.assert_raises(ValueError, reverse_pretty, '2P')
