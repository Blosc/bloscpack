#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import os  # pragma: no cover


def drop_caches():  # pragma: no cover
    if os.geteuid() == 0:
        os.system('echo 3 > /proc/sys/vm/drop_caches')
    else:
        raise RuntimeError('Need root permission to drop caches')


def sync():  # pragma: no cover
    os.system('sync')
