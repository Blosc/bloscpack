#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


class FileNotFound(IOError):
    pass


class NoSuchChecksum(ValueError):
    pass
