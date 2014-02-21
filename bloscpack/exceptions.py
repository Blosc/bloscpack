#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


class FileNotFound(IOError):
    pass


class NoSuchChecksum(ValueError):
    pass


class NoSuchCodec(ValueError):
    pass


class NoSuchSerializer(ValueError):
    pass

class ChunkingException(BaseException):
    pass


