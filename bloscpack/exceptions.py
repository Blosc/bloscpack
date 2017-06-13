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


class ChunkSizeTypeSizeMismatch(ValueError):
    pass


class ChecksumMismatch(RuntimeError):
    pass


class FormatVersionMismatch(RuntimeError):
    pass


class ChecksumLengthMismatch(RuntimeError):
    pass


class NoMetadataFound(RuntimeError):
    pass


class NoChangeInMetadata(RuntimeError):
    pass


class MetadataSectionTooSmall(RuntimeError):
    pass


class NonUniformTypesize(RuntimeError):
    pass


class NotEnoughSpace(RuntimeError):
    pass


class NotANumpyArray(RuntimeError):
    pass


class ObjectNumpyArrayRejection(RuntimeError):
    pass
