#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

""" Command line interface to Blosc via python-blosc """

from .version import __version__  # pragma: no cover

from .args import (BloscArgs,
                   BloscpackArgs,
                   MetadataArgs,
                   )
from .file_io import (pack_file,
                      pack_file_to_file,
                      unpack_file,
                      unpack_file_from_file,
                      pack_bytes_file,
                      pack_bytes_to_file,
                      unpack_bytes_file,
                      unpack_bytes_from_file,
                      pack_bytes_to_bytes,
                      unpack_bytes_from_bytes,
                      )
from .numpy_io import (pack_ndarray_file,
                       pack_ndarray_to_file,
                       unpack_ndarray_from_file,
                       unpack_ndarray_file,
                       pack_ndarray_str,
                       pack_ndarray_to_bytes,
                       unpack_ndarray_str,
                       unpack_ndarray_from_bytes,
                       )
