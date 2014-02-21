#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:

""" Command line interface to Blosc via python-blosc """

from __future__ import division


from .args import (DEFAULT_META_CODEC,
                   DEFAULT_META_LEVEL,
                   METADATA_ARGS,
                   _check_metadata_arguments,
                   )
from .checksums import (check_valid_checksum,
                        CHECKSUMS_LOOKUP,
                        CHECKSUMS_AVAIL,
                        )
from .constants import (FORMAT_VERSION,
                        BLOSC_HEADER_LENGTH,
                        BLOSCPACK_HEADER_LENGTH,
                        METADATA_HEADER_LENGTH,
                        MAX_CLEVEL,
                        )
from .headers import (BloscPackHeader,
                      check_range,
                      encode_int64,
                      decode_int64,
                      decode_blosc_header,
                      create_metadata_header,
                      decode_metadata_header,
                      )
from .metacodecs import (CODECS_AVAIL,
                         CODECS_LOOKUP,
                         check_valid_codec,
                         )
from .pretty import (double_pretty_size,
                     )
from .serializers import(SERIALIZERS_LOOKUP,
                         check_valid_serializer,
                         )
import log
from .version import __version__  # pragma: no cover


