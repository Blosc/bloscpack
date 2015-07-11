#!/usr/bin/env python

import sys

if eval('sys.version_info[0:2] ' + sys.argv[1]):
    sys.exit(0)
else:
    sys.exit(1)
