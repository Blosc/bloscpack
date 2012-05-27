#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup
import bloscpack as bp

setup(
    name = "bloscpack",
    version = bp.__version__,
    modules = ['bloscpack'],
    scripts = ['blpk'],
    author = "Valentin Haenel",
    author_email = "valentin.haenel@gmx.de",
    description = "Command line interface to Blosc via python-blosc",
    license = "MIT",
    keywords = ('compression', 'applied information theory'),
    url = "https://github.com/esc/bloscpack",
    classifiers = ['Development Status :: 3 - Alpha',
                   'Environment :: Console',
                   'License :: OSI Approved ::  MIT',
                   'Operating System :: POSIX',
                   'Programming Language :: Python',
                   'Topic :: Scientific/Engineering',
                   'Topic :: Utilities',
                  ],
     )
