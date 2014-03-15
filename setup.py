#!/usr/bin/env python
# -*- coding: utf-8 -*-
# flake8: noqa

from distutils.core import setup

long_description = open('README.rst').read()

exec(open('bloscpack/version.py').read())

setup(
    name = "bloscpack",
    version = __version__,
    packages = ['bloscpack'],
    scripts = ['blpk'],
    author = "Valentin Haenel",
    author_email = "valentin@haenel.co",
    description = "Command line interface to and serialization format for Blosc",
    long_description=long_description,
    license = "MIT",
    keywords = ('compression', 'applied information theory'),
    url = "https://github.com/blosc/bloscpack",
    classifiers = ['Development Status :: 3 - Alpha',
                   'Environment :: Console',
                   'License :: OSI Approved :: MIT License',
                   'Operating System :: POSIX',
                   'Programming Language :: Python',
                   'Topic :: Scientific/Engineering',
                   'Topic :: Utilities',
                  ],
     )
