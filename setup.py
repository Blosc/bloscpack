#!/usr/bin/env python
# -*- coding: utf-8 -*-
# flake8: noqa

from distutils.core import setup

with open('README.rst') as fp:
    long_description = fp.read()

with open('bloscpack/version.py') as fp:
    version = fp.read()
    exec(version)

setup(
    name = "bloscpack",
    version = __version__,
    py_modules = ['bloscpack'],
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
