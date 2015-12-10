#!/usr/bin/env python
# -*- coding: utf-8 -*-
# flake8: noqa

from setuptools import setup
import sys

with open('README.rst') as f:
    long_description = f.read()

with open('bloscpack/version.py') as f:
    exec(f.read())

install_requires = [
    'blosc==1.2.7',
    'numpy',
    'six',
]

# Dependencies for 2.6
if sys.version_info[:2] < (2, 7):
    install_requires += ['ordereddict', 'argparse']

tests_require = [
    'nose',
    'cram>=0.6',
    'mock',
    'coverage',
    'coveralls'
]

setup(
    name = "bloscpack",
    version = __version__,
    packages = ['bloscpack'],
    entry_points = {
        'console_scripts' : [
            'blpk = bloscpack.cli:main',
        ]
    },
    author = "Valentin Haenel",
    author_email = "valentin@haenel.co",
    description = "Command line interface to and serialization format for Blosc",
    long_description=long_description,
    license = "MIT",
    keywords = ('compression', 'applied information theory'),
    url = "https://github.com/blosc/bloscpack",
    install_requires = install_requires,
    extras_require = dict(tests=tests_require),
    tests_require = tests_require,
    classifiers = ['Development Status :: 3 - Alpha',
                   'Environment :: Console',
                   'License :: OSI Approved :: MIT License',
                   'Operating System :: Microsoft :: Windows',
                   'Operating System :: POSIX',
                   'Programming Language :: Python',
                   'Topic :: Scientific/Engineering',
                   'Topic :: System :: Archiving :: Compression',
                   'Topic :: Utilities',
                   'Programming Language :: Python',
                   'Programming Language :: Python :: 2.6',
                   'Programming Language :: Python :: 2.7',
                   'Programming Language :: Python :: 3.4',
                   'Programming Language :: Python :: 3.5',
                  ],
     )
