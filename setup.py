#!/usr/bin/env python
# -*- coding: utf-8 -*-
# flake8: noqa

from setuptools import setup
import io

with io.open('README.rst', encoding='utf-8') as f:
    long_description = f.read()

with open('bloscpack/version.py') as f:
    exec(f.read())

with open('requirements.txt') as f:
    install_requires = f.readlines()

with open('test_requirements.txt') as f:
    tests_require = f.readlines()

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
    classifiers = ['Development Status :: 4 - Beta',
                   'Environment :: Console',
                   'License :: OSI Approved :: MIT License',
                   'Operating System :: Microsoft :: Windows',
                   'Operating System :: POSIX',
                   'Programming Language :: Python',
                   'Topic :: Scientific/Engineering',
                   'Topic :: System :: Archiving :: Compression',
                   'Topic :: Utilities',
                   'Programming Language :: Python',
                   'Programming Language :: Python :: 2.7',
                   'Programming Language :: Python :: 3.4',
                   'Programming Language :: Python :: 3.5',
                   'Programming Language :: Python :: 3.6',
                   'Programming Language :: Python :: 3.7',
                  ],
     )
