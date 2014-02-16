#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim :set ft=py:


import sys


NORMAL  = 'NORMAL'
VERBOSE = 'VERBOSE'
DEBUG   = 'DEBUG'
VERBOSITY_LEVELS = (NORMAL, VERBOSE, DEBUG)

LEVEL = NORMAL
PREFIX = "bloscpack.py"


def set_prefix(prefix):
    global PREFIX
    PREFIX = prefix


def print_verbose(message, level=VERBOSE):
    """ Print message with desired verbosity level. """
    if level not in VERBOSITY_LEVELS:
        raise TypeError("Desired level '%s' is not one of %s" % (level,
                        str(VERBOSITY_LEVELS)))
    if VERBOSITY_LEVELS.index(level) <= VERBOSITY_LEVELS.index(LEVEL):
        for line in [l for l in message.split('\n') if l != '']:
            print('%s: %s' % (PREFIX, line))


def print_debug(message):
    """ Print message with verbosity level ``DEBUG``. """
    print_verbose(message, level=DEBUG)


def print_normal(message):
    """ Print message with verbosity level ``NORMAL``. """
    print_verbose(message, level=NORMAL)


def error(message, exit_code=1):
    """ Print message and exit with desired code. """
    for line in [l for l in message.split('\n') if l != '']:
        print('%s: error: %s' % (PREFIX, line))
    sys.exit(exit_code)
