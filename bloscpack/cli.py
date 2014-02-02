#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


from os import path

from bloscpack import print_verbose
from .exceptions import FileNotFound


def check_files(in_file, out_file, args):
    """ Check files exist/don't exist.

    Parameters
    ----------
    in_file : str:
        the input file
    out_file : str
        the output file
    args : parser args
        any additional arguments from the parser

    Raises
    ------
    FileNotFound
        in case any of the files isn't found.

    """
    if not path.exists(in_file):
        raise FileNotFound("input file '%s' does not exist!" % in_file)
    if path.exists(out_file):
        if not args.force:
            raise FileNotFound("output file '%s' exists!" % out_file)
        else:
            print_verbose("overwriting existing file: '%s'" % out_file)
    print_verbose("input file is: '%s'" % in_file)
    print_verbose("output file is: '%s'" % out_file)


