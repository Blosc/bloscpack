#!/usr/bin/env nosetests
# -*- coding: utf-8 -*-
# vim :set ft=py:


from mock import patch, Mock
import nose.tools as nt

from bloscpack import cli
from bloscpack.exceptions import FileNotFound

def test_parser():
    # hmmm I guess we could override the error
    parser = cli.create_parser()


@patch('os.path.exists')
def test_non_existing_input_file_raises_exception(mock_exists):
    args = Mock(force=False)
    mock_exists.return_value = False
    nt.assert_raises(FileNotFound,
                     cli.check_files,
                     'nosuchfile',
                     'nosuchfile',
                     args)


@patch('os.path.exists')
def test_existing_output_file_raises_exception(mock_exists):
    args = Mock(force=False)
    mock_exists.side_effects = [True, True]
    nt.assert_raises(FileNotFound,
                     cli.check_files,
                     'anyfile',
                     'anyfile',
                     args)


@patch('os.path.exists')
def test_check_files_force_works(mock_exists):
    args = Mock(force=True)
    mock_exists.side_effects = [True, True]
    cli.check_files('anyfile', 'anyfile', args)
