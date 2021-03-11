import pytest
from mock import patch


from bloscpack import log


def test_verbose():
    with pytest.raises(TypeError):
        log.verbose('message', 'MAXIMUM')
    log.set_level(log.DEBUG)
    # should probably hijack the print statement
    log.verbose('notification')
    log.set_level(log.NORMAL)


@patch('sys.exit')
def test_error(exit_mock):
    log.error('error')
    exit_mock.assert_called_once_with(1)


def test_set_level_exception():
    with pytest.raises(ValueError):
        log.set_level('NO_SUCH_LEVEL')
