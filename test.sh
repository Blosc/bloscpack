#!/bin/sh
if ! $( which coverage > /dev/null ); then
    echo "Error: Coverage script not found!"
    exit 1
fi
echo "Testing command line interface with cram"
COVERAGE=1 cram --verbose $@  test_cmdline/*.cram
cram_exit=$?
echo "Executing unit tests with pytest"
pytest --cov=bloscpack test
pytest_exit=$?
if [ $cram_exit -gt 0 ] || [ $pytest_exit -gt 0 ] ; then
    echo "some tests failed"
    exit 1
fi

