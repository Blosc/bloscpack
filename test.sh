#!/bin/sh
echo "Testing command line interface with cram"
COVERAGE=1 ./test_bloscpack.cram
echo "Executing unit tests with nosetests"
nosetests --with-coverage --cover-package=bloscpack test
