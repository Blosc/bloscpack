#!/usr/bin/env cram
# vim: set syntax=sh :

# Bootstrapping script for running cram tests with coverage.
# Insert the following as the first command of a cram test to use:
#
#  $ . $TESTDIR/cram_strap.sh


if [ -n "$COVERAGE" ]; then
alias blpk="COVERAGE_FILE=$TESTDIR/../.coverage $( which coverage ) run --source bloscpack --timid -a $TESTDIR/../blpk"
else
  alias blpk="$TESTDIR/../blpk"
fi

