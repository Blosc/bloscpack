# This is the maintainers undocumented Makefile.
# Nothin to see here, please move along.
.PHONY: build test clean doc

build:

test:
	./test.sh

clean:
	git clean -dfX; git clean -dfx
