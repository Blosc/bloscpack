============================
Announcing Bloscpack v0.16.0
============================

What is new?
============

The Python API naming has been overhauled and a few functions have been
deprecated. Also the documentation for the Python API has been extended
to inlcude more uses cases and potential applications.

A big thank you goes out to Daniel Stender from the Debian project for his
continued efforts to package the Blosc stack -- including python-blosc -- for
Debian. This also means it is likely that a recent version of
bloscpack will be included in Buster.

For more info, have a look at the changelog:

https://github.com/Blosc/bloscpack#changelog

Documentation and examples are available at:

https://github.com/Blosc/bloscpack


What is it?
===========

Bloscpack is a command-line interface and serialization format for
Blosc. Blosc (http://www.blosc.org) is an extremely fast meta-codec
designed for high compression speeds. Bloscpack allows you to use Blosc
from the command-line to compress and decompress files. Additionally,
Bloscpack has a Python-API that allows you to compress and serialize
data to a file system. Additionally, Bloscpack supports efficient
serialization and de-serialization of Numpy arrays and might in fact be
one of the fastest ways to save arrays to disk. Bloscpack uses the
Python bindings for Blosc (http://python-blosc.blosc.org/) under the
hood.

----

  **Enjoy data!**


.. Local Variables:
.. mode: rst
.. coding: utf-8
.. fill-column: 72
.. End:
.. vim: set tw=72:
