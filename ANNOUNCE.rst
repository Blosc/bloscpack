===========================
Announcing Bloscpack 0.15.0
===========================

What is new?
============

Two new high-level API functions have been added:

* 'pack_bytes_to_bytes'
* 'unpack_bytes_from_bytes'

As you might expect from the naming, these allow you to perform fully
in-memory based compression and decompression via the bytes datatype.

Additionally there are a  few bugfixes, support for python-blosc
1.6.1 and support for Python 3.7.

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
