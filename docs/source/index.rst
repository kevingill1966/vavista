.. vavista documentation master file, created by
   sphinx-quickstart on Mon Jun 18 22:26:32 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

vavista's documentation!
========================

vavista provides a Python namespace package for working with 
`VistA, the USA Department of Veterans Affairs' Health Information System and
Technology Architecture <http://en.wikipedia.org/wiki/VistA>`_.

This initial release of the package provides two modules, vavista._gtm and vavista.M.
vavista._gtm provides a lowlevel mapping to the GT.M database and mumps implementation,
and vavista.M provides a higher level API intended to make it relatively simple for
Python programmers to work with VistA code and data.

Contents
--------

.. toctree::
   :maxdepth: 1

   install
   M
   fileman
   rpc


Contributing
------------

vavista is on GitHub: http://github.com/kevingill1966/vavista. Fork away. Pull
requests are welcome!

Running the tests
-----------------

To run the tests::

    $ python setup.py test


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

