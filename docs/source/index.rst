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

Installing (with GT.M)
----------------------

Before the _gtm package can run, you must configure a number of environment variables.

For Ubuntu, I use the LD_LIBRARY_PATH varaiable to locate the GT.M shared library.::

    export LD_LIBRARY_PATH=/usr/lib/fis-gtm/V5.4-002B_x86

You also need to inform the package of the location of your GT.M globals directory,
and your routines paths. Here is how I set them up on Ubuntu.::

    export vista_home="/home/vademo4-09/EHR"
    export gtm_dist="/usr/local/gtm"
    export gtmgbldir="$vista_home/g/mumps.gld"
    export gtmroutines="$vista_home/o($vista_home/r) $gtm_dist"

Ultimately, vavista should install directly from pypi using ``pip``::

    $ pip install vavista

However, this has not yet been completed. [TODO: compile .m file to .o]

Instead, clone the archive and use setuputils::

    $ git clone http://github.com/kevingill1966/vavista
    $ cd vavista
    $ python setup.py test
    $ sudo python setup.py install

To build this documentation::

    $ sudo pip install sphinx
    $ python setup.py build_sphinx


Contents
--------

.. toctree::
   :maxdepth: 1

   _gtm
   M

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

