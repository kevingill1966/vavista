Installing (with GT.M)
======================

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

However, this has not yet been completed. 

Instead, clone the archive and use setuputils::

    $ git clone http://github.com/kevingill1966/vavista
    $ cd vavista
    $ python setup.py test
    $ sudo python setup.py install

To build this documentation::

    $ sudo pip install sphinx
    $ python setup.py build_sphinx

