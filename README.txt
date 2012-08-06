
vavista

The concept is to create a python namespace containing tools for working with
vista. The initial tools will be to call mumps code from python, and to 
read, write, lock and delete globals.

import vavista.M                # Mumps API
import vavista.fileman          # work with fileman files (new APIs and utilities)
import vavista.rpc              # New style RPC API


To install:

    sudo PATH=/usr/local/gtm:$PATH gtm_dist=$gtm_dist python setup.py install

To build docs:

    python setup.py build_sphinx


Issues
------

I am having trouble with two versions of GT.M on the same system. I need to
install the GT.M componets into the GT.M environment. Look at the axiomware install
again.

A conflict resolution mechanism is needed in the GT.M implementation before
I can overlay transactions on the mumps code.
