
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


