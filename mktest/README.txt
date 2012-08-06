
To test the fileman system, you need to create a mumps system.

Get the sources:

The vavista module is intended to be used with the OSHERA FOIA distribution
of vista. Clone the code to your home directory

    echo "git clone git://code.osehra.org/VistA-FOIA.git $HOME/VistA-FOIA"


Patch GT.M

TODO: write instructions.


Create the VistA database:

Source the gtm profile

    . /usr/local/gtm/gtmprofile

Source the environment 

    . ./setup_env

Run the initdb.sh script

    ./initdb.sh

This will create a vista database in the current directory. The vista
database contains kernel and fileman. The database is in a subdirectory,
named based on the gtmver environment variable.


You can run the tests against this database.

    cd ..
    sudo PATH=/usr/local/gtm:$PATH gtm_dist=$gtm_dist python setup.py install
    python setup.py test

