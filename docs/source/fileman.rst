
vavista.fileman (api for working with fileman files)
====================================================

This is currently under development. 

DBS
---

VistA provides two librarys for accessing Fileman. This implementation is based
on DBS, which does not involve user input.

To import::

    from vavista import fileman

Create a connection object. This takes two parameters, the user id (DUZ) variable and
DT (the date/time format string). These are passed to the Mumps interpreter.::

    dbs = fileman.connect("0", "")

Files
-----

VistA data, for the most part, is stored in Fileman "Files". These are areas
of persistent globals which are accessed via Fileman APIs. There is a class
called DBSFile wrapping file level logic. Request the file object from the
DBS instance.::

    patients = dbs.get_file('PATIENT')

Once you have the file, you can retrieve data from it. The data is retrieved
in rows.::

    patient1 = patients.get('1')

Example::

    from vavista.fileman import connect
    dbs = connect("0", "")
    patients = dbs.get_file('PATIENT')
    patient1 = patients.get('1')
    print patient1.NAME
    print patient1

Modifying data
--------------

You can modify files. Files can only be modified in a transaction. The
updates are written to fileman when the transaction commit occurs.


::

    from vavista.fileman import connect, transaction
    transaction.begin()
    dbs = connect("0", "")
    patients = dbs.get_file('PATIENT')
    patient2 = patients.get('2')
    patient2.NAME = 'FIRST,LAST'
    transaction.commit()    # writes out here.

To create a new record, use the new() operator on the FILE.::

    from vavista.fileman import connect, transaction
    transaction.begin()
    dbs = connect("0", "")
    patients = dbs.get_file('PATIENT')
    patientn = patients.new()
    patientn.NAME = 'NEW,PATIENT'
    transaction.commit()    # writes out here.

Locking
-------

Once a record is modified, the row is locked in the database. Locks are
released on transaction commit/abort, and on process exit.

GT.M has a lock manager called lke. 

::

    $ lke
    LKE> SHOW -ALL

    DEFAULT
    ^DIZ(999900,18) Owned by PID= 1475 which is an existing process


WIP
---

The insert / update logic has only been validated with very simple field types.
The full set of field types has to be investigated.

sub-files, references and back references have to be investigated.

I have to verify that inserts/updates maintain integrity of indexes, audit.

I have to test with non-programming user and understand the security 
infrastructure.

I need index and file iterators, so that I can produce a resultset.

I need flags to get() and new() to use internal instead of external form data.

I need functions to create simple tables so that I can build automated
tests.

How to delete records. Seems to be classic api, but no DBS api call.
There also seems to be no interactive option.
