
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

    patients = dbs.get_file('PATIENT', internal=True, fieldids=None)

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

Searching
---------

The fileman files contain indexes. You can list the indexes using the data
dictionary::

    from vavista.fileman import connect
    dbs = connect("0", "")
    patients = dbs.get_file('PATIENT')

    print patients.dd.indices[0]
    > Index(AAICN) on table 2.0992, columns ['.01']
    print patients.dd.indices[1]
    > Index(AAP) on table 2, columns ['.1041']
    print patients.dd.indices[2]
    > Index(AB) on table 2.312, columns ['.01']

To search, the low level functions just walk the index, returning (key, rowid)
pairs::

    cursor = patients.traverser("SSN")
    for i in range(5): print cursor.next()
    ('443483527', '702'),
    ('666000000', '100014'),
    ('666000001', '237'),
    ('666000002', '205'),
    ('666000003', '25')

Note: the traverser is walking the index on each call to next. If you evaluate
the cursor, it will walk the entire cursor before returning.

To and From values. The start and end point to investigate can be included.
By default the start value is included but the end value is excluded. ::

    cursor = patients.traverser("SSN", '666000001', '666000003')
    print list(cursor)
    > [('666000001', '237'), ('666000002', '205')]

You can change the order of the search::

    cursor = patients.traverser("SSN", '666000003', '666000001', ascending=False)
    print list(cursor)
    [('666000003', '25'), ('666000002', '205')]

By default, the from value is included, but the to value is excluded, e.g. to get
the 666's use::

    cursor = patients.traverser("SSN", '666', '667')
    print list(cursor)

You can include change the inclusion rules::

    cursor = patients.traverser("SSN", '666000001', '666000003', to_rule="<=", from_rule=">=")
    print list(cursor)
    [('666000001', '237'), ('666000002', '205'), ('666000003', '25')]


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


Internal Versus External
------------------------

Fileman supports a notion of internal and external representation of data.
External representation is a UI concept, converting 0/1 to Yes/No or looking
up foreign keys on tables.

I feel that developers will work with "internal" format data. UI considerations
are the realm of the toolkits, not the database layer.

To get internal format, use...::

    patients = dbs.get_file('PATIENT')

To get external format, use...::
    
    patients = dbs.get_file('PATIENT', internal=False)

A huge consideration here is dates. It would be silly to allow Fileman to
format dates for presentation. However, the internal format is not great.
I need to covert dates to datetime formats.

The knock-on is that I should consider converting other types, when using
"internal" representation.

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

There doesn't seem to be an api to create files. You seem to have to
create them interactively, and then dump the globals. 

The idea of presenting the mumps values to the application is not
sound. Use the fieldtypes in the data dictionary to convert between
the fileman storage and the python space.
