
vavista.fileman (api for working with fileman files)
====================================================

The vavista fileman API provides a record based API for VistA.

DBS
---

VistA provides two librarys for accessing Fileman. This implementation is based
on DBS, which does not involve user input.

To import::

    from vavista import fileman

Create a connection object. This takes two parameters, the user id (DUZ) variable and
DT (the date/time format string). These are passed to the Mumps interpreter.::

    dbs = fileman.connect("0", "")

If you are running a remote filemand server, you can connect to it via the 
network,::
`
    dbs = fileman.connect("0", "", remote=True, host='localhost', port=9010)

filemand
--------

vavista installs a simple daemon called filemand. This allows the separation
of the client code and the server code. This is particularly useful for 
threaded applications, or where GT.M terminal manipulation would upset the
client screens.::

    filemand --help
    filemand --host localhost --port 9010
    filemand --dameon
    filemand --log /var/log/filemand.log

Files
-----

VistA data, for the most part, is stored in Fileman "Files". These are areas
of persistent globals which are accessed via Fileman APIs. There is a class
called DBSFile wrapping file level logic. Request the file object from the
DBS instance.

You can specify the fieldnames your are interested in when getting the file
handle. Fileman can be very slow if you do not limit your queries to the
fields you are using.::

    patients = dbs.get_file('PATIENT', fieldnames=None)

Once you have the file, you can retrieve data from it. The data is retrieved
in rows. The following will retrieve the data for rowid=1.::

    patient1 = patients.get('1')

Example::

    from vavista.fileman import connect
    dbs = connect("0", "")
    patients = dbs.get_file('PATIENT', fieldnames=['NAME'])
    patient1 = patients.get('1')
    print "patient %s is %s" % (1, patient1[0])
    patient1 = patients.get('1', asdict=True)
    print "patient %s is %s" % (1, patient1['NAME'])

The result from a get is a tuple, containing the requested fields. The layout
of that tuple can be examined using the *description* property on the DBSFile
object.::

    print patients.description

Modifying data
--------------

You can update an existing record in a file.::

    from vavista.fileman import connect, transaction
    transaction.begin()
    dbs = connect("0", "")
    patients = dbs.get_file('PATIENT', fieldnames=['NAME'])
    patients.update(2, NAME='FIRST,LAST')
    transaction.commit()

    patient2 = patients.get('2')
    print "patient %s is %s" % (2, patient2[0])

You can also insert a new record to the file.::

    from vavista.fileman import connect, transaction
    transaction.begin()
    dbs = connect("0", "")
    patients = dbs.get_file('PATIENT', fieldnames=["NAME"])
    rowid = patients.insert(NAME = 'NEW,PATIENT')
    transaction.commit()

    patient3 = patients.get(rowid)
    print "patient %s is %s" % (rowid, patient3[0])

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

    cursor = patients.traverser("SSN", raw=True)
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

    cursor = patients.traverser("SSN", '666000001', '666000003', raw=True)
    print list(cursor)
    > [('666000001', '237'), ('666000002', '205')]

You can change the order of the search::

    cursor = patients.traverser("SSN", '666000003', '666000001', ascending=False, raw=True)
    print list(cursor)
    [('666000003', '25'), ('666000002', '205')]

By default, the From value is included, but the To value is excluded, e.g. to get
the 666's use::

    cursor = patients.traverser("SSN", '666', '667', raw=True)
    print list(cursor)

You can include change the inclusion rules::

    cursor = patients.traverser("SSN", '666000001', '666000003', to_rule="<=", from_rule=">=", raw=True)
    print list(cursor)
    [('666000001', '237'), ('666000002', '205'), ('666000003', '25')]

You can retrieve records by excluding the raw=True flag.

TODO: Fileman has a number of index styles. These have not been investigated fully.

Following Pointers
------------------

Many field in Fileman are Pointers and VPointers. These fields contain a pointer
to a record in another file, e.g. for maintaining vocabularies.

For pointers, the value in the field is the record id of the remote file record.

For variable pointers, the value is a foreign file selector and the record id in the
foreign file (separated by dot).

You can retrieve the remote record using the traverse_pointer function. This is a file
level method, which takes the field name and field value as a parameter.::

        patients = dbs.get_file('PATIENT', fieldnames=['MARITAL_STATUS'])
        ms = patients.get(2)[0]
        print patients.traverse_pointer("MARITAL_STATUS", ms)

To look up the name for a reference value::

        print patients.traverse_pointer("MARITAL_STATUS", ms, fieldnames=['NAME'])[0]

Sub-Files / Multiples
---------------------

Where a field is a "multiple" value, the data is stored in a "sub-file". 

Subfiles are treated as multi-values on the parent file. 

If you do not name fields in your fieldnames variable, only the names
field is returned.

::
    patients = dbs.get_file('PATIENT', fieldnames=['INSURANCE_TYPE',
        "INSURANCE_TYPE->GROUP_PLAN", "INSURANCE_TYPE->COORDINATION_OF_BENEFITS",
        "INSURANCE_TYPE->SUBSCRIBER_ID", "INSURANCE_TYPE->DATE_ENTERED", ])

    print patients.get(2)

If you do not list the fields, the sub-file records are not returned. Only,
the "NAME" field (.01) from the subfile is listed.

::

    patients = dbs.get_file('PATIENT')
    rec = patients.get(2)
    for fieldid, f in enumerate(patients.description):
        if f[0].find('INS') != -1:
            print f[0], rec[fieldid]

Locking
-------

Lock, unlock a record.

::

    import time

    from vavista.fileman import connect
    dbs = connect("0", "")
    patients = dbs.get_file('PATIENT')
    patients.lock(2)
    print 'record 2 is locked'
    time.sleep(60);
    patients.unlock(2)
    print 'record 2 is unlocked'
    time.sleep(60);

GT.M has a lock manager called lke. 

::

    $ lke
    LKE> SHOW -ALL

    DEFAULT
    ^DPT(2) Owned by PID= 9294 which is an existing process


Deleting
--------

*Warning:* the delete logic is rudimentary. I mapped the Fileman call, but I
haven't determined the level of validation, specifically how foreign key
constraints are handled.

::

    from vavista.fileman import connect
    dbs = connect("0", "")
    patients = dbs.get_file('PATIENT')
    patients.delete('1')

    patient = patients.get('1')
    # Throws an exception

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

Cross References / Keys and Identifiers
---------------------------------------

I am very confused here.

*Cross References* are mechanisms for locating records in Fileman. When
I create a new file, it creates a *Traditional* *Regular* cross reference
using an index B.

*Traditional Cross References* are stored on the main file. *New Style 
Cross References* are stored in the *INDEX* file.

*Keys* are identifying *Cross References*, i.e. they are unique.

*Identifiers* are groups of fields which uniquely identify records.

There are *Regular* and *Mumps* cross references.

WIP
---

sub-files, references and back references have to be investigated.

I have to verify that inserts/updates maintain integrity of indexes, audit.

I have to test with non-programming user and understand the security 
infrastructure.

I need index and file iterators, so that I can produce a resultset.

I need functions to create simple tables so that I can build automated
tests.

How to delete records. Seems to be classic api, but no DBS api call.
There also seems to be no interactive option.

There doesn't seem to be an api to create files. You seem to have to
create them interactively, and then dump the globals. 

The description concept is not sufficient for the application. Specifically,
subfiles contain lists of values rather than primitives. Need to use the
data dictionary to drive the description information.
