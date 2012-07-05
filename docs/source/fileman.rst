
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
    print patient1

I am working on the update logic. The concept is that you modify an object
and it will write out the data during a transaction commit.

::

    from vavista.fileman import connect, transaction
    transaction.begin()
    dbs = connect("0", "")
    patients = dbs.get_file('PATIENT')
    patient1 = patients.get('1')
    patient1.TYPE = 'NEW VALUE'
    transaction.commit()    # writes out here.

However, not making good progress at the moment.
