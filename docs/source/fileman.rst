
fileman (api for working with fileman files)
============================================

This is currently under development. 

DBS
---

VistA provides two librarys for accessing Fileman. This implementation is based
on DBS, which does not involve user input.

To import::

    from vavista.fileman.dbs import DBS

Create a DBS object. This takes two parameters, the user id (DUZ) variable and
DT (the date/time format string). These are passed to the Mumps interpreter.::

    dbs = DBS("0", "")

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

    from vavista.fileman.dbs import DBS
    dbs = DBS("0", "")
    patients = dbs.get_file('PATIENT')
    patient1 = patients.get('1')

