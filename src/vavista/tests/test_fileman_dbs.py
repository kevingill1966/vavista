
# Test the Fileman DBS interface

import unittest

from vavista.fileman import connect, transaction

class TestFileman(unittest.TestCase):

    def setUp(self):
        self.dbs = connect("0", "")

    def tearDown(self):
        pass

    def test_fileid(self):
        file = self.dbs.get_file("FILE")
        self.assertEqual(file.dd.fileid, "1")
        self.assertEqual(file.get("1")[".01"], "FILE")
        self.assertEqual(file.get("1").NAME, "FILE")

    def test_list_files(self):
        rv = self.dbs.list_files()
        #TODO: Check a value

    def test_dd(self):
        """
            I created this file via Fileman to test
        """
        dd = self.dbs.dd("KEVIN1")
        self.assertEqual(dd.fileid, "999900")
        print '\n'
        print dd
        print self.dbs.dd("FILE")
        print self.dbs.dd("LOCATION")
        print self.dbs.dd("LOCATION").indices
        print self.dbs.dd("LOCATION").new_indices

    def test_fileget(self):
        file = self.dbs.get_file("OPTION")
        print file.get("5159")

    def test_insert(self):
        # Create a new record - write it to the database
        kevin1 = self.dbs.get_file("KEVIN1")
        record = kevin1.new()
        record.NAME = 'hello there from unit test'
        transaction.commit()

        # There is no rowid until after the commit
        # read the record back and make sure it is the same
        rowid = record._rowid
        copy = kevin1.get(rowid)
        self.assertEqual(str(copy.NAME), 'hello there from unit test')

        # update the record and verify that the updates worked.
        record.NAME = 'hello there from unit test2'
        transaction.commit()
        copy = kevin1.get(rowid)
        self.assertEqual(str(copy.NAME), 'hello there from unit test2')

    def test_insert_external(self):
        # Create a new record - write it to the database
        kevin1 = self.dbs.get_file("KEVIN1", internal=False)
        record = kevin1.new()
        record.NAME = 'hello there from unit test'
        transaction.commit()

        # There is no rowid until after the commit
        # read the record back and make sure it is the same
        rowid = record._rowid
        copy = kevin1.get(rowid)
        self.assertEqual(str(copy.NAME), 'hello there from unit test')

        # update the record and verify that the updates worked.
        record.NAME = 'hello there from unit test2'
        transaction.commit()
        copy = kevin1.get(rowid)
        self.assertEqual(str(copy.NAME), 'hello there from unit test2')

        # delete
        copy.delete()

        # Verify that the copy row no longer exists.
        missing = False
        try:
            kevin1.get(rowid)
        except:
            missing = True
        self.assertEqual(missing, True)


test_cases = (TestFileman, )

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
