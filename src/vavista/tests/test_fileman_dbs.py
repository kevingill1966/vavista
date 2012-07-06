
# Test the Fileman DBS interface

import unittest

from vavista.fileman import connect

class TestFileman(unittest.TestCase):

    def setUp(self):
        self.dbs = connect("0", "")

    def tearDown(self):
        pass

    def test_fileid(self):
        file = self.dbs.get_file("FILE")
        self.assertEqual(file.dd.fileid, "1")
        self.assertEqual(file.get("1")[".01"].value, "FILE")
        self.assertEqual(file.get("1").NAME.value, "FILE")

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

    def test_fileget(self):
        file = self.dbs.get_file("OPTION")
        print file.get("5159")


test_cases = (TestFileman,)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
