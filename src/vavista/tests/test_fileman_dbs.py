
# Test the Fileman DBS interface

import unittest

from vavista.fileman import DBS

class TestDBS(unittest.TestCase):

    def setUp(self):
        self.dbs = DBS("0", "")
        pass

    def tearDown(self):
        pass

    def test_fileid(self):
        file = self.dbs.get_file("FILE")
        self.assertEqual(file.dd.fileid, "1")
        print file.get("1")

    def test_list_files(self):
        rv = self.dbs.list_files()
        #TODO: Check a value

    def test_dd(self):
        """
            I created this file via Fileman to test
        """
        dd = self.dbs.dd("KEVIN1")
        self.assertEqual(dd.fileid, "999900")
        dd.fileid
        f = dd.fields

    def test_fileget(self):
        file = self.dbs.get_file("OPTION")
        print file.get("5159")


test_cases = (TestDBS,)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
