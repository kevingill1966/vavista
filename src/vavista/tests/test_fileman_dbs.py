
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
        rv = self.dbs.fileid("FILE")
        self.assertEqual(rv, "1")

    def test_list_files(self):
        rv = self.dbs.list_files()
        #TODO: Check a value

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
