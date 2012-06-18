# vavista.M.proc provides a mechanism to invoke a Mumps procedure
# with a set of parameters from Python code.

import unittest

from vavista.M import mexec, proc, INOUT, REF

class TestProc(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_testproc(self):
        rv = proc("testproc^vavistagtm", INOUT(""), INOUT(0), INOUT(0.0))
        self.assertEqual(rv[0], "testproc")
        self.assertEqual(rv[1], 1111)
        self.assertEqual(rv[2], 222.22)

    def test_ref(self):
        mexec('set MYVAR="derefme"')
        rv = proc("testref^vavistagtm", INOUT(""), REF("MYVAR"))
        self.assertEqual(rv[0], "derefme")

test_cases = (TestProc,)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
