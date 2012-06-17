# The concept here is to move to a generic calling convention.
# The mumps call will pass the system barrier using a single call
# which takes a command and up to eight strings, ints and doubles

import unittest

from vavista.M import mexec, INOUT

class TestMExec(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_s0(self):
        rv = mexec('set s0="TEST VALUE"', INOUT(""))
        self.assertEqual(rv[0], "TEST VALUE")
        rv = mexec('set s0=2', INOUT(""))
        self.assertEqual(rv[0], "2")
        rv = mexec('set s0=0.2', INOUT(""))
        self.assertEqual(rv[0], ".2")

    def test_d0(self):
        rv = mexec('set d0=d0+0.1', INOUT(1.0))
        self.assertEqual(rv[0], 1.1)
        rv = mexec('set d0="0.2"', INOUT(1.0))
        self.assertEqual(rv[0], 0.2)

    def test_d1(self):
        rv = mexec('set d1=d0+0.1', INOUT(1.0), INOUT(0.0))
        self.assertEqual(rv[1], 1.1)

    def test_l0(self):
        rv = mexec('set l0=l0*10', INOUT(10))
        self.assertEqual(rv[0], 100)

    def test_set_get(self):
        rv = mexec('set donkey=l0,l1=21,l2=donkey+1', 12, 0, INOUT(0))
        self.assertEqual(rv[0], 13)
        rv = mexec('set l0=donkey', INOUT(10))
        self.assertEqual(rv[0], 12)

test_cases = (TestMExec,)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
