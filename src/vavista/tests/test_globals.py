
#  Have to set up real test - just hacking the vista data here

import unittest

from vavista.M import Globals

class TestGlobals(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_keys(self):
        g = Globals()
        keys = g.keys()

    def test_value(self):
        g = Globals()
        v = g["^DD"][1][0].value
        self.assertEqual(v, 'ATTRIBUTE^N^^22')

test_cases = (TestGlobals,)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
