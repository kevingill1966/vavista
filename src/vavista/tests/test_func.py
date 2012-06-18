# vavista.M.func provides a mechanism to invoke a Mumps function
# with a set of parameters from Python code.

import unittest

from vavista.M import mexec, func, INOUT, REF

class TestFunc(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_intrinsic(self):
        ## "intrinsic" functions are those built into the language
        ## they must be preceded with a "$"
        rv = func("$A", "Beethoven")
        self.assertEqual(rv[0], "66")

    def test_extrinsic(self):
        ## "extrinisc" functions are those defined in the application.
        ## they must be preceded with a "$$"
        rv = func("$$testfunc^vavistagtm", INOUT(""), INOUT(0), INOUT(0.0))
        self.assertEqual(rv[0], "99")
        self.assertEqual(rv[1], "testfunc")
        self.assertEqual(rv[2], 3333)
        self.assertEqual(rv[3], 444.44)

    def test_ref(self):
        ## Ensure that a reference will be followed
        mexec('set MYVAR="HAYDEN"')
        rv = func("$A", REF("MYVAR"), 3)
        self.assertEqual(rv[0], "89")

test_cases = (TestFunc,)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
