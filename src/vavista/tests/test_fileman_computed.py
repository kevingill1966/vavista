
import sys
import unittest

from vavista.fileman import connect, transaction, FilemanError
from vavista.M import Globals

class TestComputed(unittest.TestCase):
    """
        Create a simple file containing a computed field.
        Verify that the read and write functionality works.
    """

    filename = 'PYTEST7'

    DIC = [
        ('^DIC(999912,0)', u'PYTEST7^999912'),
        ('^DIC(999912,0,"AUDIT")', '@'),
        ('^DIC(999912,0,"DD")', '@'),
        ('^DIC(999912,0,"DEL")', '@'),
        ('^DIC(999912,0,"GL")', '^DIZ(999912,'),
        ('^DIC(999912,0,"LAYGO")', '@'),
        ('^DIC(999912,0,"RD")', '@'),
        ('^DIC(999912,0,"WR")', '@'),
        ('^DIC(999912,"%A")', '10000000020^3120806'),
        ('^DIC("B","PYTEST7",999912)', ''),
    ]

    DIZ = [
        ('^DIZ(999912,0)', 'PYTEST7^999912^'),
    ]

    DD = [
        ('^DD(999912,0)', u'FIELD^^1^2'),
        ('^DD(999912,0,"IX","B",999912,.01)', ''),
        ('^DD(999912,0,"NM","PYTEST7")', ''),
        ('^DD(999912,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X"),
        ('^DD(999912,.01,1,0)', '^.1'),
        ('^DD(999912,.01,1,1,0)', '999912^B'),
        ('^DD(999912,.01,1,1,1)', 'S ^DIZ(999912,"B",$E(X,1,30),DA)=""'),
        ('^DD(999912,.01,1,1,2)', 'K ^DIZ(999912,"B",$E(X,1,30),DA)'),
        ('^DD(999912,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(999912,1,0)', 'c1^CJ8^^ ; ^S X=100 S:X<0 X=-X'),
        ('^DD(999912,1,9)', '^'),
        ('^DD(999912,1,9.01)', ''),
        ('^DD(999912,1,9.1)', 'ABS(100)'),
        ('^DD(999912,"B","NAME",.01)', ''),
        ('^DD(999912,"B","c1",1)', ''),
        ('^DD(999912,"GL",0,1,.01)', ''),
        ('^DD(999912,"IX",.01)', ''),
        ('^DD(999912,"RQ",.01)', ''),
    ]

    IX = [
    ]


    def _createFile(self):
        # This creates a file
        transaction.begin()
        g = Globals["^DIC"]["B"][self.filename]
        if len(g.keys()) != 0:
            sys.stderr.write("File already exists: %s\n" % self.filename)
            sys.exit(1)
        Globals.deserialise(self.DIC)
        Globals.deserialise(self.DD)
        Globals.deserialise(self.DIZ)
        Globals.deserialise(self.IX)
        transaction.commit()

    def _cleanupFile(self):
        # This deletes a file
        transaction.begin()
        Globals["^DIC"]["999912"].kill()
        Globals["^DIC"]["B"][self.filename].kill()
        Globals["^DD"]["999912"].kill()
        Globals["^DIZ"]["999912"].kill()
        transaction.commit()

    def setUp(self):
        self.dbs = connect("0", "")

        self._cleanupFile()
        self._createFile()

    def tearDown(self):
        # destroy the file
        self._cleanupFile()
        if transaction.in_transaction:
            transaction.abort()

    def test_readwrite(self):
        dd = self.dbs.dd("PYTEST7")
        self.assertEqual(dd.fileid, "999912")

        pytest = self.dbs.get_file("PYTEST7")
        transaction.begin()
        rowid = pytest.insert(NAME = 'Test Insert')
        transaction.commit()

        # The low-level traverser, walks index "B", on NAME field
        # ('^DD(9999903,0,"IX","B",9999903,.01)', ''),
        cursor = pytest.traverser("B", "Test")
        rec = cursor.next()

        # validate the inserted data
        self.assertEqual(rec[0], "Test Insert")
        self.assertEqual(rec[1], "100")

        # Attempt to update the field
        e = None
        transaction.begin()
        try:
            pytest.update(rowid, C1="200")
            transaction.commit()
        except FilemanError, e1:
            transaction.abort()
            e = e1
        self.assertTrue(isinstance(e, FilemanError))

test_cases = (TestComputed, )

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
