
import sys
import unittest

from vavista.fileman import connect, transaction, FilemanError
from vavista.M import Globals

class TestMumps(unittest.TestCase):
    """
        Create a simple file containing a computed field.
        Verify that the read and write functionality works.
    """

    filename = 'PYTEST8'

    DIC = [
        ('^DIC(9999914,0)', u'PYTEST8^9999914'),
        ('^DIC(9999914,0,"AUDIT")', '@'),
        ('^DIC(9999914,0,"DD")', '@'),
        ('^DIC(9999914,0,"DEL")', '@'),
        ('^DIC(9999914,0,"GL")', '^DIZ(9999914,'),
        ('^DIC(9999914,0,"LAYGO")', '@'),
        ('^DIC(9999914,0,"RD")', '@'),
        ('^DIC(9999914,0,"WR")', '@'),
        ('^DIC(9999914,"%A")', '10000000020^3120806'),
        ('^DIC("B","PYTEST8",9999914)', ''),
    ]

    DIZ = [
        ('^DIZ(9999914,0)', 'PYTEST8^9999914^0^0'),
    ]

    DD = [
        ('^DD(9999914,0)', u'FIELD^^1^2'),
        ('^DD(9999914,0,"DT")', '3120806'),
        ('^DD(9999914,0,"IX","B",9999914,.01)', ''),
        ('^DD(9999914,0,"NM","PYTEST8")', ''),
        ('^DD(9999914,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X"),
        ('^DD(9999914,.01,1,0)', '^.1'),
        ('^DD(9999914,.01,1,1,0)', '9999914^B'),
        ('^DD(9999914,.01,1,1,1)', 'S ^DIZ(9999914,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999914,.01,1,1,2)', 'K ^DIZ(9999914,"B",$E(X,1,30),DA)'),
        ('^DD(9999914,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999914,1,0)', 'm1^K^^1;E1,245^K:$L(X)>245 X D:$D(X) ^DIM'),
        ('^DD(9999914,1,3)', 'This is Standard MUMPS code.'),
        ('^DD(9999914,1,9)', '@'),
        ('^DD(9999914,1,"DT")', '3120806'),
        ('^DD(9999914,"B","NAME",.01)', ''),
        ('^DD(9999914,"B","m1",1)', ''),
        ('^DD(9999914,"GL",0,1,.01)', ''),
        ('^DD(9999914,"GL",1,"E1,245",1)', ''),
        ('^DD(9999914,"IX",.01)', ''),
        ('^DD(9999914,"RQ",.01)', ''),
    ]

    IX = [
    ]


    def _createFile(self):
        # This creates a file
        g = Globals["^DIC"]["B"][self.filename]
        if len(g.keys()) != 0:
            sys.stderr.write("File already exists: %s\n" % self.filename)
            sys.exit(1)
        Globals.deserialise(self.DIC)
        Globals.deserialise(self.DD)
        Globals.deserialise(self.DIZ)
        Globals.deserialise(self.IX)

        dd = self.dbs.dd("PYTEST8")
        self.assertEqual(dd.fileid, "9999914")

    def _cleanupFile(self):
        # This deletes a file
        Globals["^DIC"]["9999914"].kill()
        Globals["^DIC"]["B"]["PYTEST8"].kill()
        Globals["^DD"]["9999914"].kill()
        Globals["^DIZ"]["9999914"].kill()


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
        pytest = self.dbs.get_file("PYTEST8")
        transaction.begin()
        record = pytest.new()
        record.NAME = 'Test Insert'
        record.M1 = 'SET X=4,Y=7'
        transaction.commit()

        # The low-level traverser, walks index "B", on NAME field
        cursor = pytest.traverser("B", "Test")
        rec = cursor.next()

        # validate the inserted data
        self.assertEqual(str(rec.NAME), "Test Insert")
        self.assertEqual(str(rec.M1), "SET X=4,Y=7")

        # Attempt to update the field
        e = None
        transaction.begin()
        try:
            rec.M1 = "Invalid Code"
            transaction.commit()
        except FilemanError, e1:
            transaction.abort()
            e = e1
        self.assertTrue(isinstance(e, FilemanError))

test_cases = (TestMumps, )

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
