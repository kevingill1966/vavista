
# Tests for the vpointer type - this allows one field to 
# foreign key to different files.

# TODO: inserts

import unittest
import sys

from vavista.fileman import connect, transaction, FilemanError
from vavista.M import Globals

class TestVPointer(unittest.TestCase):
    """
        Create a simple file containing 2 pointer fields.
        One is created without laygo, the other has laygo
    """

    DIC = [
        ('^DIC(9999920,0)', u'PYTEST10A^9999920'),
        ('^DIC(9999920,0,"AUDIT")', '@'),
        ('^DIC(9999920,0,"DD")', '@'),
        ('^DIC(9999920,0,"DEL")', '@'),
        ('^DIC(9999920,0,"GL")', '^DIZ(9999920,'),
        ('^DIC(9999920,0,"LAYGO")', '@'),
        ('^DIC(9999920,0,"RD")', '@'),
        ('^DIC(9999920,0,"WR")', '@'),
        ('^DIC(9999920,"%A")', '10000000020^3120807'),
        ('^DIC("B","PYTEST10A",9999920)', ''),
        ('^DIC(9999921,0)', u'PYTEST10B^9999921'),
        ('^DIC(9999921,0,"AUDIT")', '@'),
        ('^DIC(9999921,0,"DD")', '@'),
        ('^DIC(9999921,0,"DEL")', '@'),
        ('^DIC(9999921,0,"GL")', '^DIZ(9999921,'),
        ('^DIC(9999921,0,"LAYGO")', '@'),
        ('^DIC(9999921,0,"RD")', '@'),
        ('^DIC(9999921,0,"WR")', '@'),
        ('^DIC(9999921,"%A")', '10000000020^3120807'),
        ('^DIC("B","PYTEST10B",9999921)', ''),
        ('^DIC(9999923,0)', u'PYTEST10C^9999923'),
        ('^DIC(9999923,0,"AUDIT")', '@'),
        ('^DIC(9999923,0,"DD")', '@'),
        ('^DIC(9999923,0,"DEL")', '@'),
        ('^DIC(9999923,0,"GL")', '^DIZ(9999923,'),
        ('^DIC(9999923,0,"LAYGO")', '@'),
        ('^DIC(9999923,0,"RD")', '@'),
        ('^DIC(9999923,0,"WR")', '@'),
        ('^DIC(9999923,"%A")', '10000000020^3120807'),
        ('^DIC("B","PYTEST10C",9999923)', ''),
    ]

    DIZ = [
        ('^DIZ(9999920,0)', 'PYTEST10A^9999920^3^3'),
        ('^DIZ(9999920,1,0)', 'ONE^1'),
        ('^DIZ(9999920,2,0)', 'TWO^2'),
        ('^DIZ(9999920,3,0)', 'THREE^3'),
        ('^DIZ(9999920,"B","ONE",1)', ''),
        ('^DIZ(9999920,"B","THREE",3)', ''),
        ('^DIZ(9999920,"B","TWO",2)', ''),
        ('^DIZ(9999921,0)', 'PYTEST10B^9999921^3^3'),
        ('^DIZ(9999921,1,0)', 'TEN^10'),
        ('^DIZ(9999921,2,0)', 'ELEVEN^11'),
        ('^DIZ(9999921,3,0)', 'TWELVE^12'),
        ('^DIZ(9999921,"B","ELEVEN",2)', ''),
        ('^DIZ(9999921,"B","TEN",1)', ''),
        ('^DIZ(9999921,"B","TWELVE",3)', ''),
        ('^DIZ(9999923,0)', 'PYTEST10C^9999923^2^2'),
        ('^DIZ(9999923,1,0)', 'ONE^1;DIZ(9999920,'),
        ('^DIZ(9999923,2,0)', 'TEN^1;DIZ(9999921,'),
        ('^DIZ(9999923,"B","ONE",1)', ''),
        ('^DIZ(9999923,"B","TEN",2)', ''),
    ]

    DD = [
        ('^DD(9999920,0)', u'FIELD^^1^2'),
        ('^DD(9999920,0,"DT")', '3120807'),
        ('^DD(9999920,0,"IX","B",9999920,.01)', ''),
        ('^DD(9999920,0,"NM","PYTEST10A")', ''),
        ('^DD(9999920,0,"PT",9999923,1)', ''),
        ('^DD(9999920,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X"),
        ('^DD(9999920,.01,1,0)', '^.1'),
        ('^DD(9999920,.01,1,1,0)', '9999920^B'),
        ('^DD(9999920,.01,1,1,1)', 'S ^DIZ(9999920,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999920,.01,1,1,2)', 'K ^DIZ(9999920,"B",$E(X,1,30),DA)'),
        ('^DD(9999920,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999920,1,0)', 'VALUE^NJ3,0^^0;2^K:+X\'=X!(X>100)!(X<0)!(X?.E1"."1N.N) X'),
        ('^DD(9999920,1,3)', 'Type a Number between 0 and 100, 0 Decimal Digits'),
        ('^DD(9999920,1,"DT")', '3120807'),
        ('^DD(9999920,"B","NAME",.01)', ''),
        ('^DD(9999920,"B","VALUE",1)', ''),
        ('^DD(9999920,"GL",0,1,.01)', ''),
        ('^DD(9999920,"GL",0,2,1)', ''),
        ('^DD(9999920,"IX",.01)', ''),
        ('^DD(9999920,"RQ",.01)', ''),
        ('^DD(9999921,0)', u'FIELD^^1^2'),
        ('^DD(9999921,0,"DT")', '3120807'),
        ('^DD(9999921,0,"IX","B",9999921,.01)', ''),
        ('^DD(9999921,0,"NM","PYTEST10B")', ''),
        ('^DD(9999921,0,"PT",9999923,1)', ''),
        ('^DD(9999921,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X"),
        ('^DD(9999921,.01,1,0)', '^.1'),
        ('^DD(9999921,.01,1,1,0)', '9999921^B'),
        ('^DD(9999921,.01,1,1,1)', 'S ^DIZ(9999921,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999921,.01,1,1,2)', 'K ^DIZ(9999921,"B",$E(X,1,30),DA)'),
        ('^DD(9999921,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999921,1,0)', 'VALUE^NJ3,0^^0;2^K:+X\'=X!(X>100)!(X<0)!(X?.E1"."1N.N) X'),
        ('^DD(9999921,1,3)', 'Type a Number between 0 and 100, 0 Decimal Digits'),
        ('^DD(9999921,1,"DT")', '3120807'),
        ('^DD(9999921,"B","NAME",.01)', ''),
        ('^DD(9999921,"B","VALUE",1)', ''),
        ('^DD(9999921,"GL",0,1,.01)', ''),
        ('^DD(9999921,"GL",0,2,1)', ''),
        ('^DD(9999921,"IX",.01)', ''),
        ('^DD(9999921,"RQ",.01)', ''),
        ('^DD(9999923,0)', u'FIELD^^1^2'),
        ('^DD(9999923,0,"DT")', '3120807'),
        ('^DD(9999923,0,"IX","B",9999923,.01)', ''),
        ('^DD(9999923,0,"NM","PYTEST10C")', ''),
        ('^DD(9999923,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X"),
        ('^DD(9999923,.01,1,0)', '^.1'),
        ('^DD(9999923,.01,1,1,0)', '9999923^B'),
        ('^DD(9999923,.01,1,1,1)', 'S ^DIZ(9999923,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999923,.01,1,1,2)', 'K ^DIZ(9999923,"B",$E(X,1,30),DA)'),
        ('^DD(9999923,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999923,1,0)', 'VP1^V^^0;2^Q'),
        ('^DD(9999923,1,"DT")', '3120807'),
        ('^DD(9999923,1,"V",0)', '^.12P^2^2'),
        ('^DD(9999923,1,"V",1,0)', '9999920^THIS IS VP1^1^VP1^n^n'),
        ('^DD(9999923,1,"V",2,0)', '9999921^THIS IS VP2^2^VP2^n^y'),
        ('^DD(9999923,1,"V","B",9999920,1)', ''),
        ('^DD(9999923,1,"V","B",9999921,2)', ''),
        ('^DD(9999923,1,"V","M","THIS IS VP1",1)', ''),
        ('^DD(9999923,1,"V","M","THIS IS VP2",2)', ''),
        ('^DD(9999923,1,"V","O",1,1)', ''),
        ('^DD(9999923,1,"V","O",2,2)', ''),
        ('^DD(9999923,1,"V","P","VP1",1)', ''),
        ('^DD(9999923,1,"V","P","VP2",2)', ''),
        ('^DD(9999923,"B","NAME",.01)', ''),
        ('^DD(9999923,"B","VP1",1)', ''),
        ('^DD(9999923,"GL",0,1,.01)', ''),
        ('^DD(9999923,"GL",0,2,1)', ''),
        ('^DD(9999923,"IX",.01)', ''),
        ('^DD(9999923,"RQ",.01)', ''),
    ]

    IX = [
    ]

    def _createFile(self):
        # This creates a file
        transaction.begin()
        for filename in ["PYTEST10A", "PYTEST10B", "PYTEST10C"]:
            g = Globals["^DIC"]["B"][filename]
            if len(g.keys()) != 0:
                sys.stderr.write("File already exists: %s\n" % filename)
                sys.exit(1)
        Globals.deserialise(self.DIC)
        Globals.deserialise(self.DD)
        Globals.deserialise(self.DIZ)
        Globals.deserialise(self.IX)
        transaction.commit()

    def _cleanupFile(self):
        # This deletes a file
        transaction.begin()
        for fileid, filename in [ ("9999920","PYTEST10A"), ("9999921","PYTEST10B"), ("9999923","PYTEST10C")]:
            Globals["^DIC"][fileid].kill()
            Globals["^DIC"]["B"][filename].kill()
            Globals["^DD"][fileid].kill()
            Globals["^DIZ"][fileid].kill()
        transaction.commit()

    def setUp(self):
        self.dbs = connect("0", "")

        self._cleanupFile()
        self._createFile()

    def tearDown(self):
        # destroy the file
        if transaction.in_transaction:
            transaction.abort()
        self._cleanupFile()

    def test_external(self):
        pytest = self.dbs.get_file("PYTEST10C", internal=False)

        # The low-level traverser, walks index "B", on NAME field
        cursor = pytest.traverser("B", " ")
        key, rowid = cursor.next()

        # retrieve the record
        rec = pytest.get(rowid)

        # validate the inserted data
        self.assertEqual(str(rec.NAME), "ONE")
        self.assertEqual(str(rec.VP1), "ONE")

        key, rowid = cursor.next()

        # retrieve the record
        rec = pytest.get(rowid)

        # validate the inserted data
        self.assertEqual(str(rec.NAME), "TEN")
        self.assertEqual(str(rec.VP1), "TEN")


    def test_internal(self):
        pytest = self.dbs.get_file("PYTEST10C", internal=True)

        # The low-level traverser, walks index "B", on NAME field
        cursor = pytest.traverser("B", " ")

        key, rowid = cursor.next()
        rec = pytest.get(rowid)

        # VPointer perfixes the remote file id to the value.
        self.assertEqual(str(rec.NAME), "ONE")
        self.assertEqual(str(rec.VP1), "VP1.1")

        key, rowid = cursor.next()
        rec = pytest.get(rowid)

        self.assertEqual(str(rec.NAME), "TEN")
        self.assertEqual(str(rec.VP1), "VP2.1")

    def test_traverse(self):
        pytest = self.dbs.get_file("PYTEST10C", internal=True)

        # The low-level traverser, walks index "B", on NAME field
        cursor = pytest.traverser("B", " ")

        key, rowid = cursor.next()
        rec = pytest.get(rowid)

        self.assertEqual(str(rec.NAME), "ONE")
        self.assertEqual(str(rec.VP1), "VP1.1")

        # Traverse
        reference = rec.traverse("VP1")
        self.assertEqual(str(reference.NAME), "ONE")
        self.assertEqual(str(reference.VALUE), "1")

        key, rowid = cursor.next()
        rec = pytest.get(rowid)

        self.assertEqual(str(rec.NAME), "TEN")
        self.assertEqual(str(rec.VP1), "VP2.1")

        reference = rec.traverse("VP1")
        self.assertEqual(str(reference.NAME), "TEN")
        self.assertEqual(str(reference.VALUE), "10")

    def test_insert(self):

        pytest = self.dbs.get_file("PYTEST10C", internal=True)

        transaction.begin()
        rec = pytest.new()
        rec.NAME = "TEST INSERT"
        rec.VP1 = "VP1.2"
        transaction.commit()

        cursor = pytest.traverser("B", "TEST INSERT")
        key, rowid = cursor.next()
        rec = pytest.get(rowid)

        self.assertEqual(str(rec.NAME), "TEST INSERT")
        self.assertEqual(str(rec.VP1), "VP1.2")

        reference = rec.traverse("VP1")
        self.assertEqual(str(reference.NAME), "TWO")
        self.assertEqual(str(reference.VALUE), "2")

    def test_badinsert(self):
        """
            Should fail to insert if foreign key non-existant
        """
        pytest = self.dbs.get_file("PYTEST10C", internal=True)

        transaction.begin()
        exception = False
        try:
            rec = pytest.new()
            rec.NAME = "TEST INSERT"
            rec.VP1 = "VP1.20"
            transaction.commit()
        except Exception, e:
            transaction.abort()
            exception = e
        self.assertNotEqual(exception, None)

test_cases = (TestVPointer, )

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
