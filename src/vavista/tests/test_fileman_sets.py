

# Test the Fileman DBS interface

import unittest

from vavista.fileman import connect, transaction, FilemanError
from vavista.M import Globals

class TestSets(unittest.TestCase):
    """
        Create a simple file containing a couple of sets.
    """

    DIC = [
        ('^DIC(9999906,0)', u'PYTEST4^9999906'),
        ('^DIC(9999906,0,"AUDIT")', '@'),
        ('^DIC(9999906,0,"DD")', '@'),
        ('^DIC(9999906,0,"DEL")', '@'),
        ('^DIC(9999906,0,"GL")', '^DIZ(9999906,'),
        ('^DIC(9999906,0,"LAYGO")', '@'),
        ('^DIC(9999906,0,"RD")', '@'),
        ('^DIC(9999906,0,"WR")', '@'),
        ('^DIC(9999906,"%A")', '10000000020^3120720'),
        ('^DIC("B","PYTEST4",9999906)', ''),
    ]

    DIZ = [
        ('^DIZ(9999906,0)', 'PYTEST4^9999906^1^1'),
        ('^DIZ(9999906,1,0)', 'entered via fileman^Y^M^8'),
        ('^DIZ(9999906,"B","entered via fileman",1)', ''),
    ]

    DD = [
        ('^DD(9999906,0)', u'FIELD^^3^4'),
        ('^DD(9999906,0,"DT")', '3120720'),
        ('^DD(9999906,0,"IX","B",9999906,.01)', ''),
        ('^DD(9999906,0,"NM","PYTEST4")', ''),
        ('^DD(9999906,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X"),
        ('^DD(9999906,.01,1,0)', '^.1'),
        ('^DD(9999906,.01,1,1,0)', '9999906^B'),
        ('^DD(9999906,.01,1,1,1)', 'S ^DIZ(9999906,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999906,.01,1,1,2)', 'K ^DIZ(9999906,"B",$E(X,1,30),DA)'),
        ('^DD(9999906,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999906,1,0)', 'yesno^S^Y:YES;N:NO;^0;2^Q'),
        ('^DD(9999906,1,"DT")', '3120720'),
        ('^DD(9999906,2,0)', 'gender^S^M:MALE;F:FEMALE;^0;3^Q'),
        ('^DD(9999906,2,"DT")', '3120720'),
        ('^DD(9999906,3,0)', 'numbers^S^1:ONE;2:TWO;3:THREE;4:FOUR;5:FIVE;6:SIX;7:SEVEN;8:EIGHT;9:NINE;^0;4^Q'),
        ('^DD(9999906,3,"DT")', '3120720'),
        ('^DD(9999906,"B","NAME",.01)', ''),
        ('^DD(9999906,"B","gender",2)', ''),
        ('^DD(9999906,"B","numbers",3)', ''),
        ('^DD(9999906,"B","yesno",1)', ''),
        ('^DD(9999906,"GL",0,1,.01)', ''),
        ('^DD(9999906,"GL",0,2,1)', ''),
        ('^DD(9999906,"GL",0,3,2)', ''),
        ('^DD(9999906,"GL",0,4,3)', ''),
        ('^DD(9999906,"IX",.01)', ''),
        ('^DD(9999906,"RQ",.01)', ''),
    ]

    IX = [
    ]


    def _createFile(self):
        # This creates a file
        transaction.begin()
        Globals.deserialise(self.DIC)
        Globals.deserialise(self.DD)
        Globals.deserialise(self.DIZ)
        Globals.deserialise(self.IX)
        transaction.commit()

        # Are indices setup
        dd = self.dbs.dd("PYTEST4")
        self.assertEqual(dd.fileid, "9999906")
        self.assertEqual(len(dd.indices), 1)
        #self.assertEqual(len(dd.new_indices), 1)

    def _cleanupFile(self):
        # This deletes a file
        transaction.begin()
        Globals["^DIC"]["9999906"].kill()
        Globals["^DIC"]["B"]["PYTEST4"].kill()
        Globals["^DD"]["9999906"].kill()
        Globals["^DIZ"]["9999906"].kill()
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

    def test_read(self):
        """
            Validate the data which was in the file. This data
            was created via FILEMAN.
        """
        pytest4 = self.dbs.get_file("PYTEST4", internal=True)
        cursor = pytest4.traverser("B", "e")
        key, rowid = cursor.next()
        rec = pytest4.get(rowid)

        self.assertEqual(rec.GENDER, "M")
        self.assertEqual(rec.NUMBERS, "8")
        self.assertEqual(rec.YESNO, "Y")

        pytest4 = self.dbs.get_file("PYTEST4", internal=False)
        cursor = pytest4.traverser("B", "e")
        key, rowid = cursor.next()
        rec = pytest4.get(rowid)

        self.assertEqual(rec.GENDER, "MALE")
        self.assertEqual(rec.NUMBERS, "EIGHT")
        self.assertEqual(rec.YESNO, "YES")

    def test_write(self):
        pytest4 = self.dbs.get_file("PYTEST4", internal=True)
        transaction.begin()
        rec = pytest4.new()
        rec.NAME = "Insert Internal"
        rec.GENDER = "F"
        rec.NUMBERS = "4"
        rec.YESNO = "N"
        transaction.commit()

        cursor = pytest4.traverser("B", "Insert Internal")
        key, rowid = cursor.next()
        rec = pytest4.get(rowid)

        self.assertEqual(rec.GENDER, "F")
        self.assertEqual(rec.NUMBERS, "4")
        self.assertEqual(rec.YESNO, "N")

        # Check validation
        transaction.begin()
        try:
            rec.GENDER = "UNKNOWN"
            transaction.commit()
        except FilemanError, e:
            transaction.abort()

        rec = pytest4.get(rowid)
        self.assertEqual(rec.GENDER, "F")

    def test_indexing(self):
        """
            TODO: walk a date index. Can I return the values as ints / floats.
        """

test_cases = (TestSets,)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
