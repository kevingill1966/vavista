

# Test the Fileman DBS interface

import unittest

from vavista.fileman import connect, transaction, FilemanError
from vavista.M import Globals

class TestNumerics(unittest.TestCase):
    """
        Create a simple file containing a couple of numeric values.
    """
    DIC = [
        ('^DIC(9999905,0)', 'PYTEST3^9999905'),
        ('^DIC(9999905,0,"AUDIT")', '@'),
        ('^DIC(9999905,0,"DD")', '@'),
        ('^DIC(9999905,0,"DEL")', '@'),
        ('^DIC(9999905,0,"GL")', '^DIZ(9999905,'),
        ('^DIC(9999905,0,"LAYGO")', '@'),
        ('^DIC(9999905,0,"RD")', '@'),
        ('^DIC(9999905,0,"WR")', '@'),
        ('^DIC(9999905,"%A")', '10000000020^3120720'),
        ('^DIC("B","PYTEST3",9999905)', ''),
    ]

    DIZ = [
        ('^DIZ(9999905,0)', 'PYTEST3^9999905^1^1'),
        ('^DIZ(9999905,1,0)', 'entered via fileman^10^-10^33.33^22222.22^-333'),
        ('^DIZ(9999905,"B","entered via fileman",1)', ''),
    ]

    DD = [
        ('^DD(9999905,0)', 'FIELD^^5^6'),
        ('^DD(9999905,0,"DT")', '3120720'),
        ('^DD(9999905,0,"IX","B",9999905,.01)', ''),
        ('^DD(9999905,0,"NM","PYTEST3")', ''),
        ('^DD(9999905,.01,0)', '''NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X'''),
        ('^DD(9999905,.01,1,0)', '^.1'),
        ('^DD(9999905,.01,1,1,0)', '9999905^B'),
        ('^DD(9999905,.01,1,1,1)', 'S ^DIZ(9999905,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999905,.01,1,1,2)', 'K ^DIZ(9999905,"B",$E(X,1,30),DA)'),
        ('^DD(9999905,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999905,1,0)', '''int1^NJ6,0^^0;2^K:+X'=X!(X>999999)!(X<0)!(X?.E1"."1N.N) X'''),
        ('^DD(9999905,1,3)', 'Type a Number between 0 and 999999, 0 Decimal Digits'),
        ('^DD(9999905,1,"DT")', '3120720'),
        ('^DD(9999905,2,0)', '''int2^NJ5,0^^0;3^K:+X'=X!(X>10000)!(X<-10000)!(X?.E1"."1N.N) X'''),
        ('^DD(9999905,2,3)', 'Type a Number between -10000 and 10000, 0 Decimal Digits'),
        ('^DD(9999905,2,"DT")', '3120720'),
        ('^DD(9999905,3,0)', '''dollars^NJ8,2^^0;4^S:X["$" X=$P(X,"$",2) K:X'?.N.1".".2N!(X>10000)!(X<0) X'''),
        ('^DD(9999905,3,3)', 'Type a Dollar Amount between 0 and 10000, 2 Decimal Digits'),
        ('^DD(9999905,3,"DT")', '3120720'),
        ('^DD(9999905,4,0)', '''float1^NJ8,2^^0;5^K:+X'=X!(X>99999)!(X<0)!(X?.E1"."3N.N) X'''),
        ('^DD(9999905,4,3)', 'Type a Number between 0 and 99999, 2 Decimal Digits'),
        ('^DD(9999905,4,"DT")', '3120720'),
        ('^DD(9999905,5,0)', '''float2^NJ9,4^^0;6^K:+X'=X!(X>1000)!(X<-1000)!(X?.E1"."5N.N) X'''),
        ('^DD(9999905,5,3)', 'Type a Number between -1000 and 1000, 4 Decimal Digits'),
        ('^DD(9999905,5,"DT")', '3120720'),
        ('^DD(9999905,"B","NAME",.01)', ''),
        ('^DD(9999905,"B","dollars",3)', ''),
        ('^DD(9999905,"B","float1",4)', ''),
        ('^DD(9999905,"B","float2",5)', ''),
        ('^DD(9999905,"B","int1",1)', ''),
        ('^DD(9999905,"B","int2",2)', ''),
        ('^DD(9999905,"GL",0,1,.01)', ''),
        ('^DD(9999905,"GL",0,2,1)', ''),
        ('^DD(9999905,"GL",0,3,2)', ''),
        ('^DD(9999905,"GL",0,4,3)', ''),
        ('^DD(9999905,"GL",0,5,4)', ''),
        ('^DD(9999905,"GL",0,6,5)', ''),
        ('^DD(9999905,"IX",.01)', ''),
        ('^DD(9999905,"RQ",.01)', ''),
    ]

    IX = [
    ]

    def _cleanupFile(self):
        transaction.begin()
        Globals["^DIC"]["9999905"].kill()
        Globals["^DIC"]['B']["PYTEST3"].kill()
        Globals["^DD"]["9999905"].kill()
        Globals["^DIZ"]["9999905"].kill()
        transaction.commit()

    def _createFile(self):
        # This creates a file
        transaction.begin()
        Globals.deserialise(self.DIC)
        Globals.deserialise(self.DD)
        Globals.deserialise(self.DIZ)
        #Globals.deserialise(self.IX)
        transaction.commit()

        # Are indices setup
        dd = self.dbs.dd("PYTEST3")
        self.assertEqual(dd.fileid, "9999905")
        self.assertEqual(len(dd.indices), 1)
        #self.assertEqual(len(dd.new_indices), 1)

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
        pytest3 = self.dbs.get_file("PYTEST3", internal=True)
        cursor = pytest3.traverser("B", "e")
        rec = cursor.next()

        self.assertEqual(rec.INT1, 10)
        self.assertEqual(rec.INT2, -10)
        self.assertEqual(rec.DOLLARS, 33.33)
        self.assertEqual(rec.FLOAT1, 22222.22)
        self.assertEqual(rec.FLOAT2, -333.0)

        pytest3 = self.dbs.get_file("PYTEST3", internal=False)
        cursor = pytest3.traverser("B", "e")
        rec = cursor.next()

        self.assertEqual(rec.INT1, 10)
        self.assertEqual(rec.INT2, -10)
        self.assertEqual(rec.DOLLARS, 33.33)
        self.assertEqual(rec.FLOAT1, 22222.22)
        self.assertEqual(rec.FLOAT2, -333.0)

    def test_write(self):
        pytest3 = self.dbs.get_file("PYTEST3")
        transaction.begin()
        rec = pytest3.new()
        rec.NAME = "Insert"
        rec.INT1 = 11
        rec.INT2 = -11
        rec.DOLLARS = 44.44
        rec.FLOAT1 = 3333.33
        rec.FLOAT2 = -444.0
        transaction.commit()

        cursor = pytest3.traverser("B", "Insert")
        rec = cursor.next()

        self.assertEqual(rec.INT1, 11)
        self.assertEqual(rec.INT2, -11)
        self.assertEqual(rec.DOLLARS, 44.44)
        self.assertEqual(rec.FLOAT1, 3333.33)
        self.assertEqual(rec.FLOAT2, -444.0)

    def test_badwrite(self):
        pytest3 = self.dbs.get_file("PYTEST3")

        transaction.begin()
        rec = pytest3.new()
        e = None
        try:
            rec.NAME = "bad Insert1"
            rec.INT1 = 1.1
            transaction.commit()
        except FilemanError, e1:
            transaction.abort()
            e = e1

        # rounding errors ignored
        self.assertEquals(e, None)

        transaction.begin()
        rec = pytest3.new()
        e = None
        try:
            rec.NAME = "bad Insert2"
            rec.DOLLARS = 44.4499
            transaction.commit()
        except FilemanError, e1:
            transaction.abort()
            e = e1

        # rounding errors ignored
        self.assertEquals(e, None)

        transaction.begin()
        rec = pytest3.new()
        e = None
        try:
            rec.NAME = "bad Insert3"
            rec.FLOAT1 = "abc"
            transaction.commit()
        except FilemanError, e1:
            transaction.abort()
            e = e1
        self.assertTrue(isinstance(e, FilemanError))

    def test_indexing(self):
        """
            TODO: walk a numeric index. Can I return the values as ints / floats.
        """

test_cases = (TestNumerics,)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
