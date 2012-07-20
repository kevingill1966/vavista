
# Test the Fileman DBS interface

import unittest

from vavista.fileman import connect, transaction, FilemanError
from vavista.M import Globals

class TestWP(unittest.TestCase):
    """
        Create a simple file containing a couple of wp fields.
    """

    DIC = [
        ('^DIC(9999907,0)', u'PYTEST5^9999907'),
        ('^DIC(9999907,0,"AUDIT")', '@'),
        ('^DIC(9999907,0,"DD")', '@'),
        ('^DIC(9999907,0,"DEL")', '@'),
        ('^DIC(9999907,0,"GL")', '^DIZ(9999907,'),
        ('^DIC(9999907,0,"LAYGO")', '@'),
        ('^DIC(9999907,0,"RD")', '@'),
        ('^DIC(9999907,0,"WR")', '@'),
        ('^DIC(9999907,"%A")', '10000000020^3120720'),
        ('^DIC("B","PYTEST5",9999907)', ''),
    ]

    DIZ = [
        ('^DIZ(9999907,0)', 'PYTEST5^9999907^1^1'),
        ('^DIZ(9999907,1,0)', 'entered via fileman'),
        ('^DIZ(9999907,1,1,0)', '^^6^6^3120720^'),
        ('^DIZ(9999907,1,1,1,0)', 'This is text entered via the line editor.'),
        ('^DIZ(9999907,1,1,2,0)', 'This is another line.'),
        ('^DIZ(9999907,1,1,3,0)', 'This is a long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, l'),
        ('^DIZ(9999907,1,1,4,0)', 'ong, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, '),
        ('^DIZ(9999907,1,1,5,0)', 'long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long,'),
        ('^DIZ(9999907,1,1,6,0)', ' long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long line'),
        ('^DIZ(9999907,1,2,0)', '^^1^1^3120720^'),
        ('^DIZ(9999907,1,2,1,0)', 'wp field 2 - what was that about | '),
        ('^DIZ(9999907,"B","entered via fileman",1)', ''),
    ]

    DD = [
        ('^DD(9999907,0)', u'FIELD^^2^3'),
        ('^DD(9999907,0,"DT")', '3120720'),
        ('^DD(9999907,0,"IX","B",9999907,.01)', ''),
        ('^DD(9999907,0,"NM","PYTEST5")', ''),
        ('^DD(9999907,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X"),
        ('^DD(9999907,.01,1,0)', '^.1'),
        ('^DD(9999907,.01,1,1,0)', '9999907^B'),
        ('^DD(9999907,.01,1,1,1)', 'S ^DIZ(9999907,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999907,.01,1,1,2)', 'K ^DIZ(9999907,"B",$E(X,1,30),DA)'),
        ('^DD(9999907,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999907,1,0)', 'wp1^9999907.01^^1;0'),
        ('^DD(9999907,2,0)', 'wp2^9999907.02^^2;0'),
        ('^DD(9999907,"B","NAME",.01)', ''),
        ('^DD(9999907,"B","wp1",1)', ''),
        ('^DD(9999907,"B","wp2",2)', ''),
        ('^DD(9999907,"GL",0,1,.01)', ''),
        ('^DD(9999907,"GL",1,0,1)', ''),
        ('^DD(9999907,"GL",2,0,2)', ''),
        ('^DD(9999907,"IX",.01)', ''),
        ('^DD(9999907,"RQ",.01)', ''),
        ('^DD(9999907,"SB",9999907.01,1)', ''),
        ('^DD(9999907,"SB",9999907.02,2)', ''),
        ('^DD(9999907.01,0)', u'wp1 SUB-FIELD^^.01^1'),
        ('^DD(9999907.01,0,"DT")', '3120720'),
        ('^DD(9999907.01,0,"NM","wp1")', ''),
        ('^DD(9999907.01,0,"UP")', '9999907'),
        ('^DD(9999907.01,.01,0)', 'wp1^Wx^^0;1^Q'),
        ('^DD(9999907.01,.01,"DT")', '3120720'),
        ('^DD(9999907.01,"B","wp1",.01)', ''),
        ('^DD(9999907.01,"GL",0,1,.01)', ''),
        ('^DD(9999907.02,0)', u'wp2 SUB-FIELD^^.01^1'),
        ('^DD(9999907.02,0,"DT")', '3120720'),
        ('^DD(9999907.02,0,"NM","wp2")', ''),
        ('^DD(9999907.02,0,"UP")', '9999907'),
        ('^DD(9999907.02,.01,0)', 'wp2^WL^^0;1^Q'),
        ('^DD(9999907.02,.01,"DT")', '3120720'),
        ('^DD(9999907.02,"B","wp2",.01)', ''),
        ('^DD(9999907.02,"GL",0,1,.01)', ''),
    ]

    IX = [
    ]

    def _createFile(self):
        # This creates a file
        Globals.deserialise(self.DIC)
        Globals.deserialise(self.DD)
        Globals.deserialise(self.DIZ)
        Globals.deserialise(self.IX)

        # Are indices setup
        dd = self.dbs.dd("PYTEST5")
        self.assertEqual(dd.fileid, "9999907")
        self.assertEqual(len(dd.indices), 1)
        #self.assertEqual(len(dd.new_indices), 1)

    def _cleanupFile(self):
        # This deletes a file
        Globals["^DIC"]["9999907"].kill()
        Globals["^DIC"]["B"]["PYTEST5"].kill()
        Globals["^DD"]["9999907"].kill()
        Globals["^DIZ"]["9999907"].kill()
        Globals["^DD"]["9999907.01"].kill()
        Globals["^DD"]["9999907.02"].kill()

    def setUp(self):
        self.dbs = connect("0", "")

        self._cleanupFile()
        self._createFile()

    def tearDown(self):
        # destroy the file
        self._cleanupFile()

    def test_read(self):
        """
            Validate the data which was in the file. This data
            was created via FILEMAN.
        """
        pytest5 = self.dbs.get_file("PYTEST5", internal=True)
        cursor = pytest5.traverser("B", "e")
        key, rowid = cursor.next()
        rec = pytest5.get(rowid)

        self.assertEqual(rec.WP1, ["This is text entered via the line editor.",
            "This is another line.",
            "This is a long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, l",
            "ong, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, ",
            "long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long,",
            " long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long line"])

        pytest5 = self.dbs.get_file("PYTEST5", internal=False)
        cursor = pytest5.traverser("B", "e")
        key, rowid = cursor.next()
        rec = pytest5.get(rowid)

        self.assertEqual(rec.WP1, ["This is text entered via the line editor.",
            "This is another line.",
            "This is a long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, l",
            "ong, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, ",
            "long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long,",
            " long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long, long line"])

    def test_write(self):
        """
            This is a simple write.

            TODO: Verify UTF8
            TODO: Convert WP type to a string. The list is not Pythonic
        """
        pytest5 = self.dbs.get_file("PYTEST5", internal=True)
        transaction.begin()
        rec = pytest5.new()
        rec.NAME = "Insert Internal"
        rec.WP1 = [u"line 1", u"line 2", u"line 3"]
        rec.WP2 = [u"2 line 1", u"2 line 2", u"2 line 3"]
        transaction.commit()

        cursor = pytest5.traverser("B", "Insert Internal")
        key, rowid = cursor.next()
        rec = pytest5.get(rowid)
        self.assertEqual(str(rec.NAME), "Insert Internal")

        self.assertEqual(rec.WP1, [u"line 1", u"line 2", u"line 3"])
        self.assertEqual(rec.WP2, [u"2 line 1", u"2 line 2", u"2 line 3"])

    def test_indexing(self):
        """
            TODO: walk a date index. Can I return the values as ints / floats.
        """

test_cases = (TestWP,)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
