
import unittest

from vavista.fileman import connect, transaction
from vavista.M import Globals

class TestTextline(unittest.TestCase):
    """
        Create a simple file containing two text lines,
        one mandatory and one optional. Verify that the
        read and write functionality works.
    """

    # DIC record
    DIC = [
        ('^DIC(9999903,0)', u'PYTEST1^9999903'),
        ('^DIC(9999903,0,"AUDIT")', '@'),
        ('^DIC(9999903,0,"DD")', '@'),
        ('^DIC(9999903,0,"DEL")', '@'),
        ('^DIC(9999903,0,"GL")', '^DIZ(9999903,'),
        ('^DIC(9999903,0,"LAYGO")', '@'),
        ('^DIC(9999903,0,"RD")', '@'),
        ('^DIC(9999903,0,"WR")', '@'),
        ('^DIC(9999903,"%A")', '10000000020^3120716'),
        ('^DIC("B","PYTEST1",9999903)', ''),
    ]

    # ^DIZ record
    DIZ = [
        ('^DIZ(9999903,0)', 'PYTEST1^9999903^')
    ]

    # ^DD record
    # I added a traditional style index / cross reference (C)
    DD = [
        ('^DD(9999903,0)', u'FIELD^^2^3'),
        ('^DD(9999903,0,"DT")', '3120716'),
        ('^DD(9999903,0,"IX","B",9999903,.01)', ''),
        ('^DD(9999903,0,"IX","C",9999903,1)', ''),
        ('^DD(9999903,0,"NM","PYTEST1")', ''),
        ('^DD(9999903,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X"),
        ('^DD(9999903,.01,1,0)', '^.1'),
        ('^DD(9999903,.01,1,1,0)', '9999903^B'),
        ('^DD(9999903,.01,1,1,1)', 'S ^DIZ(9999903,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999903,.01,1,1,2)', 'K ^DIZ(9999903,"B",$E(X,1,30),DA)'),
        ('^DD(9999903,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999903,1,0)', 'Textline One^F^^0;2^K:$L(X)>200!($L(X)<1) X'),
        ('^DD(9999903,1,.1)', 'Text Line One'),
        ('^DD(9999903,1,1,0)', '^.1'),
        # Traditional Index
        ('^DD(9999903,1,1,1,0)', '9999903^C'),
        ('^DD(9999903,1,1,1,1)', 'S ^DIZ(9999903,"C",$E(X,1,30),DA)=""'),
        ('^DD(9999903,1,1,1,2)', 'K ^DIZ(9999903,"C",$E(X,1,30),DA)'),
        ('^DD(9999903,1,1,1,"DT")', '3120716'),
        ('^DD(9999903,1,3)', 'Answer must be 1-200 characters in length.'),
        ('^DD(9999903,1,"DT")', '3120716'),
        ('^DD(9999903,2,0)', 'textline2^RF^^1;1^K:$L(X)>200!($L(X)<1) X'),
        ('^DD(9999903,2,3)', 'Answer must be 1-200 characters in length.'),
        ('^DD(9999903,2,"DT")', '3120716'),
        ('^DD(9999903,"B","NAME",.01)', ''),
        ('^DD(9999903,"B","Text Line One",1)', '1'),
        ('^DD(9999903,"B","Textline One",1)', ''),
        ('^DD(9999903,"B","textline2",2)', ''),
        ('^DD(9999903,"GL",0,1,.01)', ''),
        ('^DD(9999903,"GL",0,2,1)', ''),
        ('^DD(9999903,"GL",1,1,2)', ''),
        ('^DD(9999903,"IX",.01)', ''),
        # Traditional Index
        ('^DD(9999903,"IX",1)', ''),
        ('^DD(9999903,"RQ",.01)', ''),
        ('^DD(9999903,"RQ",2)', ''),
    ]

    # ^DD("IX") describes "New" style indexes
    # TODO: I must allocate the index id dynamically
    IX = [
        ('^DD("IX",116,0)', '9999903^D^Regular index on textline2^R^^F^IR^I^9999903^^^^^LS'),
        ('^DD("IX",116,1)', 'S ^DIZ(9999903,"D",$E(X,1,30),DA)=""'),
        ('^DD("IX",116,2)', 'K ^DIZ(9999903,"D",$E(X,1,30),DA)'),
        ('^DD("IX",116,2.5)', 'K ^DIZ(9999903,"D")'),
        ('^DD("IX",116,11.1,0)', '^.114IA^1^1'),
        ('^DD("IX",116,11.1,1,0)', '1^F^9999903^2^30^1^F'),
        ('^DD("IX",116,11.1,1,3)', ''),
        ('^DD("IX",116,11.1,"AC",1,1)', ''),
        ('^DD("IX",116,11.1,"B",1,1)', ''),
        ('^DD("IX",116,11.1,"BB",1,1)', ''),
        ('^DD("IX","B",9999903,116)', ''),
        ('^DD("IX","IX","D",116)', ''),
        ('^DD("IX","AC",9999903,116)', ''),
        ('^DD("IX","BB",9999903,"D",116)', ''),
        ('^DD("IX","F",9999903,2,116,1)', ''),
    ]

    def _cleanupFile(self):
        transaction.begin()
        Globals["^DIC"]["9999903"].kill()
        Globals["^DIC"]['B']["PYTEST1"].kill()
        Globals["^DD"]["9999903"].kill()
        Globals["^DIZ"]["9999903"].kill()
        Globals["^DD"]["IX"]["116"].kill()
        Globals["^DD"]["IX"]["B"]["9999903"].kill()
        Globals["^DD"]["IX"]["BB"]["9999903"].kill()
        Globals["^DD"]["IX"]["AC"]["9999903"].kill()
        Globals["^DD"]["IX"]["IX"]["D"]["116"].kill()
        Globals["^DD"]["IX"]["F"]["9999903"].kill()
        transaction.commit()

    def _createFile(self):
        # This creates a file
        transaction.begin()
        Globals.deserialise(self.DIC)
        Globals.deserialise(self.DD)
        Globals.deserialise(self.DIZ)
        Globals.deserialise(self.IX)
        transaction.commit()

        # Are indices setup
        dd = self.dbs.dd("PYTEST1")
        self.assertEqual(len(dd.indices), 2)
        self.assertEqual(len(dd.new_indices), 1)

    def setUp(self):
        self.dbs = connect("0", "")

        self._cleanupFile()
        self._createFile()

    def tearDown(self):
        # destroy the file
        if transaction.in_transaction:
            transaction.abort()
        self._cleanupFile()

    def test_readwrite(self):
        transaction.begin()
        pytest1 = self.dbs.get_file("PYTEST1", internal=False)
        record = pytest1.new()
        record.NAME = 'Test Insert'
        record.TEXTLINE_ONE = "LINE 1"
        record.TEXTLINE2 = "LINE 2"
        transaction.commit()

        # The low-level traverser, walks index "B", on NAME field
        # ('^DD(9999903,0,"IX","B",9999903,.01)', ''),
        cursor = pytest1.traverser("B", "Test")
        key, rowid = cursor.next()

        # retrieve the record
        rec = pytest1.get(rowid)

        # validate the inserted data
        self.assertEqual(str(rec.NAME), "Test Insert")
        self.assertEqual(str(rec.TEXTLINE_ONE), "LINE 1")
        self.assertEqual(str(rec.TEXTLINE2), "LINE 2")

        # Once this is working
        # Verify mandatory field insert logic
        # Verify utf-8 characters
        # Verify update

    def test_traversal2(self):
        # Strange error here - the first pass on the file
        # works fine. The second pass inserts rows in a strange
        # order and then fails. It is as if it is getting
        # the rowids in the wrong place.

        try:
            return self.test_traversal(False) # External Data
        except Exception, e:
            import pdb; pdb.set_trace()
            raise

    def test_traversal(self, internal=True):
        """
            Insert multiple items. Verify that traversal back and 
            forward works.
        """
        transaction.begin()
        pytest1 = self.dbs.get_file("PYTEST1", internal=internal)
        for i in range(10):
            record = pytest1.new()
            record.NAME = 'ROW%d' % i
            record.TEXTLINE_ONE = "%d: LINE 1" % i
            record.TEXTLINE2 = "%d: LINE 2" % i
        transaction.commit()

        # Index B is a default Key Field on the NAME field
        # It is a "traditional" index
        cursor = pytest1.traverser("B", "ROW4", "ROW8")
        result = list(cursor)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0][0], "ROW4")
        self.assertEqual(result[1][0], "ROW5")
        self.assertEqual(result[2][0], "ROW6")
        self.assertEqual(result[3][0], "ROW7")

        cursor = pytest1.traverser("B", "ROW8", "ROW4", ascending=False)
        result = list(cursor)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0][0], "ROW8")
        self.assertEqual(result[1][0], "ROW7")
        self.assertEqual(result[2][0], "ROW6")
        self.assertEqual(result[3][0], "ROW5")

        cursor = pytest1.traverser("B", "ROW4", "ROW8", to_rule="<=", from_rule=">=")
        result = list(cursor)
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0][0], "ROW4")
        self.assertEqual(result[1][0], "ROW5")
        self.assertEqual(result[2][0], "ROW6")
        self.assertEqual(result[3][0], "ROW7")
        self.assertEqual(result[4][0], "ROW8")

        cursor = pytest1.traverser("B", "ROW8", "ROW4", ascending=False, to_rule=">=", from_rule="<=")
        result = list(cursor)
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0][0], "ROW8")
        self.assertEqual(result[1][0], "ROW7")
        self.assertEqual(result[2][0], "ROW6")
        self.assertEqual(result[3][0], "ROW5")
        self.assertEqual(result[4][0], "ROW4")

        # Index C is a Cross reference Field on the TEXTLINE_ONE field
        # It is a "traditional" index

        # If I just pass in "4" it searches wrongly, putting integers
        # before strings.
        # TODO: how to make numbers and strings collate properly
        cursor = pytest1.traverser("C", "4:", "8:")
        result = list(cursor)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0][0], "4: LINE 1")
        self.assertEqual(result[1][0], "5: LINE 1")
        self.assertEqual(result[2][0], "6: LINE 1")
        self.assertEqual(result[3][0], "7: LINE 1")

        cursor = pytest1.traverser("C", "8:", "4:", ascending=False)
        result = list(cursor)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0][0], "7: LINE 1")
        self.assertEqual(result[1][0], "6: LINE 1")
        self.assertEqual(result[2][0], "5: LINE 1")
        self.assertEqual(result[3][0], "4: LINE 1")

        # TODO: This is not working - I tried to renumber the index and
        # since then it is not working properly. 

        # Index D is a new style index. Traversal works the same as 
        # traditional indices.
        cursor = pytest1.traverser("D", "4:", "8:")
        result = list(cursor)
        self.assertEqual(result[0][0], "4: LINE 2")
        self.assertEqual(result[1][0], "5: LINE 2")
        self.assertEqual(result[2][0], "6: LINE 2")
        self.assertEqual(result[3][0], "7: LINE 2")

        cursor = pytest1.traverser("D", "8:", "4:", ascending=False)
        result = list(cursor)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0][0], "7: LINE 2")
        self.assertEqual(result[1][0], "6: LINE 2")
        self.assertEqual(result[2][0], "5: LINE 2")
        self.assertEqual(result[3][0], "4: LINE 2")

    def test_mandatory(self):
        """
            Fileman's concept of mandatory is that the value cannot be
            set to a null value. However, if a value is never put into
            it, it is not validated.
        """
        transaction.begin()
        pytest1 = self.dbs.get_file("PYTEST1", internal=False)
        record = pytest1.new()
        record.NAME = 'ROW1'
        record.TEXTLINE_ONE = "LINE 1"
        transaction.commit()

        transaction.begin()
        exception = False
        try:
            pytest1 = self.dbs.get_file("PYTEST1", internal=False)
            record = pytest1.new()
            record.NAME = 'ROW2'
            record.TEXTLINE_ONE = "LINE 1"
            record.TEXTLINE2 = ""
        except:
            exception = True
        self.assertEqual(exception, True)
        transaction.abort()

        transaction.begin()
        exception = False
        try:
            pytest1 = self.dbs.get_file("PYTEST1", internal=False)
            record = pytest1.new()
            record.NAME = 'ROW3'
            record.TEXTLINE_ONE = "LINE 1"
            record.TEXTLINE2 = None
        except:
            exception = True
        self.assertEqual(exception, True)
        transaction.abort()

        transaction.begin()
        pytest1 = self.dbs.get_file("PYTEST1", internal=True)
        record = pytest1.new()
        record.NAME = 'ROW4'
        record.TEXTLINE_ONE = "LINE 1"

        pytest1 = self.dbs.get_file("PYTEST1", internal=True)
        record = pytest1.new()
        record.NAME = 'ROW5'
        record.TEXTLINE_ONE = "LINE 1"
        record.TEXTLINE2 = ""

        pytest1 = self.dbs.get_file("PYTEST1", internal=True)
        record = pytest1.new()
        record.NAME = 'ROW6'
        record.TEXTLINE_ONE = "LINE 1"
        record.TEXTLINE2 = None

        transaction.commit()

test_cases = (TestTextline, )

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
