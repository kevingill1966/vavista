
# Test the Fileman DBS interface

# TODO: Investigate new and old style indexes

import unittest

from vavista.fileman import connect, transaction
from vavista.M import Globals

class TestFileman(unittest.TestCase):

    def setUp(self):
        self.dbs = connect("0", "")

    def tearDown(self):
        pass

    def test_fileid(self):
        file = self.dbs.get_file("FILE")
        self.assertEqual(file.dd.fileid, "1")
        self.assertEqual(file.get("1")[".01"], "FILE")
        self.assertEqual(file.get("1").NAME, "FILE")

    def test_list_files(self):
        rv = self.dbs.list_files()
        #TODO: Check a value

    def test_dd(self):
        """
            I created this file via Fileman to test
        """
        dd = self.dbs.dd("KEVIN1")
        self.assertEqual(dd.fileid, "999900")
        print '\n'
        print dd
        print self.dbs.dd("FILE")
        print self.dbs.dd("LOCATION")

    def test_fileget(self):
        file = self.dbs.get_file("OPTION")
        print file.get("5159")

    def test_insert(self):
        # Create a new record - write it to the database
        kevin1 = self.dbs.get_file("KEVIN1")
        record = kevin1.new()
        record.NAME = 'hello there from unit test'
        transaction.commit()

        # There is no rowid until after the commit
        # read the record back and make sure it is the same
        rowid = record._rowid
        copy = kevin1.get(rowid)
        self.assertEqual(str(copy.NAME), 'hello there from unit test')

        # update the record and verify that the updates worked.
        record.NAME = 'hello there from unit test2'
        transaction.commit()
        copy = kevin1.get(rowid)
        self.assertEqual(str(copy.NAME), 'hello there from unit test2')

    def test_insert_external(self):
        # Create a new record - write it to the database
        kevin1 = self.dbs.get_file("KEVIN1", internal=False)
        record = kevin1.new()
        record.NAME = 'hello there from unit test'
        transaction.commit()

        # There is no rowid until after the commit
        # read the record back and make sure it is the same
        rowid = record._rowid
        copy = kevin1.get(rowid)
        self.assertEqual(str(copy.NAME), 'hello there from unit test')

        # update the record and verify that the updates worked.
        record.NAME = 'hello there from unit test2'
        transaction.commit()
        copy = kevin1.get(rowid)
        self.assertEqual(str(copy.NAME), 'hello there from unit test2')

        # delete
        copy.delete()

        # Verify that the copy row no longer exists.
        missing = False
        try:
            kevin1.get(rowid)
        except:
            missing = True
        self.assertEqual(missing, True)


class TestTextline(unittest.TestCase):
    """
        Create a simple file containing two text lines,
        one mandatory and one optional. Verify that the
        read and write functionality works.
    """

    # DIC record
    DIC = [
        ('^DIC(9999902,0)', u'PYTEST1^9999902'),
        ('^DIC(9999902,0,"AUDIT")', '@'),
        ('^DIC(9999902,0,"DD")', '@'),
        ('^DIC(9999902,0,"DEL")', '@'),
        ('^DIC(9999902,0,"GL")', '^DIZ(9999902,'),
        ('^DIC(9999902,0,"LAYGO")', '@'),
        ('^DIC(9999902,0,"RD")', '@'),
        ('^DIC(9999902,0,"WR")', '@'),
        ('^DIC(9999902,"%A")', '10000000020^3120716'),
    ]

    # ^DIZ record
    DIZ = [
        ('^DIZ(9999902,0)', 'PYTEST1^9999902^')
    ]

    # ^DD record
    DD = [
        ('^DD(9999902,0)', u'FIELD^^2^3'),
        ('^DD(9999902,0,"DT")', '3120716'),
        ('^DD(9999902,0,"IX","B",9999902,.01)', ''),
        ('^DD(9999902,0,"NM","PYTEST1")', ''),
        ('^DD(9999902,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X"),
        ('^DD(9999902,.01,1,0)', '^.1'),
        ('^DD(9999902,.01,1,1,0)', '9999902^B'),
        ('^DD(9999902,.01,1,1,1)', 'S ^DIZ(9999902,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999902,.01,1,1,2)', 'K ^DIZ(9999902,"B",$E(X,1,30),DA)'),
        ('^DD(9999902,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999902,1,0)', 'Textline One^F^^0;2^K:$L(X)>200!($L(X)<1) X'),
        ('^DD(9999902,1,.1)', 'Text Line One'),
        ('^DD(9999902,1,3)', 'Answer must be 1-200 characters in length.'),
        ('^DD(9999902,1,"DT")', '3120716'),
        ('^DD(9999902,2,0)', 'textline2^RF^^1;1^K:$L(X)>200!($L(X)<1) X'),
        ('^DD(9999902,2,3)', 'Answer must be 1-200 characters in length.'),
        ('^DD(9999902,2,"DT")', '3120716'),
        ('^DD(9999902,"B","NAME",.01)', ''),
        ('^DD(9999902,"B","Text Line One",1)', '1'),
        ('^DD(9999902,"B","Textline One",1)', ''),
        ('^DD(9999902,"B","textline2",2)', ''),
        ('^DD(9999902,"GL",0,1,.01)', ''),
        ('^DD(9999902,"GL",0,2,1)', ''),
        ('^DD(9999902,"GL",1,1,2)', ''),
        ('^DD(9999902,"IX",.01)', ''),
        ('^DD(9999902,"RQ",.01)', ''),
        ('^DD(9999902,"RQ",2)', '')
    ]

    def setUp(self):
        self.dbs = connect("0", "")

        # This creates a file
        Globals["^DIC"]["9999902"].kill()
        Globals["^DD"]["9999902"].kill()
        Globals["^DIZ"]["9999902"].kill()
        Globals.deserialise(self.DIC)
        Globals.deserialise(self.DD)
        Globals.deserialise(self.DIZ)

    def tearDown(self):
        # destroy the file
        Globals["^DIC"]["9999902"].kill()
        Globals["^DD"]["9999902"].kill()
        Globals["^DIZ"]["9999902"].kill()

    def test_readwrite(self):
        dd = self.dbs.dd("PYTEST1")
        self.assertEqual(dd.fileid, "9999902")

        pytest1 = self.dbs.get_file("PYTEST1", internal=False)
        record = pytest1.new()
        record.NAME = 'Test Insert'
        record.TEXTLINE_ONE = "LINE 1"
        record.TEXTLINE2 = "LINE 2"
        transaction.commit()

        # The low-level traverser, walks index "B", on NAME field
        # ('^DD(9999902,0,"IX","B",9999902,.01)', ''),
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

    def test_traversal(self):
        """
            Insert multiple items. Verify that traversal back and 
            forward works.
            TODO: use an index on one of my fields rather than on the
            default NAME field.
        """
        pytest1 = self.dbs.get_file("PYTEST1")
        for i in range(10):
            record = pytest1.new()
            record.NAME = 'ROW%d' % i
            record.TEXTLINE_ONE = "%d: LINE 1" % i
            record.TEXTLINE2 = "%d: LINE 2" % i
        transaction.commit()

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

test_cases = (TestFileman,TestTextline)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
