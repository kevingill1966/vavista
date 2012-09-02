"""
    For django integration, I have to put query logic into the server
    side. The django client starts with a query that selects the entire
    file. It then filters it down, and it provides a sort order.

    This is implemented in dbsfile->_index_select, RowInterator, IndexIterator
    etc.

    The initial implementation is being done by hacking the traverse
    method. A more specific method will eventually be crafted.
"""

import unittest

from vavista.fileman import connect, transaction, FilemanError
from vavista.M import Globals

from vavista.fileman.dbsfile import RowIterator, IndexIterator
from vavista.fileman.shared import clean_rowid

class TestDjango(unittest.TestCase):

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
        ('^DIZ(9999903,0)', 'PYTEST1^9999903^0^0')
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

        pytest1 = self.dbs.get_file("PYTEST1")
        for i in range(10):
            pytest1.insert(NAME='ROW%d' % i, TEXTLINE_ONE="%d: LINE 1" % i, TEXTLINE2="%d: LINE 2" % i)
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

    def test_single_rule_rowid(self):
        """
            One rule - based on rowid
        """
        pytest1 = self.dbs.get_file("PYTEST1", fieldnames=[
            'NAME', 'Textline_One', 'textline2'])

        cursor = pytest1.traverser(filters=[['_rowid', '=', '4']])
        result = [(cursor.lastrowid, values) for values in cursor]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], '4')
        self.assertEqual(result[0][1][0], 'ROW3')

        # Check the query plan
        self.assertEqual(cursor.__class__, RowIterator)
        self.assertEqual(clean_rowid(cursor.from_rowid), '4')
        self.assertEqual(clean_rowid(cursor.to_rowid), '4')
        self.assertEqual(cursor.from_rule, '>=')
        self.assertEqual(cursor.to_rule, '<=')

        cursor = pytest1.traverser(filters=[['_rowid', '>=', '4']])
        result = [(cursor.lastrowid, values) for values in cursor]
        self.assertEqual(len(result), 7)
        self.assertEqual(result[0][0], '4')
        self.assertEqual(result[0][1][0], 'ROW3')
        self.assertEqual(result[-1][0], '10')
        self.assertEqual(result[-1][1][0], 'ROW9')

        # Check the query plan
        self.assertEqual(cursor.__class__, RowIterator)
        self.assertEqual(clean_rowid(cursor.from_rowid), '4')
        self.assertEqual(cursor.to_rowid, None)
        self.assertEqual(cursor.from_rule, '>=')

        cursor = pytest1.traverser(filters=[['_rowid', 'in', ['4']]])
        result = [(cursor.lastrowid, values) for values in cursor]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], '4')
        self.assertEqual(result[0][1][0], 'ROW3')

        # Check the query plan
        self.assertEqual(cursor.__class__, RowIterator)
        self.assertEqual(clean_rowid(cursor.from_rowid), '4')
        self.assertEqual(clean_rowid(cursor.to_rowid), '4')
        self.assertEqual(cursor.from_rule, '>=')
        self.assertEqual(cursor.to_rule, '<=')


    def test_single_rule_index(self):
        """
            One rule based on an indexed column
        """
        pytest1 = self.dbs.get_file("PYTEST1", fieldnames=[
            'NAME', 'Textline_One', 'textline2'])

        cursor = pytest1.traverser(filters=[['name', '=', 'ROW3']])
        result = [(cursor.lastrowid, values) for values in cursor]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], '4')
        self.assertEqual(result[0][1][0], 'ROW3')

        # Check the query plan
        self.assertEqual(cursor.__class__, IndexIterator)
        self.assertEqual(cursor.index, 'B')
        self.assertEqual(cursor.from_value, 'ROW3')
        self.assertEqual(cursor.to_value, 'ROW3')
        self.assertEqual(cursor.from_rule, '>=')
        self.assertEqual(cursor.to_rule, '<=')

        pytest1 = self.dbs.get_file("PYTEST1", fieldnames=[
            'NAME', 'Textline_One', 'textline2'])

        cursor = pytest1.traverser(filters=[['name', '>=', 'ROW3']])
        result = [(cursor.lastrowid, values) for values in cursor]
        self.assertEqual(len(result), 7)
        self.assertEqual(result[0][0], '4')
        self.assertEqual(result[0][1][0], 'ROW3')

        # Check the query plan
        self.assertEqual(cursor.__class__, IndexIterator)
        self.assertEqual(cursor.index, 'B')
        self.assertEqual(cursor.from_value, 'ROW3')
        self.assertEqual(cursor.to_value, None)
        self.assertEqual(cursor.from_rule, '>=')

        pytest1 = self.dbs.get_file("PYTEST1", fieldnames=[
            'NAME', 'Textline_One', 'textline2'])

        cursor = pytest1.traverser(filters=[['name', 'in', ['ROW3']]])
        result = [(cursor.lastrowid, values) for values in cursor]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], '4')
        self.assertEqual(result[0][1][0], 'ROW3')

        # Check the query plan
        self.assertEqual(cursor.__class__, IndexIterator)
        self.assertEqual(cursor.index, 'B')
        self.assertEqual(cursor.from_value, 'ROW3')
        self.assertEqual(cursor.to_value, 'ROW3')
        self.assertEqual(cursor.from_rule, '>=')
        self.assertEqual(cursor.to_rule, '<=')


test_cases = (TestDjango, )

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
