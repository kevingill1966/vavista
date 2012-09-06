"""
    Test the query planner
"""

import unittest

from vavista.fileman import connect, transaction
from vavista.M import Globals

from vavista.fileman.query_planner import make_plan

class TestPlanner(unittest.TestCase):

    # DIC record
    DIC = [
        ('^DIC(9999920,0)', u'PYTEST20^9999920'),
        ('^DIC(9999920,0,"AUDIT")', '@'),
        ('^DIC(9999920,0,"DD")', '@'),
        ('^DIC(9999920,0,"DEL")', '@'),
        ('^DIC(9999920,0,"GL")', '^DIZ(9999920,'),
        ('^DIC(9999920,0,"LAYGO")', '@'),
        ('^DIC(9999920,0,"RD")', '@'),
        ('^DIC(9999920,0,"WR")', '@'),
        ('^DIC(9999920,"%A")', '10000000020^3120716'),
        ('^DIC("B","PYTEST20",9999920)', ''),
    ]

    # ^DIZ record
    DIZ = [
        ('^DIZ(9999920,0)', 'PYTEST20^9999920^0^0')
    ]

    # ^DD record
    # I added a traditional style index / cross reference (C)
    DD = [
        ('^DD(9999920,0)', u'FIELD^^2^3'),
        ('^DD(9999920,0,"DT")', '3120716'),
        ('^DD(9999920,0,"IX","B",9999920,.01)', ''),
    #   ('^DD(9999920,0,"IX","C",9999920,1)', ''),
        ('^DD(9999920,0,"NM","PYTEST20")', ''),
        ('^DD(9999920,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X"),
        ('^DD(9999920,.01,1,0)', '^.1'),
        ('^DD(9999920,.01,1,1,0)', '9999920^B'),
        ('^DD(9999920,.01,1,1,1)', 'S ^DIZ(9999920,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999920,.01,1,1,2)', 'K ^DIZ(9999920,"B",$E(X,1,30),DA)'),
        ('^DD(9999920,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999920,1,0)', 'Textline One^F^^0;2^K:$L(X)>200!($L(X)<1) X'),
        ('^DD(9999920,1,.1)', 'Text Line One'),
        ('^DD(9999920,1,1,0)', '^.1'),
        # Traditional Index
        ('^DD(9999920,1,1,1,0)', '9999920^C'),
        ('^DD(9999920,1,1,1,1)', 'S ^DIZ(9999920,"C",$E(X,1,30),DA)=""'),
        ('^DD(9999920,1,1,1,2)', 'K ^DIZ(9999920,"C",$E(X,1,30),DA)'),
        ('^DD(9999920,1,1,1,"DT")', '3120716'),
        ('^DD(9999920,1,3)', 'Answer must be 1-200 characters in length.'),
        ('^DD(9999920,1,"DT")', '3120716'),
        ('^DD(9999920,2,0)', 'textline2^RF^^1;1^K:$L(X)>200!($L(X)<1) X'),
        ('^DD(9999920,2,3)', 'Answer must be 1-200 characters in length.'),
        ('^DD(9999920,2,"DT")', '3120716'),
        ('^DD(9999920,"B","NAME",.01)', ''),
        ('^DD(9999920,"B","Text Line One",1)', '1'),
        ('^DD(9999920,"B","Textline One",1)', ''),
        ('^DD(9999920,"B","textline2",2)', ''),
        ('^DD(9999920,"GL",0,1,.01)', ''),
        ('^DD(9999920,"GL",0,2,1)', ''),
        ('^DD(9999920,"GL",1,1,2)', ''),
        ('^DD(9999920,"IX",.01)', ''),
        # Traditional Index
        ('^DD(9999920,"IX",1)', ''),
        ('^DD(9999920,"RQ",.01)', ''),
        ('^DD(9999920,"RQ",2)', ''),
    ]

    # ^DD("IX") describes "New" style indexes
    # TODO: I must allocate the index id dynamically
    IX = [
        ('^DD("IX",116,0)', '9999920^D^Regular index on textline2^R^^F^IR^I^9999920^^^^^LS'),
        ('^DD("IX",116,1)', 'S ^DIZ(9999920,"D",$E(X,1,30),DA)=""'),
        ('^DD("IX",116,2)', 'K ^DIZ(9999920,"D",$E(X,1,30),DA)'),
        ('^DD("IX",116,2.5)', 'K ^DIZ(9999920,"D")'),
        ('^DD("IX",116,11.1,0)', '^.114IA^1^1'),
        ('^DD("IX",116,11.1,1,0)', '1^F^9999920^2^30^1^F'),
        ('^DD("IX",116,11.1,1,3)', ''),
        ('^DD("IX",116,11.1,"AC",1,1)', ''),
        ('^DD("IX",116,11.1,"B",1,1)', ''),
        ('^DD("IX",116,11.1,"BB",1,1)', ''),
        ('^DD("IX","B",9999920,116)', ''),
        ('^DD("IX","IX","D",116)', ''),
        ('^DD("IX","AC",9999920,116)', ''),
        ('^DD("IX","BB",9999920,"D",116)', ''),
        ('^DD("IX","F",9999920,2,116,1)', ''),
    ]

    def _cleanupFile(self):
        transaction.begin()
        Globals["^DIC"]["9999920"].kill()
        Globals["^DIC"]['B']["PYTEST20"].kill()
        Globals["^DD"]["9999920"].kill()
        Globals["^DIZ"]["9999920"].kill()
        Globals["^DD"]["IX"]["116"].kill()
        Globals["^DD"]["IX"]["B"]["9999920"].kill()
        Globals["^DD"]["IX"]["BB"]["9999920"].kill()
        Globals["^DD"]["IX"]["AC"]["9999920"].kill()
        Globals["^DD"]["IX"]["IX"]["D"]["116"].kill()
        Globals["^DD"]["IX"]["F"]["9999920"].kill()
        transaction.commit()

    def _createFile(self):
        # This creates a file
        transaction.begin()
        Globals.deserialise(self.DIC)
        Globals.deserialise(self.DD)
        Globals.deserialise(self.DIZ)
        Globals.deserialise(self.IX)

        pytest = self.dbs.get_file("PYTEST20")
        for i in range(1, 11):
            pytest.insert(NAME='ROW%x' % i, TEXTLINE_ONE="%x: LINE 1" % i, TEXTLINE2="%x: LINE 2" % i)
        transaction.commit()

        # Are indices setup
        dd = self.dbs.dd("PYTEST20")
    #   self.assertEqual(len(dd.indices), 2)
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

    def test_file_order(self):
        """
            File order traversals.

            Simple case, only filters by / order bys the file order
        """
        pytest = self.dbs.get_file("PYTEST20", fieldnames=[
            'NAME', 'Textline_One', 'textline2'])

        # File order traversal, default order - ascending
        result = list(make_plan(pytest))
        result = [row[0] for row in result]
        self.assertEqual(result, ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])

        # File order traversal, default order - ascending
        result = list(make_plan(pytest, order_by = [["_rowid", "ASC"]]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])

        # File order traversal, descending
        result = list(make_plan(pytest, order_by = [["_rowid", "DESC"]]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['10', '9', '8', '7', '6', '5', '4', '3', '2', '1'])

        result = list(make_plan(pytest, order_by = [["_rowid", "ASC"]], limit=3))
        result = [row[0] for row in result]
        self.assertEqual(result, ['1', '2', '3'])

        result = list(make_plan(pytest, order_by = [["_rowid", "ASC"]], offset=8))
        result = [row[0] for row in result]
        self.assertEqual(result, ['9', '10'])

        result = list(make_plan(pytest, filters = [["_rowid", ">", '8']]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['9', '10'])

        result = list(make_plan(pytest, filters = [["_rowid", ">=", '8']]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['8', '9', '10'])

        result = list(make_plan(pytest, filters = [["_rowid", "<", '3']]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['1', '2'])

        result = list(make_plan(pytest, filters = [["_rowid", "<=", '3']]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['1', '2', '3'])

        result = list(make_plan(pytest, filters = [["_rowid", "=", '5']]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['5'])

        result = list(make_plan(pytest, filters = [["_rowid", ">=", '5'], ["_rowid", "<=", "7"]]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['5', '6', '7'])

        result = list(make_plan(pytest, filters = [["_rowid", ">=", '5'], ["_rowid", "<=", "7"]],
            order_by = [["_rowid", "DESC"]]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['7', '6', '5'])

    def test_index_b(self):
        """
            Name order.

            Simple case, only filters by / order bys the name order
        """
        pytest = self.dbs.get_file("PYTEST20", fieldnames=[
            'NAME', 'Textline_One', 'textline2'])

        result = list(make_plan(pytest, order_by=[['NAME', 'ASC']]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])

        result = list(make_plan(pytest, order_by=[['NAME', 'DESC']]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['10', '9', '8', '7', '6', '5', '4', '3', '2', '1'])

        result = list(make_plan(pytest, order_by = [["NAME", "ASC"]], limit=3))
        result = [row[0] for row in result]
        self.assertEqual(result, ['1', '2', '3'])

        result = list(make_plan(pytest, order_by = [["NAME", "ASC"]], offset=8))
        result = [row[0] for row in result]
        self.assertEqual(result, ['9', '10'])

        result = list(make_plan(pytest, filters = [["NAME", ">", 'ROW8']]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['9', '10'])

        result = list(make_plan(pytest, filters = [["NAME", ">=", 'ROW8']]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['8', '9', '10'])

        result = list(make_plan(pytest, filters = [["NAME", "<", 'ROW3']]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['1', '2'])

        result = list(make_plan(pytest, filters = [["NAME", "<=", 'ROW3']]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['1', '2', '3'])

        result = list(make_plan(pytest, filters = [["NAME", "=", 'ROW5']]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['5'])

        result = list(make_plan(pytest, filters = [["NAME", ">=", 'ROW5'], ["NAME", "<=", "ROW7"]]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['5', '6', '7'])

        result = list(make_plan(pytest, filters = [["NAME", ">=", 'ROW5'], ["NAME", "<=", "ROW7"]],
            order_by = [["NAME", "DESC"]]))
        result = [row[0] for row in result]
        self.assertEqual(result, ['7', '6', '5'])


test_cases = (TestPlanner, )

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
