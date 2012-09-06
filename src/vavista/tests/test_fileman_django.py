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

from vavista.fileman import connect, transaction
from vavista.M import Globals

class TestDjango(unittest.TestCase):

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

    def test_single_rule_rowid(self):
        """
            One rule - based on rowid
        """
        pytest = self.dbs.get_file("PYTEST20", fieldnames=[
            'NAME', 'Textline_One', 'textline2'])

        cursor = pytest.query(filters=[['_rowid', '=', '4']])
        result = list(cursor)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], '4')
        self.assertEqual(result[0][1][0], 'ROW4')

        # Check the query plan
        plan = list(pytest.query(filters=[['_rowid', '=', '4']], explain=True))
        self.assertEqual(len(plan), 2)
        self.assertEqual(plan[0].find("file_order_traversal"), 0)
        self.assertNotEquals(plan[0].find("ascending=True"), -1)
        self.assertNotEquals(plan[0].find("X >= 4 AND X <= 4"), -1)

        cursor = pytest.query(filters=[['_rowid', '>=', '4']])
        result = list(cursor)
        self.assertEqual(len(result), 7)
        self.assertEqual(result[0][0], '4')
        self.assertEqual(result[0][1][0], 'ROW4')
        self.assertEqual(result[-1][0], '10')
        self.assertEqual(result[-1][1][0], 'ROWa')

        # Check the query plan
        plan = list(pytest.query(filters=[['_rowid', '>=', '4']], explain=True))
        self.assertEqual(len(plan), 2)
        self.assertEqual(plan[0].find("file_order_traversal"), 0)
        self.assertNotEquals(plan[0].find("ascending=True"), -1)
        self.assertNotEquals(plan[0].find("X >= 4 AND X None None"), -1)

        cursor = pytest.query(filters=[['_rowid', 'in', ['4']]])
        result = list(cursor)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], '4')
        self.assertEqual(result[0][1][0], 'ROW4')

        # Check the query plan
        plan = list(pytest.query(filters=[['_rowid', 'in', ['4']]], explain=True))
        self.assertEqual(len(plan), 2)
        self.assertEqual(plan[0].find("file_order_traversal"), 0)
        self.assertNotEquals(plan[0].find("ascending=True"), -1)
        self.assertNotEquals(plan[0].find("X >= 4 AND X <= 4"), -1)


    def test_single_rule_index(self):
        """
            One rule based on an indexed column
        """
        pytest = self.dbs.get_file("PYTEST20", fieldnames=[
            'NAME', 'Textline_One', 'textline2'])

        cursor = pytest.query(filters=[['name', '=', 'ROW4']])
        result = list(cursor)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], '4')
        self.assertEqual(result[0][1][0], 'ROW4')

        # Check the query plan
        plan = list(pytest.query(filters=[['name', '=', 'ROW4']], explain=True))
        self.assertEqual(len(plan), 2)
        self.assertEqual(plan[0].find("index_order_traversal"), 0)
        self.assertNotEquals(plan[0].find("ascending=True"), -1)
        self.assertNotEquals(plan[0].find("X >= 'ROW4' AND X <= 'ROW4'"), -1)
        self.assertNotEquals(plan[0].find("index=B"), -1)

        cursor = pytest.query(filters=[['name', '>=', 'ROW4']])
        result = list(cursor)
        self.assertEqual(len(result), 7)
        self.assertEqual(result[0][0], '4')
        self.assertEqual(result[0][1][0], 'ROW4')

        # Check the query plan
        plan = list(pytest.query(filters=[['name', '>=', 'ROW4']], explain=True))
        self.assertEqual(len(plan), 2)
        self.assertEqual(plan[0].find("index_order_traversal"), 0)
        self.assertNotEquals(plan[0].find("ascending=True"), -1)
        self.assertNotEquals(plan[0].find("X >= 'ROW4' AND X None 'None'"), -1)
        self.assertNotEquals(plan[0].find("index=B"), -1)

        cursor = pytest.query(filters=[['name', 'in', ['ROW4']]])
        result = list(cursor)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], '4')
        self.assertEqual(result[0][1][0], 'ROW4')

        plan = list(pytest.query(filters=[['name', 'in', ['ROW4']]], explain=True))
        self.assertEqual(len(plan), 2)
        self.assertEqual(plan[0].find("index_order_traversal"), 0)
        self.assertNotEquals(plan[0].find("ascending=True"), -1)
        self.assertNotEquals(plan[0].find("X >= 'ROW4' AND X <= 'ROW4'"), -1)
        self.assertNotEquals(plan[0].find("index=B"), -1)

    def test_two_rule_index(self):
        """
            Two rules based on an indexed column
        """
        pytest = self.dbs.get_file("PYTEST20", fieldnames=[
            'NAME', 'Textline_One', 'textline2'])

        cursor = pytest.query(filters=[['name', '>=', 'ROW3'], ['name', '<=', 'ROW6']])
        result = list(cursor)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0][1][0], 'ROW3')
        self.assertEqual(result[1][1][0], 'ROW4')
        self.assertEqual(result[2][1][0], 'ROW5')
        self.assertEqual(result[3][1][0], 'ROW6')

        # Check the query plan
        plan = list(pytest.query(filters=[['name', '>=', 'ROW3'], ['name', '<=', 'ROW6']], explain=True))
        self.assertEqual(len(plan), 2)
        self.assertEqual(plan[0].find("index_order_traversal"), 0)
        self.assertNotEquals(plan[0].find("ascending=True"), -1)
        self.assertNotEquals(plan[0].find("X >= 'ROW3' AND X <= 'ROW6'"), -1)
        self.assertNotEquals(plan[0].find("index=B"), -1)


        cursor = pytest.query(filters=[['name', '>=', 'ROW3'], ['name', '<=', 'ROW6']], order_by=[["name", "desc"]])
        result = list(cursor)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0][1][0], 'ROW6')
        self.assertEqual(result[1][1][0], 'ROW5')
        self.assertEqual(result[2][1][0], 'ROW4')
        self.assertEqual(result[3][1][0], 'ROW3')

        # Check the query plan
        plan = list(pytest.query(filters=[['name', '>=', 'ROW3'], ['name', '<=', 'ROW6']], order_by=[["name", "desc"]], explain=True))
        self.assertEqual(len(plan), 3)
        self.assertEqual(plan[0].find("index_order_traversal"), 0)
        self.assertNotEquals(plan[0].find("ascending=False"), -1)
        self.assertNotEquals(plan[0].find("X <= 'ROW6' AND X >= 'ROW3'"), -1)
        self.assertNotEquals(plan[0].find("index=B"), -1)

    def test_no_index(self):
        """
            Search based on a non-indexed column
        """
        pytest = self.dbs.get_file("PYTEST20", fieldnames=[
            'NAME', 'Textline_One', 'textline2'])

        cursor = pytest.query(filters=[['textline_one', '>=', '3:'], ['textline_one', '<=', '6:']])
        result = list(cursor)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][1][0], 'ROW3')
        self.assertEqual(result[1][1][0], 'ROW4')
        self.assertEqual(result[2][1][0], 'ROW5')

        # Check the query plan
        plan = list(pytest.query(filters=[['textline_one', '>=', '3:'], ['textline_one', '<=', '6:']], explain=True))
        self.assertEqual(len(plan), 2)
        self.assertEqual(plan[0].find("file_order_traversal"), 0)
        self.assertNotEquals(plan[0].find("ascending=True"), -1)
        self.assertNotEquals(plan[0].find("X None None AND X None None"), -1)
        self.assertEquals(plan[1].find("apply_filters filters"), 0)
        self.assertNotEquals(plan[1].find("[['textline_one', '>=', '3:'], ['textline_one', '<=', '6:']]"), -1)


    def test_index_plus_filter(self):
        """
            The filters contain an indexed value.
            There is a secondary column constraint which 
            reduces the selection
        """
        pytest = self.dbs.get_file("PYTEST20", fieldnames=[
            'NAME', 'Textline_One', 'textline2'])

        cursor = pytest.query(filters=[['name', '>', 'A'], ['name', '<', 'Z'],
            ['textline_one', '>=', '3:'], ['textline_one', '<=', '6:']])
        result = list(cursor)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][1][0], 'ROW3')
        self.assertEqual(result[1][1][0], 'ROW4')
        self.assertEqual(result[2][1][0], 'ROW5')

        # Check the query plan
        plan = list(pytest.query(filters=[['name', '>', 'A'], ['name', '<', 'Z'],
            ['textline_one', '>=', '3:'], ['textline_one', '<=', '6:']], explain=True))
        self.assertEqual(len(plan), 2)

        self.assertEqual(plan[0].find("index_order_traversal"), 0)
        self.assertNotEquals(plan[0].find("ascending=True"), -1)
        self.assertNotEquals(plan[0].find("X > 'A' AND X < 'Z'"), -1)
        self.assertNotEquals(plan[0].find("index=B"), -1)

        self.assertEquals(plan[1].find("apply_filters filters"), 0)
        self.assertNotEquals(plan[1].find("['textline_one', '>=', '3:'], ['textline_one', '<=', '6:']"), -1)

    def test_index_in(self):
        """
            right now this gives a non-indexed traversal. In reality,
            it should create a third type of cursor, for multi-set retrieval.
        """
        pytest = self.dbs.get_file("PYTEST20", fieldnames=[
            'NAME', 'Textline_One', 'textline2'])

        cursor = pytest.query(filters=[['name', 'in', ['ROW3', 'ROW4', 'ROW5']]])
        result = list(cursor)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][1][0], 'ROW3')
        self.assertEqual(result[1][1][0], 'ROW4')
        self.assertEqual(result[2][1][0], 'ROW5')

        # Check the query plan
        plan = list(pytest.query(filters=[['name', 'in', ['ROW3', 'ROW4', 'ROW5']]], explain=True))
        self.assertEqual(plan[0].find("file_order_traversal"), 0)
        self.assertNotEquals(plan[0].find("ascending=True"), -1)
        self.assertNotEquals(plan[0].find("X None None AND X None None"), -1)
        self.assertEquals(plan[1].find("apply_filters"), 0)
        self.assertNotEquals(plan[1].find("filters = [['name', 'in', ['ROW3', 'ROW4', 'ROW5']]]"), -1)

    def test_subfile(self):
        """
            This is pulling county information from a state.
            TODO: build a test file.
        """
        pytest = self.dbs.get_file("5.01", fieldnames=["county", "abbreviation", "va_county_code", "catchment_code", "inactive_date"])

        plan = list(pytest.query(order_by = [["_rowid1", "ASC"], ["_rowid", "ASC"]],
                filters = [["_rowid1", "=", "6"]], explain=True))
        print plan
        result = pytest.query(order_by = [["_rowid1", "ASC"], ["_rowid", "ASC"]],
                filters = [["_rowid1", "=", "6"]])
        result = list(result)

test_cases = (TestDjango, )

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
