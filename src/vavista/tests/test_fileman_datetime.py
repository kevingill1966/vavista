

# Test the Fileman DBS interface

# TODO: Investigate new and old style indexes

import unittest
import datetime

from vavista.fileman import connect, transaction
from vavista.M import Globals

class TestDatetime(unittest.TestCase):
    """
        Create a simple file containing a couple of dates.
    """
    DIC = [
        ('^DIC(9999904,0)', 'PYTEST2^9999904'),
        ('^DIC(9999904,0,"AUDIT")', '@'),
        ('^DIC(9999904,0,"DD")', '@'),
        ('^DIC(9999904,0,"DEL")', '@'),
        ('^DIC(9999904,0,"GL")', '^DIZ(9999904,'),
        ('^DIC(9999904,0,"LAYGO")', '@'),
        ('^DIC(9999904,0,"RD")', '@'),
        ('^DIC(9999904,0,"WR")', '@'),
        ('^DIC(9999904,"%A")', '10000000020^3120719'),
        ('^DIC("B","PYTEST2",9999904)', ''),
    ]

    DIZ = [
        ('^DIZ(9999904,0)', 'PYTEST2^9999904^1^1'),
        ('^DIZ(9999904,1,0)', 'fileman^3120720^3120720.110102^3120720.110102'),
        ('^DIZ(9999904,"B","fileman",1)', ''),
        ('^DIZ(9999904,"C",3120720.110102,12)', ''),
    ]

    DD = [
        ('^DD(9999904,0)' , 'FIELD^^3^4'),
        ('^DD(9999904,0,"DT")' , '3120719'),
        ('^DD(9999904,0,"IX","B",9999904,.01)', ''),
        ('^DD(9999904,0,"NM","PYTEST2")', ''),
        ('^DD(9999904,.01,0)', '''NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X'''),
        ('^DD(9999904,.01,1,0)', '^.1'),
        ('^DD(9999904,.01,1,1,0)', '9999904^B'),
        ('^DD(9999904,.01,1,1,1)', 'S ^DIZ(9999904,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999904,.01,1,1,2)', 'K ^DIZ(9999904,"B",$E(X,1,30),DA)'),
        ('^DD(9999904,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999904,1,0)', 'date1^D^^0;2^S %DT="E" D ^%DT S X=Y K:Y<1 X'),
        ('^DD(9999904,1,"DT")', '3120719'),
        ('^DD(9999904,2,0)', 'datetime1^D^^0;3^S %DT="EST" D ^%DT S X=Y K:Y<1 X'),
        ('^DD(9999904,2,"DT")', '3120719'),
        ('^DD(9999904,3,0)', 'datetime2^D^^0;4^S %DT="ESTX" D ^%DT S X=Y K:Y<1 X'),
        ('^DD(9999904,3,"DT")', '3120719'),
        ('^DD(9999904,"B","NAME",.01)', ''),
        ('^DD(9999904,"B","date1",1)', ''),
        ('^DD(9999904,"B","datetime1",2)', ''),
        ('^DD(9999904,"B","datetime2",3)', ''),
        ('^DD(9999904,"GL",0,1,.01)', ''),
        ('^DD(9999904,"GL",0,2,1)', ''),
        ('^DD(9999904,"GL",0,3,2)', ''),
        ('^DD(9999904,"GL",0,4,3)', ''),
        ('^DD(9999904,"IX",.01)', ''),
        ('^DD(9999904,"RQ",.01)', ''),
    ]

    IX = [
        ('^DD("IX",117,0)', '9999904^C^date field new style index^R^^F^IR^I^9999904^^^^^LS'),
        ('^DD("IX",117,1)', 'S ^DIZ(9999904,"C",X,DA)=""'),
        ('^DD("IX",117,2)', 'K ^DIZ(9999904,"C",X,DA)'),
        ('^DD("IX",117,2.5)', 'K ^DIZ(9999904,"C")'),
        ('^DD("IX",117,11.1,0)', '^.114IA^1^1'),
        ('^DD("IX",117,11.1,1,0)', '1^F^9999904^2^^1^F'),
        ('^DD("IX",117,11.1,1,3)', ''),
        ('^DD("IX",117,11.1,"AC",1,1)', ''),
        ('^DD("IX",117,11.1,"B",1,1)', ''),
        ('^DD("IX",117,11.1,"BB",1,1)', ''),
        ('^DD("IX","B",9999904,117)', ''),
        ('^DD("IX","AC",9999904,117)', ''),
        ('^DD("IX","BB",9999904,"C",117)', ''),
        ('^DD("IX","F",9999904,2,117,1)', ''),
        ('^DD("IX","IX","C",117)', ''),
    ]

    def _cleanupFile(self):
        transaction.begin()
        Globals["^DIC"]["9999904"].kill()
        Globals["^DIC"]['B']["PYTEST2"].kill()
        Globals["^DD"]["9999904"].kill()
        Globals["^DIZ"]["9999904"].kill()
        Globals["^DD"]["IX"]["117"].kill()
        Globals["^DD"]["IX"]["B"]["9999904"].kill()
        Globals["^DD"]["IX"]["BB"]["9999904"].kill()
        Globals["^DD"]["IX"]["AC"]["9999904"].kill()
        Globals["^DD"]["IX"]["IX"]["C"]["117"].kill()
        Globals["^DD"]["IX"]["F"]["9999904"].kill()
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
        dd = self.dbs.dd("PYTEST2")
        self.assertEqual(len(dd.indices), 1)
        self.assertEqual(len(dd.new_indices), 1)

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
        dd = self.dbs.dd("PYTEST2")
        self.assertEqual(dd.fileid, "9999904")

        pytest2 = self.dbs.get_file("PYTEST2", internal=True,
                fieldnames = ["DATE1", "DATETIME1", "DATETIME2"])
        cursor = pytest2.traverser("B", "e")
        rec = cursor.next()
        self.assertEquals(rec[0], datetime.date(2012,7,20))
        self.assertEquals(rec[1], datetime.datetime(2012,7,20,11,1,2))
        self.assertEquals(rec[2], datetime.datetime(2012,7,20,11,1,2))

        pytest2 = self.dbs.get_file("PYTEST2", internal=False,
                fieldnames = ["DATE1", "DATETIME1", "DATETIME2"])
        cursor = pytest2.traverser("B", "e")
        rec = cursor.next()
        self.assertEquals(rec[0], datetime.date(2012,7,20))
        self.assertEquals(rec[1], datetime.datetime(2012,7,20,11,1,2))
        self.assertEquals(rec[2], datetime.datetime(2012,7,20,11,1,2))


    def test_readwrite(self):
        """
            Verify that we can write dates and times and read them back.
            Check both internal and external formats.
        """
        int_pytest2 = self.dbs.get_file("PYTEST2",
                fieldnames = ["NAME", "DATE1", "DATETIME1", "DATETIME2"])
        transaction.begin()
        rowid = int_pytest2.insert(NAME='Test Internal Dates', DATE1=datetime.date(2012,1,2),
            DATETIME1=datetime.datetime(2012,1,2,3,4,5), DATETIME2=datetime.datetime(2012,1,2,3,4,5))
        transaction.commit()

        cursor = int_pytest2.traverser("B", "Test Internal Dates") 

        rec = cursor.next()
        self.assertEqual(str(rec[0]), "Test Internal Dates")
        self.assertEquals(rec[1], datetime.date(2012,1,2))
        self.assertEquals(rec[2], datetime.datetime(2012,1,2,3,4,5))
        self.assertEquals(rec[3], datetime.datetime(2012,1,2,3,4,5))

    def test_indexing(self):
        """
            TODO: walk a date index. Can I return the values as dates.
        """
test_cases = (TestDatetime,)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
