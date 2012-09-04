
"""
    Where the subfile contains one column only, it is a list.
    Where the subfile contains multiple columns, it is an embedded schema.

    I can force the result to be a list if I give P->C as the
    column name, where P is the parent id and C is child id.
"""

import unittest

from vavista.fileman import connect, transaction
from vavista.M import Globals

class TestMulti(unittest.TestCase):
    """
    """
    DIC = [
        ('^DIC(9999940,0)', u'PYMULT1^9999940'),
        ('^DIC(9999940,0,"AUDIT")', '@'),
        ('^DIC(9999940,0,"DD")', '@'),
        ('^DIC(9999940,0,"DEL")', '@'),
        ('^DIC(9999940,0,"GL")', '^DIZ(9999940,'),
        ('^DIC(9999940,0,"LAYGO")', '@'),
        ('^DIC(9999940,0,"RD")', '@'),
        ('^DIC(9999940,0,"WR")', '@'),
        ('^DIC(9999940,"%A")', '10000000020^3120810'),
        ('^DIC("B","PYMULT1",9999940)', ''),
    ]

    DIZ = [
        ('^DIZ(9999940,0)', 'PYMULT1^9999940^1^1'),
        ('^DIZ(9999940,1,0)', 'ONE'),
        ('^DIZ(9999940,1,1,0)', '^9999940.01^3^3'),   # subfile
        ('^DIZ(9999940,1,1,1,0)', '1^a'),               # subfile
        ('^DIZ(9999940,1,1,2,0)', '2^b'),               # subfile
        ('^DIZ(9999940,1,1,3,0)', '3^c'),               # subfile
        ('^DIZ(9999940,1,1,"B",1,1)', ''),            # subfile index
        ('^DIZ(9999940,1,1,"B",2,2)', ''),            # subfile index
        ('^DIZ(9999940,1,1,"B",3,3)', ''),            # subfile index
        ('^DIZ(9999940,"B","ONE",1)', ''),

    ]

    DD = [
        ('^DD(9999940,0)', u'FIELD^^1^2'),
        ('^DD(9999940,0,"DT")', '3120810'),
        ('^DD(9999940,0,"IX","B",9999940,.01)', ''),
        ('^DD(9999940,0,"NM","PYMULT1")', ''),
        ('^DD(9999940,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X"),
        ('^DD(9999940,.01,1,0)', '^.1'),
        ('^DD(9999940,.01,1,1,0)', '9999940^B'),
        ('^DD(9999940,.01,1,1,1)', 'S ^DIZ(9999940,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999940,.01,1,1,2)', 'K ^DIZ(9999940,"B",$E(X,1,30),DA)'),
        ('^DD(9999940,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999940,1,0)', 'T1^9999940.01^^1;0'),
        ('^DD(9999940,"B","NAME",.01)', ''),
        ('^DD(9999940,"B","T1",1)', ''),
        ('^DD(9999940,"GL",0,1,.01)', ''),
        ('^DD(9999940,"GL",1,0,1)', ''),
        ('^DD(9999940,"IX",.01)', ''),
        ('^DD(9999940,"RQ",.01)', ''),
        ('^DD(9999940,"SB",9999940.01,1)', ''),
        ('^DD(9999940.01,0)', u'T1 SUB-FIELD^^.01^1'),
        ('^DD(9999940.01,0,"DT")', '3120810'),
        ('^DD(9999940.01,0,"IX","B",9999940.01,.01)', ''),
        ('^DD(9999940.01,0,"NM","T1")', ''),
        ('^DD(9999940.01,0,"UP")', '9999940'),
        ('^DD(9999940.01,.01,0)', 'T1^MF^^0;1^K:$L(X)>10!($L(X)<1) X'),
        ('^DD(9999940.01,.01,1,0)', '^.1'),
        ('^DD(9999940.01,.01,1,1,0)', '9999940.01^B'),
        ('^DD(9999940.01,.01,1,1,1)', 'S ^DIZ(9999940,DA(1),1,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999940.01,.01,1,1,2)', 'K ^DIZ(9999940,DA(1),1,"B",$E(X,1,30),DA)'),
        ('^DD(9999940.01,.01,3)', 'Answer must be 1-10 characters in length.'),
        ('^DD(9999940.01,.01,"DT")', '3120810'),

        ('^DD(9999940.01,1,0)', 'T2^MF^^0;2^K:$L(X)>10!($L(X)<1) X'),
        ('^DD(9999940.01,1,1,0)', '^.1'),
        ('^DD(9999940.01,1,1,1,0)', '9999940.01^B'),
        ('^DD(9999940.01,1,1,1,1)', 'S ^DIZ(9999940,DA(1),1,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999940.01,1,1,1,2)', 'K ^DIZ(9999940,DA(1),1,"B",$E(X,1,30),DA)'),
        ('^DD(9999940.01,1,3)', 'Answer must be 1-10 characters in length.'),
        ('^DD(9999940.01,1,"DT")', '3120810'),

        ('^DD(9999940.01,"B","T1",.01)', ''),
        ('^DD(9999940.01,"B","T2",1)', ''),
        ('^DD(9999940.01,"GL",0,1,.01)', ''),
        ('^DD(9999940.01,"IX",.01)', ''),

    ]

    IX = [
    ]

    def _cleanupFile(self):
        transaction.begin()
        Globals["^DIC"]["9999940"].kill()
        Globals["^DIC"]["B"]["PYMULT1"].kill()
        Globals["^DD"]["9999940"].kill()
        Globals["^DIZ"]["9999940"].kill()
        Globals["^DD"]["9999940.01"].kill()
        transaction.commit()

    def _createFile(self):
        # This creates a file
        transaction.begin()
        Globals.deserialise(self.DIC)
        Globals.deserialise(self.DD)
        Globals.deserialise(self.DIZ)
        Globals.deserialise(self.IX)
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

    def test_embedded_schema(self):
        pymult = self.dbs.get_file("PYMULT1", fieldnames=['T1'])
        cursor = pymult.traverser("B", " ")
        record = cursor.next()

        self.assertEquals(len(record[0]), 3)
        self.assertEquals(record[0][0]['t1'], "1")
        self.assertEquals(record[0][0]['t2'], "a")
        self.assertEquals(record[0][1]['t1'], "2")
        self.assertEquals(record[0][1]['t2'], "b")
        self.assertEquals(record[0][2]['t1'], "3")
        self.assertEquals(record[0][2]['t2'], "c")

    def test_as_list(self):
        """
            Load the record. Then get a cursor for the sub-file.
        """
        pymult = self.dbs.get_file("PYMULT1", fieldnames=['T1->T1', "T1->T2"])

        cursor = pymult.traverser("B", " ")
        parent = cursor.next()

        # Given the parent record, now traverse the children
        self.assertEquals(parent[0][0], "1")
        self.assertEquals(parent[0][1], "2")
        self.assertEquals(parent[0][2], "3")

        self.assertEquals(parent[1][0], "a")
        self.assertEquals(parent[1][1], "b")
        self.assertEquals(parent[1][2], "c")

        # Verify update of a subfile element
        transaction.begin()
        pymult.update(cursor.rowid, **{'T1->T1': ["A","B","C"]})
        transaction.commit()

        transaction.begin()
        cursor = pymult.traverser("B", " ")
        parent = cursor.next()
        self.assertEquals(parent[0][0], "A")
        self.assertEquals(parent[0][1], "B")
        self.assertEquals(parent[0][2], "C")

        # Insert a subfile element
        pymult.update(cursor.rowid, **{'T1->T1': ["A","B","C","D"],
                'T1->T2': ["AA","BB","CC","DD"]})
        transaction.commit()

        transaction.begin()
        cursor = pymult.traverser("B", " ")
        parent = cursor.next()
        self.assertEquals(len(parent[0]), 4)
        self.assertEquals(parent[0][-1], "D")
        self.assertEquals(parent[1][-1], "DD")

        # delete an element
        # TODO: this functionality is not currently working.
        pymult.update(cursor.rowid, **{'T1->T1': ["A","B"],
                'T1->T2': ["AA","BB"]})
        transaction.commit()

        cursor = pymult.traverser("B", " ")
        parent = cursor.next()
        self.assertEquals(len(parent[0]), 2)
        self.assertEquals(parent[0][-1], "B")

test_cases = (TestMulti, )

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
