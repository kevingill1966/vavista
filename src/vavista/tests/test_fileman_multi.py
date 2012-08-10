
"""
    I have not worked out how the API is supposed to return sub-file items.

    THE API for SUBFILES

    In the "GETS^DIQ", pass "**" as the field list to include subfields.

    All subfile entries are returned , under the subfile fileid at the top level
    of the result set...


"""

import unittest

from vavista.fileman import connect, transaction, FilemanError
from vavista.M import Globals

class TestMulti(unittest.TestCase):
    """
        Create a simple file containing two text lines,
        one mandatory and one optional. Verify that the
        read and write functionality works.
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
        ('^DIZ(9999940,1,1,1,0)', '1'),               # subfile
        ('^DIZ(9999940,1,1,2,0)', '2'),               # subfile
        ('^DIZ(9999940,1,1,3,0)', '3'),               # subfile
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
        ('^DD(9999940.01,"B","T1",.01)', ''),
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

        # Are indices setup
        dd = self.dbs.dd("PYMULTI1")

    def setUp(self):
        self.dbs = connect("0", "")

        self._cleanupFile()
        self._createFile()

    def tearDown(self):
        # destroy the file
        if transaction.in_transaction:
            transaction.abort()
        self._cleanupFile()

    def test_traversal(self):
        """
            Load the record. Then get a cursor for the sub-file.
        """
        pymult = self.dbs.get_file("PYMULT1")

        cursor = pymult.traverser("B", " ")
        parent = cursor.next()

        # Given the parent record, now traverse the children
        cursor2 = parent.subfile_cursor("T1")
        print list(cursor2)


test_cases = (TestMulti, )

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
