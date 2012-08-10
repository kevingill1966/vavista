
# For the pointer to a file value, we should see a conspicuous
# difference between internal and external formats

# TODO: How to validate that the referenced value exists
# TODO: the target file header (DD) has a field (PT) that
#       indicates that an inbound pointer exists. Can this
#       be used to prevent deletes?

# TODO: Not insert logic

import unittest

from vavista.fileman import connect, transaction, FilemanError
from vavista.M import Globals

class TestPointer(unittest.TestCase):
    """
        Create a simple file containing 2 pointer fields.
        One is created without laygo, the other has laygo
    """

    DIC = [
        ('^DIC(9999915,0)', u'PYTEST9A^9999915'),
        ('^DIC(9999915,0,"AUDIT")', '@'),
        ('^DIC(9999915,0,"DD")', '@'),
        ('^DIC(9999915,0,"DEL")', '@'),
        ('^DIC(9999915,0,"GL")', '^DIZ(9999915,'),
        ('^DIC(9999915,0,"LAYGO")', '@'),
        ('^DIC(9999915,0,"RD")', '@'),
        ('^DIC(9999915,0,"WR")', '@'),
        ('^DIC(9999915,"%A")', '10000000020^3120806'),
        ('^DIC("B","PYTEST9A",9999915)', ''),
        ('^DIC(9999916,0)', u'PYTEST9B^9999916'),
        ('^DIC(9999916,0,"AUDIT")', '@'),
        ('^DIC(9999916,0,"DD")', '@'),
        ('^DIC(9999916,0,"DEL")', '@'),
        ('^DIC(9999916,0,"GL")', '^DIZ(9999916,'),
        ('^DIC(9999916,0,"LAYGO")', '@'),
        ('^DIC(9999916,0,"RD")', '@'),
        ('^DIC(9999916,0,"WR")', '@'),
        ('^DIC(9999916,"%A")', '10000000020^3120806'),
        ('^DIC("B","PYTEST9B",9999916)', ''),
    ]

    DIZ = [
        ('^DIZ(9999915,0)', 'PYTEST9A^9999915^6^6'),
        ('^DIZ(9999915,1,0)', 'ONE^1'),
        ('^DIZ(9999915,2,0)', 'TWO^2'),
        ('^DIZ(9999915,3,0)', 'THREE^3'),
        ('^DIZ(9999915,4,0)', 'TEN^10'),
        ('^DIZ(9999915,5,0)', 'NINE^9'),
        ('^DIZ(9999915,6,0)', 'EIGHT^8'),
        ('^DIZ(9999915,"B","EIGHT",6)', ''),
        ('^DIZ(9999915,"B","NINE",5)', ''),
        ('^DIZ(9999915,"B","ONE",1)', ''),
        ('^DIZ(9999915,"B","TEN",4)', ''),
        ('^DIZ(9999915,"B","THREE",3)', ''),
        ('^DIZ(9999915,"B","TWO",2)', ''),
        ('^DIZ(9999916,0)', 'PYTEST9B^9999916^6^6'),
        ('^DIZ(9999916,1,0)', 'ONE^1^1'),
        ('^DIZ(9999916,2,0)', 'TWO^2^2'),
        ('^DIZ(9999916,3,0)', 'THREE^3^3'),
        ('^DIZ(9999916,4,0)', 'EIGHT^6^6'),
        ('^DIZ(9999916,5,0)', 'NINE^5^5'),
        ('^DIZ(9999916,6,0)', 'TEN^4^4'),
        ('^DIZ(9999916,"B","EIGHT",4)', ''),
        ('^DIZ(9999916,"B","NINE",5)', ''),
        ('^DIZ(9999916,"B","ONE",1)', ''),
        ('^DIZ(9999916,"B","TEN",6)', ''),
        ('^DIZ(9999916,"B","THREE",3)', ''),
        ('^DIZ(9999916,"B","TWO",2)', ''),
    ]

    DD = [
        ('^DD(9999915,0)', u'FIELD^^1^2'),
        ('^DD(9999915,0,"DT")', '3120806'),
        ('^DD(9999915,0,"IX","B",9999915,.01)', ''),
        ('^DD(9999915,0,"NM","PYTEST9A")', ''),
        ('^DD(9999915,0,"PT",9999916,1)', ''),
        ('^DD(9999915,0,"PT",9999916,2)', ''),
        ('^DD(9999915,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!($L(X)<1)!'(X'?1P.E) X"),
        ('^DD(9999915,.01,1,0)', '^.1'),
        ('^DD(9999915,.01,1,1,0)', '9999915^B'),
        ('^DD(9999915,.01,1,1,1)', 'S ^DIZ(9999915,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999915,.01,1,1,2)', 'K ^DIZ(9999915,"B",$E(X,1,30),DA)'),
        ('^DD(9999915,.01,3)', 'Answer must be 1-30 characters in length.'),
        ('^DD(9999915,.01,"DT")', '3120806'),
        ('^DD(9999915,1,0)', 'Value^NJ2,0^^0;2^K:+X\'=X!(X>10)!(X<0)!(X?.E1"."1N.N) X'),
        ('^DD(9999915,1,3)', 'Type a Number between 0 and 10, 0 Decimal Digits'),
        ('^DD(9999915,1,"DT")', '3120806'),
        ('^DD(9999915,"B","NAME",.01)', ''),
        ('^DD(9999915,"B","Value",1)', ''),
        ('^DD(9999915,"GL",0,1,.01)', ''),
        ('^DD(9999915,"GL",0,2,1)', ''),
        ('^DD(9999915,"IX",.01)', ''),
        ('^DD(9999915,"RQ",.01)', ''),
        ('^DD(9999916,0)', u'FIELD^^2^3'),
        ('^DD(9999916,0,"DT")', '3120806'),
        ('^DD(9999916,0,"IX","B",9999916,.01)', ''),
        ('^DD(9999916,0,"NM","PYTEST9B")', ''),
        ('^DD(9999916,.01,0)', "NAME^RF^^0;1^K:$L(X)>30!(X?.N)!($L(X)<3)!'(X'?1P.E) X"),
        ('^DD(9999916,.01,1,0)', '^.1'),
        ('^DD(9999916,.01,1,1,0)', '9999916^B'),
        ('^DD(9999916,.01,1,1,1)', 'S ^DIZ(9999916,"B",$E(X,1,30),DA)=""'),
        ('^DD(9999916,.01,1,1,2)', 'K ^DIZ(9999916,"B",$E(X,1,30),DA)'),
        ('^DD(9999916,.01,3)', 'NAME MUST BE 3-30 CHARACTERS, NOT NUMERIC OR STARTING WITH PUNCTUATION'),
        ('^DD(9999916,1,0)', "p1^P9999915'^DIZ(9999915,^0;2^Q"),
        ('^DD(9999916,1,"DT")', '3120806'),
        ('^DD(9999916,2,0)', 'p2^P9999915^DIZ(9999915,^0;3^Q'),
        ('^DD(9999916,2,"DT")', '3120806'),
        ('^DD(9999916,"B","NAME",.01)', ''),
        ('^DD(9999916,"B","p1",1)', ''),
        ('^DD(9999916,"B","p2",2)', ''),
        ('^DD(9999916,"GL",0,1,.01)', ''),
        ('^DD(9999916,"GL",0,2,1)', ''),
        ('^DD(9999916,"GL",0,3,2)', ''),
        ('^DD(9999916,"IX",.01)', ''),
        ('^DD(9999916,"RQ",.01)', ''),
    ]

    IX = [
    ]


    def _createFile(self):
        # This creates a file
        transaction.begin()
        for filename in ["PYTEST9A", "PYTEST9B"]:
            g = Globals["^DIC"]["B"][filename]
            if len(g.keys()) != 0:
                sys.stderr.write("File already exists: %s\n" % filename)
                sys.exit(1)
        Globals.deserialise(self.DIC)
        Globals.deserialise(self.DD)
        Globals.deserialise(self.DIZ)
        Globals.deserialise(self.IX)
        transaction.commit()

    def _cleanupFile(self):
        # This deletes a file
        transaction.begin()
        Globals["^DIC"]["9999915"].kill()
        Globals["^DIC"]["B"]["PYTEST9A"].kill()
        Globals["^DD"]["9999915"].kill()
        Globals["^DIZ"]["9999915"].kill()
        Globals["^DIC"]["9999916"].kill()
        Globals["^DIC"]["B"]["PYTEST9B"].kill()
        Globals["^DD"]["9999916"].kill()
        Globals["^DIZ"]["9999916"].kill()
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

    def test_external(self):
        pytest = self.dbs.get_file("PYTEST9B", internal=False)

        # The low-level traverser, walks index "B", on NAME field
        cursor = pytest.traverser("B", " ")
        key, rowid = cursor.next()

        # retrieve the record
        rec = pytest.get(rowid)

        # validate the inserted data
        self.assertEqual(str(rec.NAME), "EIGHT")
        self.assertEqual(str(rec.P1), "EIGHT")
        self.assertEqual(str(rec.P2), "EIGHT")


    def test_internal(self):
        pytest = self.dbs.get_file("PYTEST9B", internal=True)

        # The low-level traverser, walks index "B", on NAME field
        cursor = pytest.traverser("B", " ")
        key, rowid = cursor.next()

        # retrieve the record
        rec = pytest.get(rowid)

        # validate the inserted data
        self.assertEqual(str(rec.NAME), "EIGHT")
        self.assertEqual(str(rec.P1), "6")
        self.assertEqual(str(rec.P2), "6")

    def test_traverse(self):
        pytest = self.dbs.get_file("PYTEST9B", internal=True)

        # The low-level traverser, walks index "B", on NAME field
        cursor = pytest.traverser("B", " ")
        key, rowid = cursor.next()

        # retrieve the record
        rec = pytest.get(rowid)

        # validate the inserted data
        self.assertEqual(str(rec.NAME), "EIGHT")
        self.assertEqual(str(rec.P1), "6")
        self.assertEqual(str(rec.P2), "6")

        # Traverse
        reference = rec.traverse("P1")
        self.assertEqual(str(reference.NAME), "EIGHT")
        self.assertEqual(str(reference.VALUE), "8")
        reference2 = rec.traverse("P2")
        self.assertEqual(str(reference2.NAME), "EIGHT")
        self.assertEqual(str(reference2.VALUE), "8")


test_cases = (TestPointer, )

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
