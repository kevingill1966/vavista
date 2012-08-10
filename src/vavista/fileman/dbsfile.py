"""
    DBSFile maps to a single file.

    File level features such as retrieve or create a row,
    index traversal should be implemented here.
"""

from vavista import M
from shared import FilemanError

from dbsrow import DBSRow

class IndexIterator:
    def __init__(self, gl_prefix, index, from_value=None, to_value=None, ascending=True, from_rule=">=", to_rule="<"):
        """
            An iterator which will traverse an index.
            The iterator should return (key, rowid) pairs.

            Indexes are stored:
                GLOBAL,INDEXID,VALUE,ROWID=""
            ^DIZ(999900,"B","hello there from unit test2",183)=""
            ^DIZ(999900,"B","hello there from unit test2",184)=""
            ^DIZ(999900,"B","hello there from unit test2",185)=""
            ^DIZ(999900,"B","record 1",1)=""

        """
        self.gl = gl_prefix + '"%s",' % index
        self.from_value = from_value
        self.to_value = to_value
        self.ascending = ascending
        self.from_rule = from_rule
        self.to_rule = to_rule

        if self.from_value != None and self.to_value != None:
            if self.ascending:
                assert(self.from_value <= self.to_value)
            else:
                assert(self.to_value <= self.from_value)
        
        if self.from_value is None:
            self.lastkey = ""
        else:
            self.lastkey = self.from_value
        self.lastrowid = ""

    def __iter__(self):
        return self

    def next(self):
        lastkey = self.lastkey
        lastrowid = self.lastrowid
        if self.ascending:
            asc = 1
        else:
            asc = -1

        # There is a mad collation approach in M, where numbers sort before non-numbers.
        # this really messes up the keys.
        # How should I search? 

        # There is an inefficiency here it takes three searches to find the next record.
        while 1:
            try:
                float(lastkey)
                lastkey_isnum = True
            except:
                lastkey_isnum = False

            if lastrowid is None:
                # locate the next matching index value
                lastkey, = M.mexec("""set s0=$order(%ss0),%s)""" % (self.gl, asc), M.INOUT(lastkey))
                if lastkey == "":
                    raise StopIteration

                if self.ascending:
                    if self.from_value is not None:
                        if self.from_rule == ">" and lastkey <= self.from_value:
                            continue
                        if self.from_rule == ">=" and lastkey < self.from_value:
                            assert 0
                    if self.to_value is not None:
                        if self.to_rule == "<=" and lastkey > self.to_value:
                            raise StopIteration
                        if self.to_rule == "=" and lastkey != self.to_value:
                            raise StopIteration
                        if self.to_rule == "<" and lastkey >= self.to_value:
                            raise StopIteration
                    self.lastkey = lastkey
                    lastrowid = "0"

                else: # descending
                    if self.from_value is not None:
                        if self.from_rule == "<" and lastkey >= self.from_value:
                            continue
                        if self.from_rule == "<=" and lastkey > self.from_value:
                            assert 0
                    if self.to_value is not None:
                        if self.to_rule == ">=" and lastkey < self.to_value:
                            raise StopIteration
                        if self.to_rule == "=" and lastkey != self.to_value:
                            raise StopIteration
                        if self.to_rule == ">" and lastkey <= self.to_value:
                            raise StopIteration
                    self.lastkey = lastkey
                    lastrowid = ""

            # Have the key, get the first matching rowid
            lastrowid, = M.mexec("""set s0=$order(%s"%s",s1),%d)""" % (self.gl, self.lastkey, asc),
                    M.INOUT(lastkey), lastrowid)
            if lastrowid == "":
                # No match
                lastrowid = None
                continue
            self.lastrowid = lastrowid
            return self.lastkey, self.lastrowid


class DBSFile(object):
    """
        This class provides mechanisms to return rows.
        Currently only "get" is provided which returns one row identified by an id.
        I want to provide search functionality in this class.
    """
    dd = None
    internal = True
    fieldids = None

    def __init__(self, dd, internal=True, fieldids=None):
        self.dd = dd
        self.internal = internal
        self.fieldids = fieldids
        assert (dd.fileid is not None)

    def __str__(self):
        return "DBSFILE %s (%s)" % (self.dd.filename, self.dd.fileid)

    def get(self, rowid):
        """
            The logic to retrieve and update the row is in the DBSRow class.
            This call constructs a DBSRow class, and verifies that
            the row exists in the database.
        """
        record = DBSRow(self, self.dd, rowid, fieldids=self.fieldids, internal=self.internal)
        record._retrieve() # raises exception on failure
        return record

    def traverser(self, index, from_value=None, to_value=None, ascending=True, from_rule=None, to_rule=None):
        """
            Return an iterator which will traverse an index.
            The iterator should return (key, rowid) pairs.

            By default match the from value but not the to value.
            In the case where the from value = to value, we want an 
            exact match only.
        """
        if ascending:
            if from_rule is None:
                if from_value and to_value and from_value == to_value:
                    from_rule = "="
                else:
                    from_rule = ">="
            else:
                assert from_rule in (">", ">=", "=")
            if to_rule is None:
                if from_value and to_value and from_value == to_value:
                    to_rule = "="
                else:
                    to_rule = "<"
            else:
                assert to_rule in ("<", "<=", "=")
        else:
            if from_rule is None:
                from_rule = "<="
            else:
                assert from_rule in ("<", "<=", "=")
            if to_rule is None:
                to_rule = ">"
            else:
                assert to_rule in (">", ">=", "=")
        gl_prefix = self.dd.m_open_form()
        return IndexIterator(gl_prefix, index, from_value, to_value, ascending, from_rule, to_rule)

    def new(self):
        """
            The logic to create a new row.
        """
        if not self.internal:
            raise FilemanError("You must use internal format to modify a file")
        record = DBSRow(self, self.dd, rowid=None, fieldids=self.fieldids, internal=True)
        return record
