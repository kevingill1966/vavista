"""
    DBSFile maps to a single file.

    File level features such as retrieve or create a row,
    index traversal should be implemented here.
"""

from vavista import M
from shared import FilemanError

from dbsrow import DBSRow

# TODO: offset and limit required here

class IndexIterator:
    def __init__(self, gl_prefix, index, from_value=None, to_value=None, ascending=True,
        from_rule=">=", to_rule="<", raw=False, getter=None, description=None, filters=None):
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
        self.raw = raw
        self.getter = getter
        self.description = description
        self.filters = filters

        if self.from_value != None and self.to_value != None:
            if self.ascending:
                assert(self.from_value <= self.to_value)
            else:
                assert(self.to_value <= self.from_value)
        
        if self.from_value is None:
            self.lastkey = " "
        else:
            self.lastkey = self.from_value
        self.lastrowid = ""

    def __iter__(self):
        return self

    @property
    def rowid(self):
        return self.lastrowid

    def next(self):
        lastkey = self.lastkey
        lastrowid = self.lastrowid
        if self.ascending:
            asc = 1
        else:
            asc = -1

        # TODO: Fileman seems to structure indexes with keys in the global path
        #       or in the value - need to investigate further

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
                lastkey, = M.mexec("""set s0=$order(%ss0),%s)""" % (self.gl, asc), M.INOUT(str(lastkey)))
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
                    M.INOUT(str(lastkey)), lastrowid)
            if lastrowid == "":
                # No match
                lastrowid = None
                continue

            if self.filters:
                # Are filters to be applied?
                if not self.filters(lastrowid):
                    continue

            self.lastrowid = lastrowid
            if self.raw:
                return self.lastkey, self.lastrowid
            return self.getter(self.lastrowid)

class RowIterator:
    def __init__(self, gl, from_rowid=None, to_rowid=None, ascending=True,
        from_rule=">=", to_rule="<", raw=False, getter=None, description=None, filters=None, limit=None, offset=None):
        """
            An iterator which will traverse a table
        """
        self.gl = gl
        self.from_rowid = from_rowid
        self.to_rowid = to_rowid
        self.ascending = ascending
        self.from_rule = from_rule
        self.to_rule = to_rule
        self.raw = raw
        self.getter = getter
        self.description = description
        self.filters = filters
        self.limit = limit
        self.offset = offset

        if self.from_rowid != None and self.to_rowid != None:
            if self.ascending:
                assert(self.from_rowid <= self.to_rowid)
            else:
                assert(self.to_rowid <= self.from_rowid)
        
        # TODO: full table descending - highest numeric value
        if self.from_rowid is None:
            self.lastrowid = 0
        else:
            self.lastrowid = int(self.from_rowid)

        if self.offset:
            self.skip_rows = int(self.offset)
        else:
            self.skip_rows = 0

        self.results_returned = 0
        if self.limit:
            self.limit = int(self.limit)

        self.first_pass = True

    def __iter__(self):
        return self

    @property
    def rowid(self):
        return self.lastrowid

    def next(self):

        # Have we exceeded limit
        if self.limit:
            if self.results_returned >= self.limit:
                raise StopIteration

        lastrowid = self.lastrowid
        if self.ascending:
            asc = 1
        else:
            asc = -1

        while 1:
            # If this is the first pass, we may have the id of a record, which needs to 
            # be verified
            found = False
            if self.first_pass:
                self.first_pass = False
                d, = M.mexec("""set l0=$data(%sl0))""" % (self.gl), M.INOUT(lastrowid))
                if d:
                    found = True
                    lastrowid = int(lastrowid)

            if not found:
                lastrowid, = M.mexec("""set s0=$order(%ss0),%d)""" % (self.gl, asc),
                        M.INOUT(str(lastrowid)))
                if lastrowid == "":
                    raise StopIteration
                try:
                    lastrowid = int(lastrowid)
                except ValueError:
                    # Once you go beyond numeric values, you are into indexes
                    raise StopIteration


            if self.ascending:
                if self.from_rowid is not None:
                    if lastrowid == self.from_rowid and self.from_rule == ">":
                        continue
                if self.to_rowid is not None:
                    if lastrowid >= self.to_rowid and self.to_rule == "<":
                        raise StopIteration
                    if lastrowid > self.to_rowid and self.to_rule == "<=":
                        raise StopIteration

            else: # descending:
                if self.from_rowid is not None:
                    if lastrowid == self.from_rowid and self.from_rule == "<":
                        continue
                if self.to_rowid is not None:
                    if lastrowid <= self.to_rowid and self.to_rule == ">":
                        raise StopIteration
                    if lastrowid < self.to_rowid and self.to_rule == ">=":
                        raise StopIteration

            if self.filters:
                # Are filters to be applied?
                if not self.filters(lastrowid):
                    continue

            if self.skip_rows > 0:
                self.skip_rows -= 1
                continue

            self.lastrowid = lastrowid

            self.results_returned += 1

            if self.raw:
                return self.lastrowid
            return self.getter(self.lastrowid)

class DBSFile(object):
    """
        This class provides mechanisms to return rows.
        Currently only "get" is provided which returns one row identified by an id.
        I want to provide search functionality in this class.

        The get method should return something approximating a result
        from dbapi.
    """
    dd = None
    internal = True
    fieldids = None
    _description = None
    _fieldnames = None

    def __init__(self, dd, internal=True, fieldids=None, fieldnames=None):
        self.dd = dd
        self.internal = internal
        if fieldnames:
            self.fieldids = [dd.attrs[n] for n in fieldnames]
            self._fieldnames = fieldnames
        else:
            self.fieldids = fieldids
            self._fieldnames = None

        assert (dd.fileid is not None)

    def __str__(self):
        return "DBSFILE %s (%s)" % (self.dd.filename, self.dd.fileid)

    def fieldnames(self):
        if self._fieldnames is None:
            # TODO: if only have fieldids, have to look them up
            return self.fieldids
        return self._fieldnames

    @property
    def description(self):
        """
            Describe the resultset. This is made by the DBSRow object.
        """
        if self._description is None:
            record = DBSRow(self, self.dd, None, fieldids=self.fieldids, internal=self.internal)
            self._description = record.description
        return self._description

    def get(self, rowid, asdict=False):
        """
            The logic to retrieve and update the row is in the DBSRow class.
            This call constructs a DBSRow class, and verifies that
            the row exists in the database.

            It returns sequence, as per the dbapi spec.

            Multiples are a problem. The multiple is returned as a nested
            sequence of sequences.
        """
        record = DBSRow(self, self.dd, rowid, fieldids=self.fieldids, internal=self.internal)
        record.retrieve() # raises exception on failure
        if asdict:
            return dict(zip(self.fieldnames(), record.as_list()))
        else:
            return record.as_list()

    def traverser(self, index, from_value=None, to_value=None, ascending=True, from_rule=None, to_rule=None, raw=False,
            filters=None, limit=None, offset=None):
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

        if filters:
            filter_function = lambda rowid: self.filter_row(rowid, filters)
        else:
            filter_function = None

        if index:
            return IndexIterator(gl_prefix, index, from_value, to_value, ascending,
                from_rule, to_rule, raw, getter=self.get, description=self.description,
                filters=filter_function)
        else:
            return RowIterator(gl_prefix, from_value, to_value, ascending,
                from_rule, to_rule, raw, getter=self.get, description=self.description,
                filters=filter_function, limit=limit, offset=offset)

    def filter_row(self, _rowid, filters):
        """
            Return true of false for whether rowid matches the set of filters,
            These are intended to handle the django filters.

            column > x
            column < x

            I need to switch to a 'raw' global retriever to get this working.
        """
        return True

    def update(self, _rowid, **kwargs):
        """
            Update a record. The kwargs are named parameters.
        """
        fieldnames=kwargs.keys()
        fieldnames.sort()

        values = dict([(self.dd.attrs[n], v) for (n, v) in kwargs.items()])
        record = DBSRow(self, self.dd, _rowid, internal=self.internal, fieldids=values.keys())
        record.update(values)

    def insert(self, **kwargs):
        """
            Insert a record. The kwargs are named parameters.
        """
        fieldnames=kwargs.keys()
        fieldnames.sort()

        values = dict([(self.dd.attrs[n], v) for (n, v) in kwargs.items()])
        record = DBSRow(self, self.dd, None, internal=self.internal, fieldids=values.keys())
        return record.insert(values)

    def traverse_pointer(self, fieldname, value, fieldnames=None):
        """
            Given a pointer, follow it to the next file.
        """
        handler = DBSRow(self, self.dd, None, internal=self.internal)
        return handler.traverse(fieldname, value, fieldnames=fieldnames)

    def lock(self, _rowid, timeout=5):
        record = DBSRow(self, self.dd, _rowid, internal=self.internal)
        return record.lock(timeout)

    def unlock(self, _rowid):
        record = DBSRow(self, self.dd, _rowid, internal=self.internal)
        return record.unlock()

    def delete(self, _rowid):
        record = DBSRow(self, self.dd, _rowid, internal=self.internal)
        return record.unlock()
