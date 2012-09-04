"""
    DBSFile maps to a single file.

    File level features such as retrieve or create a row,
    index traversal should be implemented here.
"""
import logging

from vavista import M
from shared import FilemanError, valid_rowid

from dbsrow import DBSRow

logger = logging.getLogger(__file__)

class Sorter:
    """
        If we are sorting the result, create a temporary store with key, fileid.
        Only support a order_by columns with the same direction (i.e. ascending or descending).
    """
    values = None
    ascending = True
    fields = None
    _gl_cache = None

    def __init__(self, order_by, dd, cache = None):
        self.order_by = order_by
        self.dd = dd
        self.fields = fields = []
        for (fieldname, direction) in order_by:
            fieldid = dd.attrs[fieldname]
            field = dd.fields[fieldid]
            if direction == 'ASC':
                self.ascending = True
            else:
                self.ascending = False
            fields.append(field)
        self._gl = M.Globals.from_closed_form("%s)" % dd.m_open_form()[:-1])
        self.values = list()
        self._gl_cache = cache

    def __iter__(self):
        """
            return the rowids 
        """
        values = sorted(self.values)
        if not self.ascending:
            values = reversed(values)
        return iter(x[1] for x in values)

    def push(self, rowid):
        """
            Retrieve the sorting data.
        """
        rec = self._gl[rowid]
        key = [field.retrieve(rec, self._gl_cache) for field in self.fields]
        self.values.append((key, rowid))

class IndexIterator:
    results_complete = False
    results = None

    def __init__(self, gl_prefix, index, from_value=None, to_value=None, ascending=True,
        from_rule=">=", to_rule="<", raw=False, getter=None, description=None, filters=None,
        limit=None, offset=None, sorter=None):
        """
            An iterator which will traverse an index.
            The iterator should return (key, rowid) pairs.

            Indices are stored:
                GLOBAL,INDEXID,VALUE,ROWID=""
            ^DIZ(999900,"B","hello there from unit test2",183)=""
            ^DIZ(999900,"B","hello there from unit test2",184)=""
            ^DIZ(999900,"B","hello there from unit test2",185)=""
            ^DIZ(999900,"B","record 1",1)=""

        """
        self.index = index
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
        self.limit, self.offset = limit, offset
        self.sorter = sorter

        if self.from_value != None and self.to_value != None:
            if self.ascending:
                assert(self.from_value <= self.to_value)
            else:
                assert(self.to_value <= self.from_value)
        
        if self.from_value is None:
            if ascending:
                self.lastkey = " "
            else:
                self.lastkey = "ZZZZZZZZZZZZZZ"
        else:
            self.lastkey = self.from_value
        self.lastrowid = ""

        if self.offset:
            self.skip_rows = int(self.offset)
        else:
            self.skip_rows = 0

        if self.limit:
            self.limit = int(self.limit)

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

        # TODO: Fileman seems to structure indices with keys in the global path
        #       or in the value - need to investigate further

        # There is a mad collation approach in M, where numbers sort before non-numbers.
        # this really messes up the keys.
        # How should I search? 

        # There is an inefficiency here it takes three searches to find the next record.
        while not self.results_complete:
            if lastrowid is None:
                # locate the next matching index value
                lastkey, = M.mexec("""set s0=$order(%ss0),%s)""" % (self.gl, asc), M.INOUT(str(lastkey)))
                if lastkey == "":
                    break

                if self.ascending:
                    if self.from_value is not None:
                        if self.from_rule == ">" and lastkey <= self.from_value:
                            continue
                        if self.from_rule == ">=" and lastkey < self.from_value:
                            assert 0
                    if self.to_value is not None:
                        if self.to_rule == "<=" and lastkey > self.to_value:
                            break
                        if self.to_rule == "=" and lastkey != self.to_value:
                            break
                        if self.to_rule == "<" and lastkey >= self.to_value:
                            break
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
                            break
                        if self.to_rule == "=" and lastkey != self.to_value:
                            break
                        if self.to_rule == ">" and lastkey <= self.to_value:
                            break
                    self.lastkey = lastkey
                    lastrowid = ""

            # Have the key, get the first matching rowid
            lastrowid, = M.mexec("""set s0=$order(%s"%s",s1),%d)""" % (self.gl, self.lastkey, asc),
                    M.INOUT(str(lastkey)), lastrowid)
            if lastrowid == "":
                # No match
                lastrowid = None
                continue

            if self.sorter:
                self.sorter.push(lastrowid)
            else:
                if self.filters:
                    # Are filters to be applied?
                    if not self.filters(lastrowid):
                        continue

                if self.skip_rows > 0:
                    self.skip_rows -= 1
                    continue

                self.lastrowid = lastrowid
                if self.raw:
                    return self.lastkey, self.lastrowid
                return self.getter(self.lastrowid)

        self.results_complete = True

        if self.sorter:
            if self.results is None:
                self.results = iter(self.sorter)

            while 1:
                lastrowid = self.results.next()
                if self.filters:
                    # Are filters to be applied?
                    if not self.filters(lastrowid):
                        continue
                if self.skip_rows > 0:
                    self.skip_rows -= 1
                    continue
                self.lastrowid = lastrowid
                if self.raw:
                    return self.lastkey, self.lastrowid
                return self.getter(self.lastrowid)

        raise StopIteration

class RowIterator:
    results_complete = False
    results = None

    def __init__(self, gl, from_rowid=None, to_rowid=None, ascending=True,
        from_rule=">=", to_rule="<", raw=False, getter=None, description=None,
        filters=None, limit=None, offset=None, sorter=None):
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
        self.sorter = sorter

        # the new person file has non-integer user ids
        if self.from_rowid != None:
            self.from_rowid = float(self.from_rowid)
        if self.to_rowid != None:
            self.to_rowid = float(self.to_rowid)

        if self.from_rowid != None and self.to_rowid != None:
            if self.ascending:
                assert(self.from_rowid <= self.to_rowid)
            else:
                assert(self.to_rowid <= self.from_rowid)
        
        # TODO: full table descending - highest numeric value
        if self.from_rowid is None:
            self.lastrowid = "0"
        else:
            self.lastrowid = ('%f' % self.from_rowid).rstrip('0').rstrip('.').lstrip('0') 
            if self.from_rowid > 0 and self.lastrowid[0] == "0":
                self.lastrowid = self.lastrowid[1:]
            if self.lastrowid.endswith(".0"):
                self.lastrowid = self.lastrowid[:-2]

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
        if not self.sorter and self.limit:
            if self.results_returned >= self.limit:
                raise StopIteration

        lastrowid = self.lastrowid    # This value should be a string throughout. 
        if self.ascending:
            asc = 1
        else:
            asc = -1

        while not self.results_complete:
            # If this is the first pass, we may have the id of a record, which needs to 
            # be verified
            found = False
            if self.first_pass:
                self.first_pass = False
                if lastrowid and float(lastrowid) > 0:
                    row_exists, = M.mexec("""set s0=$data(%ss0))""" % (self.gl), M.INOUT(lastrowid))
                    if int(row_exists):
                        found = True

            if not found:
                lastrowid, = M.mexec("""set s0=$order(%ss0),%d)""" % (self.gl, asc), M.INOUT(lastrowid))
                if not valid_rowid(lastrowid):
                    break

            # Check boundary values
            f_lastrowid = float(lastrowid)
            if self.ascending:
                if self.from_rowid is not None:
                    if f_lastrowid == self.from_rowid and self.from_rule == ">":
                        continue
                if self.to_rowid is not None:
                    if f_lastrowid >= self.to_rowid and self.to_rule == "<":
                        break
                    if f_lastrowid > self.to_rowid and self.to_rule == "<=":
                        break
            else: # descending:
                if self.from_rowid is not None:
                    if f_lastrowid == self.from_rowid and self.from_rule == "<":
                        continue
                if self.to_rowid is not None:
                    if f_lastrowid <= self.to_rowid and self.to_rule == ">":
                        break
                    if f_lastrowid < self.to_rowid and self.to_rule == ">=":
                        break

            if self.sorter:
                self.sorter.push(lastrowid)
            else:
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

        self.results_complete = True

        if self.sorter:
            if self.results is None:
                self.results = iter(self.sorter)
            while 1:
                lastrowid = self.results.next()
                if self.limit:
                    if self.results_returned >= self.limit:
                        break
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

        raise StopIteration

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
    _fm_description = None
    _fieldnames = None
    _field_cache = None
    _gl_cache = None

    def __init__(self, dd, internal=True, fieldids=None, fieldnames=None):
        self.dd = dd
        self.internal = internal
        if fieldnames:
            self.fieldids = [dd.attrs[n.lower()] for n in fieldnames]
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

    @property
    def fm_description(self):
        """
            Feturn full description of the table'ish object.
            Every thing that the data-dictionary has.
            The client can try to sort it out.
        """
        if self._fm_description is None:
            self._fm_description = self.dd.describe(fieldids = self.fieldids)
        return self._fm_description

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
        if self.internal:
            record.raw_retrieve(self._gl_cache)
        else:
            record.retrieve()
        if asdict:
            return dict(zip(self.fieldnames(), record.as_list()))
        else:
            return record.as_list()

    def _index_select(self, filters, order_by):
        """
            Given the filters, can we use an index

            returns: filters, index, from_value, to_value, from_rule, to_rule, ascending, pre_sorted

        """
        ascending = True

        # Odd case - if there is no filters, but there is an order by,
        #            find an index for the order_by
        #            format of order_by: "order_by": [["_rowid", "ASC"]

        if not filters and order_by:
            assert(len(order_by) == 1)  # TODO: support more
            if order_by[0][0] == '_rowid':
                if order_by[0][1] == 'ASC':
                    return None # default
                else:
                    return (filters, None, None, None, None, None, False, True)

            ascending = (order_by[0][1] == 'ASC')
            order_col = order_by[0][0]
            order_fieldid = self.dd.attrs[order_col]
            for index in self.dd.indices:
                if index.table != self.dd.fileid:   # indexes can be on embedded models
                    continue
                if len(index.columns) == 1 and index.columns[0] == order_fieldid:
                    return (filters, index.name, None, None, None, None, ascending, True)

        if not filters:
            return None

        # 1. Identify the sargable columns
        sargable = {}
        for colname, comparator, value in filters:
            if comparator in ["<", "<=", "=", ">=", ">"]:
                # TODO in, like with leading const.
                # TODO - ensure rhs is a constant
                if colname not in sargable.keys():
                    if colname == '_rowid':
                        sargable[colname] = None
                    else:
                        sargable[colname] = []
            elif comparator in ["in"] and len(value) == 1:
                if colname not in sargable.keys():
                    if colname == '_rowid':
                        sargable[colname] = None
                    else:
                        sargable[colname] = []

        if not sargable:
            return None

        sargable_fields = sargable.keys()

        # 2. find columns with indexes
        #    choose the preferred index (sargable + orderable, fileorder, first index)
        if len(sargable_fields) == 1 and sargable_fields[0] == '_rowid':
            # direct record retrieve - indexes not necessary
            colname = "_rowid"
            index = None
        else:
            # There is a mis-match here colnames versus fieldids
            sargable_fieldids = dict([(self.dd.attrs[colname], colname) for colname in sargable_fields])
            for index in self.dd.indices:
                if index.table != self.dd.fileid:   # indexes can be on embedded models
                    continue
                if len(index.columns) == 1 and index.columns[0] in sargable_fieldids.keys():
                    colname = sargable_fieldids[index.columns[0]]
                    sargable[colname].append(index.name)

            unindexed = [k for k,v in sargable.items() if len(v) == 0 and k != '_rowid']
            for k in unindexed:
                del sargable[k]

            if len(sargable) == 0:
                return None  # no index

            if len(sargable) == 1:
                colname = sargable.keys()[0]
                index = sargable[colname][0]
            else:
                # More than one option. How to choose the best?
                # result order, file order, other?
                # or '=' has precendence over range?

                # Choose first for now
                colname = sargable.keys()[0]
                index = sargable[colname][0]

        # At this point we have choosen an index. Have to choose the 
        # traversal rules, and remove the index from the filters
        index_filters = [rule for rule in filters if rule[0] == colname]
        filters = [rule for rule in filters if rule[0] != colname]

        # 3.  Make the traversal rules (upper/lower bounds)
        from_value, from_rule, to_value, to_rule = None, None, None, None

        for colname, comparator, value in index_filters:
            if comparator in [">", ">=", "=", 'in']:
                # TODO: comparason based on mumps rules, not python rules
                if from_value is None or from_value < value:
                    if comparator == 'in':
                        assert(len(value) == 1)
                        from_value = value[0]
                    else:
                        from_value = value
                    if comparator in ["=", 'in']:
                        from_rule = ">="
                    else:
                        from_rule = comparator
                elif from_value == value:
                    if from_rule in [">="] and comparator in [">"]:
                        from_rule = comparator
            if comparator in ["<", "<=", "=", 'in']:
                # TODO: comparason based on mumps rules, not python rules
                if to_value is None or to_value > value:
                    if comparator == 'in':
                        assert(len(value) == 1)
                        to_value = value[0]
                    else:
                        to_value = value
                    if comparator in ["=", 'in']:
                        to_rule = "<="
                    else:
                        to_rule = comparator
                elif to_value == value:
                    if to_rule in ["<="] and comparator in ["<"]:
                        to_rule = comparator

        if order_by:
            return (filters, index, from_value, to_value, from_rule, to_rule, ascending, False)
        else:
            return (filters, index, from_value, to_value, from_rule, to_rule, ascending, True)

    def traverser(self, index=None, from_value=None, to_value=None, ascending=True, from_rule=None, to_rule=None, raw=False,
            filters=None, limit=None, offset=None, order_by=None):
        """
            Return an iterator which will traverse an index.
            The iterator should return (key, rowid) pairs.

            By default match the from value but not the to value.
            In the case where the from value = to value, we want an 
            exact match only.
        """
        pre_sorted = False
        if index is None and from_value == None and to_value == None:
            # Index is not specified, but we may have filters - look at the filters
            # to see if we can select an index using them.
            rv = self._index_select(filters, order_by)
            if rv:
                filters, index, from_value, to_value, from_rule, to_rule, ascending, pre_sorted = rv
                logger.debug("_index_select return: filters %s, index %s, from_value [%s], to_value [%s], from_rule [%s], to_rule [%s], ascending [%s], pre_sorted [%s]",
                filters, index, from_value, to_value, from_rule, to_rule, ascending, pre_sorted)

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
        self._gl_cache = {}

        self._field_cache = {}
        if filters:
            filter_function = lambda rowid: self.filter_row(rowid, filters)
        else:
            filter_function = None

        # TODO: order_by - will work if I can find an index. However, I cannot find
        #                  an index, I have to extract the sorting columns and do a
        #                  filesort.

        if order_by and not pre_sorted:
            sorter = Sorter(order_by, self.dd, self._field_cache)
        else:
            sorter = None
        if index:
            return IndexIterator(gl_prefix, index, from_value, to_value, ascending,
                from_rule, to_rule, raw, getter=self.get, description=self.description,
                filters=filter_function, limit=limit, offset=offset, sorter=sorter)
        else:
            return RowIterator(gl_prefix, from_value, to_value, ascending,
                from_rule, to_rule, raw, getter=self.get, description=self.description,
                filters=filter_function, limit=limit, offset=offset, sorter=sorter)

    def _dd_field_byname(self, colname):
        """ Cached lookup of data-dictionary """
        field = self._field_cache.get(colname, None)
        if field is None:
            fieldid = self.dd.attrs[colname]
            self._field_cache[colname] = field = self.dd.fields[fieldid]
        return field

    def filter_row(self, _rowid, filters):
        """
            Return true of false for whether rowid matches the set of filters,
            These are intended to handle the django filters.

            column > x
            column < x
        """
        rec = M.Globals.from_closed_form("%s%s)"%(self.dd.m_open_form(), _rowid))
        for colname, comparator, value in filters:
            field = self._dd_field_byname(colname)
            db_value = field.retrieve(rec, self._gl_cache)

            if comparator == '='  and not (db_value == value):
                return False
            if comparator == '>=' and not (db_value >= value):
                return False
            if comparator == '>'  and not (db_value > value):
                return False
            if comparator == '<'  and not (db_value < value):
                return False
            if comparator == '<=' and not (db_value <= value):
                return False
            if comparator == 'in' and not (db_value in value):
                return False
            # TODO: move the comparator logic into the data-dictionary.
            #       logic will depend of field types.

        return True

    def update(self, _rowid, **kwargs):
        """
            Update a record. The kwargs are named parameters.
        """
        fieldnames=kwargs.keys()
        fieldnames.sort()

        values = dict([(self.dd.attrs[n.lower()], v) for (n, v) in kwargs.items()])
        record = DBSRow(self, self.dd, _rowid, internal=self.internal, fieldids=values.keys())
        record.update(values)

    def insert(self, **kwargs):
        """
            Insert a record. The kwargs are named parameters.
        """
        fieldnames=kwargs.keys()
        fieldnames.sort()

        # TODO: pass in primary key - if the client passes it, I am ignoring it.
        values = dict([(self.dd.attrs[n.lower()], v) for (n, v) in kwargs.items() if n != '_rowid'])
        record = DBSRow(self, self.dd, None, internal=self.internal, fieldids=values.keys())
        try:
            return record.insert(values)
        except:
            #import pdb; pdb.post_mortem()
            raise

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

    def _file_header(self):
        """
            Extract the file header
        """
        gl_prefix = self.dd.m_open_form() + "0"
        ns, path = gl_prefix.split("(")
        g = M.Globals[ns]
        for part in path.split(","):
            g = g[part]
        return g.value

    def count(self, limit=None):
        """
            For now, assume that there are no filters, selects etc.
        """
        header = self._file_header()
        parts = header.split("^")
        if len(parts) > 3:
            if parts[3]:
                return int(parts[3])
        return 0
