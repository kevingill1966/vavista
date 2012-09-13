"""
    This is the query planner used by the Django clients.

    The planner should return generator object, which is used to
    suck results out. Depending on the setup, the generator
    can be used in conjunction with a result set generator,
    and aggregator or a simple count call.

    TODO: Explain

    A query planner takes the SQL statement and coverts it to
    a tree. The implementation here does not support joins or
    unions or ors so the tree becomes a pipeline.

    terminology:

        relation - this is the table
        tuple - this is the row

    the optmiser must look at all the filters and choose a 
    strategy for pulling data from the storage, i.e. choose the
    best index. 

    the data is fed through a pipeline to apply all of the 
    appropriate filters to it. The output is sorted.

    This code does not handle functions. It is built as required
    rather than trying to build an optimiser.

"""

import logging

from vavista import M
from shared import valid_rowid

logger = logging.getLogger(__file__)

def _reverse_sign(s):
    "used for logging"
    if s == '>=': return '<='
    if s == '<=': return '>='
    if s == '>': return '<'
    if s == '<': return '>'
    return s

#------------------------------------------------------------------------------------------------
# The generators that implement the pipeline
# These pass the rowid and the global root of the row downwards.

def offset_limit(stream, limit=None, offset=None, explain=False):
    """
        Generator which takes an inbound stream of rowids,
        and cuts out the required chunk.
    """
    if explain:
        for message in stream: yield message
        yield "offset_limit, limit = %s, offset = %s" % (limit, offset)
        return

    if offset == None:
        skip_rows = 0
    else:
        skip_rows = offset
    emitted = 0
    for row in stream:
        if skip_rows > 0:
            skip_rows -= 1
            continue
        if limit is None or emitted < limit:
            emitted += 1
            yield row
        if limit is not None and emitted >= limit:
            break

def sorter(stream, order_by, dd, gl_cache=None, explain=False):
    """
        If we are sorting the result, create a temporary store with key, fileid.
        Only support a order_by columns with the same direction (i.e. ascending or descending).
    """
    if explain:
        for message in stream: yield message
        yield "sorter order_by = %s" % order_by
        return

    values = []
    ascending = True
    fields = []

    for (fieldid, direction) in order_by:
        if fieldid in ('_rowid', '_parentid'):
            fields.append(fieldid)
        else:
            field = dd.fields[fieldid]
            fields.append(field)
        if direction == 'ASC':
            ascending = True
        else:
            ascending = False

    for rowid, rec_gl_closed_form, rowid_path in stream:
        rec = M.Globals.from_closed_form(rec_gl_closed_form)
        key = []
        for field in fields:
            if field == '_rowid':
                key.append(rowid)
            else:
                key.append(field.retrieve(rec, gl_cache))
        values.append((key, (rowid, rec_gl_closed_form, rowid_path)))

    values.sort()
    if not ascending:
        values.reverse()
    for key, (rowid, rec_gl_closed_form, rowid_path) in values:
        yield (rowid, rec_gl_closed_form, rowid_path)

def apply_filters(stream, dbsfile, filters, gl_cache, explain=False):
    """
        Return true of false for whether rowid matches the set of filters,
        These are intended to handle the django filters.

        column > x
        column < x
    """
    if explain:
        for message in stream: yield message
        yield "apply_filters filters = %s" % filters
        return

    for rowid, rec_gl_closed_form, rowid_path in stream:
        rec = M.Globals.from_closed_form(rec_gl_closed_form)
        emit = True
        for fieldid, comparator, value in filters:
            ## Need mumps comparisons here - numerics versus non-numerics
            if fieldid == '_rowid':
                db_value = rowid
            else:
                field = dbsfile.dd.fields[fieldid]
                db_value = field.retrieve(rec, gl_cache)

            try:
                float(db_value)
                float(value)

                db_value = float(db_value)
                value = float(value)
            except:
                pass

            comparator = comparator.lower()

            if comparator == '='  and not (db_value == value):
                emit = False
                break
            if comparator == '>=' and not (db_value >= value):
                emit = False
                break
            if comparator == '>'  and not (db_value > value):
                emit = False
                break
            if comparator == '<'  and not (db_value < value):
                emit = False
                break
            if comparator == '<=' and not (db_value <= value):
                emit = False
                break
            if comparator == 'in' and not (db_value in value):
                emit = False
                break
            # TODO: move the comparator logic into the data-dictionary.
            #       logic will depend of field types.

        if emit:
            yield rowid, rec_gl_closed_form, rowid_path

def null_traversal(explain=False):
    if explain:
        yield "null_traversal"
        return

def file_order_traversal(gl, ranges=None, ascending=True, sf_path=[], explain=False):
    """
        Originate records by traversing the file in file order (i.e. no index)
    """
    if ranges:
        r = ranges[0]
        from_rowid = r['from_value']
        to_rowid = r['to_value']
        from_rule = r['from_rule']
        to_rule = r['to_rule']
    else:
        from_rowid, to_rowid, from_rule, to_rule = None, None, None, None

    if explain:
        yield "file_order_traversal, ascending=%s, gl=%s, X %s %s AND X %s %s" % (ascending,
                gl, from_rule, from_rowid, to_rule, to_rowid)
        return

    # the new person file has non-integer user ids
    if from_rowid != None:
        from_rowid = float(from_rowid)
    if to_rowid != None:
        to_rowid = float(to_rowid)

    if from_rowid != None and to_rowid != None:
        if ascending:
            assert(from_rowid <= to_rowid)
        else:
            assert(to_rowid <= from_rowid)
        
    if from_rowid is None:
        if ascending:
            lastrowid = "0"
        else:
            lastrowid = None
    else:
        # TODO: I have this in code in shared
        lastrowid = ('%f' % from_rowid).rstrip('0').rstrip('.').lstrip('0') 
        if from_rowid > 0 and lastrowid[0] == "0":
            lastrowid = lastrowid[1:]
        if lastrowid.endswith(".0"):
            lastrowid = lastrowid[:-2]

    first_pass = True

    if ascending:
        asc = 1
    else:
        asc = -1

    while 1:
        # If this is the first pass, we may have the id of a record, which needs to 
        # be verified
        found = False
        if first_pass:
            first_pass = False
            if lastrowid is None and asc == -1:
                lastrowid, = M.mexec("""set s0=$order(%ss0),-1)""" % gl, M.INOUT('%'))
                if valid_rowid(lastrowid):
                    found = True
            elif lastrowid and float(lastrowid) > 0:
                row_exists, = M.mexec("""set s0=$data(%ss0))""" % (gl), M.INOUT(lastrowid))
                if valid_rowid(row_exists):
                    found = True

        if not found:
            lastrowid, = M.mexec("""set s0=$order(%ss0),%d)""" % (gl, asc), M.INOUT(lastrowid))
            if not valid_rowid(lastrowid):
                break

        # Check boundary values
        f_lastrowid = float(lastrowid)
        if ascending:
            if from_rowid is not None:
                if f_lastrowid == from_rowid and from_rule == ">":
                    continue
            if to_rowid is not None:
                if f_lastrowid >= to_rowid and to_rule == "<":
                    break
                if f_lastrowid > to_rowid and to_rule == "<=":
                    break
        else: # descending:
            if f_lastrowid == 0:
                break # header record 
            if from_rowid is not None:
                if f_lastrowid == from_rowid and from_rule == "<":
                    continue
            if to_rowid is not None:
                if f_lastrowid <= to_rowid and to_rule == ">":
                    break
                if f_lastrowid < to_rowid and to_rule == ">=":
                    break

        # If this is a subfile, I need to return the full path.
        yield (lastrowid, "%s%s)" % (gl, lastrowid), sf_path + [lastrowid])

def subfile_traversal(stream, dd, ranges=None, ascending=True, explain=False):
    """
        This is chained to a parent file traverser.
        It receives a parent file rowid, and pulls the subfile rowids.

    """
    parent_field = dd.parent_dd.fields[dd.parent_fieldid]
    gl_subpath = parent_field.storage.split(';',1)[0]

    if explain:
        for message in stream: yield message
        yield "subfile_traversal, ascending=%s, dd=%s, ranges=%s" % (ascending, dd, ranges)
        return

    for rowid, rec_gl_closed_form, rowid_path in stream:
        sf = M.Globals.from_closed_form(rec_gl_closed_form)[gl_subpath]

        for sf_rowid, value in sf.keys_with_decendants():  # TODO: smarter filtering
            if sf_rowid == "0": 
                continue # file header
            if not valid_rowid(sf_rowid):
                break
            yield sf_rowid, "%s%s)" % (sf.open_form, sf_rowid), rowid_path + [gl_subpath, sf_rowid]


def index_order_traversal(gl_prefix, index, ranges=None, ascending=True, sf_path=[], explain=False):
    """
        A generator which will traverse an index.
        The iterator should yield rowids.

            Indices are stored:
                GLOBAL,INDEXID,VALUE,ROWID=""
            ^DIZ(999900,"B","hello there from unit test2",183)=""
            ^DIZ(999900,"B","hello there from unit test2",184)=""
            ^DIZ(999900,"B","hello there from unit test2",185)=""
            ^DIZ(999900,"B","record 1",1)=""

    """
    gl = gl_prefix + '"%s",' % index

    if ranges:
        r = ranges[0]
        from_value = r['from_value']
        to_value = r['to_value']
        from_rule = r['from_rule']
        to_rule = r['to_rule']
    else:
        from_value, to_value, from_rule, to_rule = None, None, None, None

    if explain:
        yield "index_order_traversal, ascending=%s, gl=%s, index=%s, X %s '%s' AND X %s '%s'" % (ascending,
                gl, index, from_rule, from_value, to_rule, to_value)
        return

    if from_value != None and to_value != None:
        if ascending:
            assert(from_value <= to_value)
        else:
            assert(to_value <= from_value)
    
    if from_value is None:
        if ascending:
            lastkey = " "
        else:
            lastkey = "ZZZZZZZZZZZZZZ"
        lastrowid = ""
    else:
        lastkey = from_value
        if from_rule == '>':
            lastrowid = None   # looks for the next key after lastkey
        else:
            lastrowid = ''     # looks for the lastkey


    if ascending:
        asc = 1
    else:
        asc = -1

    # TODO: Fileman seems to structure indices with keys in the global path
    #       or in the value - need to investigate further

    # There is a mad collation approach in M, where numbers sort before non-numbers.
    # this really messes up the keys.
    # How should I search? 

    # There is an inefficiency here it takes three searches to find the next record.
    while 1:
        if lastrowid is None:
            # locate the next matching index value
            lastkey, = M.mexec("""set s0=$order(%ss0),%s)""" % (gl, asc), M.INOUT(str(lastkey)))
            if lastkey == "":
                break

            if ascending:
                if from_value is not None:
                    if from_rule == ">" and lastkey <= from_value:
                        continue
                    if from_rule == ">=" and lastkey < from_value:
                        assert 0
                if to_value is not None:
                    if to_rule == "<=" and lastkey > to_value:
                        break
                    if to_rule == "=" and lastkey != to_value:
                        break
                    if to_rule == "<" and lastkey >= to_value:
                        break
                lastkey = lastkey
                lastrowid = "0"

            else: # descending
                if from_value is not None:
                    if from_rule == "<" and lastkey >= from_value:
                        continue
                    if from_rule == "<=" and lastkey > from_value:
                        assert 0
                if to_value is not None:
                    if to_rule == ">=" and lastkey < to_value:
                        break
                    if to_rule == "=" and lastkey != to_value:
                        break
                    if to_rule == ">" and lastkey <= to_value:
                        break
                lastkey = lastkey
                lastrowid = ""

        # Have the key, get the first matching rowid
        lastrowid, = M.mexec("""set s0=$order(%s"%s",s1),%d)""" % (gl, lastkey, asc),
                M.INOUT(str(lastkey)), lastrowid)
        if lastrowid == "":
            # No match
            lastrowid = None
            continue

        yield (lastrowid, "%s%s)" % (gl_prefix, lastrowid), sf_path + [lastrowid])

#------------------------------------------------------------------------------------------------

def _index_for_column(dd, col_fieldid):
    """
        Find an index in the data dictionary for the given column.
    """
    for index in dd.indices:
        if index.table != dd.fileid:   # indexes can be on embedded models
            continue
        if len(index.columns) == 1 and index.columns[0] == col_fieldid:
            return index.name
    return None

def _filters_to_sargable(filters):
    """
        Analyse the filters - see what is sargable
        return a dictionary with the column names and
        the sargable rules referring to it.
    """
    sargable = {}
    for fieldid, comparator, value in filters:
        if (comparator in ["<", "<=", "=", ">=", ">"]) or (comparator.lower() in ["in"] and len(value) == 1):
            if fieldid not in sargable.keys():
                sargable[fieldid] = []
            sargable[fieldid].append((comparator, value))
    return sargable

def _possible_indices(sargable, dd, parent_dd):
    """
        Given the sargable columns, which ones are actually indexed.
        Return column, index and rules
    """
    indices = {}

    if parent_dd:
        # This is a sub-file - indices are on the parent
        index_list = parent_dd.indices
    else:
        index_list = dd.indices
    for index in index_list:
        if index.table != dd.fileid:   # indexes can be on embedded models
            continue
        if len(index.columns) == 1 and index.columns[0] in sargable.keys():
            col_fieldid = index.columns[0]
            if col_fieldid not in indices:
                indices[col_fieldid] = []
            indices[col_fieldid].append(index.name)
    return indices

def _ranges_from_index_filters(index_filters, ascending=True):
    """
        Given a set of index filters, return a set of ranges.

        from, from_rule, to, to_rule

        There can be more than one range where the "in" rule is provided
    """
    ### TODO: First attempt - only one set

    lb_value, lb_rule, ub_value, ub_rule = None, None, None, None

    for comparator, value in index_filters:
        comparator = comparator.lower()
        if comparator in [">", ">=", "=", 'in']:
            # TODO: comparason based on mumps rules, not python rules
            if lb_value is None or lb_value < value:
                if comparator == 'in':
                    assert(len(value) == 1)
                    lb_value = value[0]
                else:
                    lb_value = value
                if comparator in ["=", 'in']:
                    lb_rule = ">="
                else:
                    lb_rule = comparator
            elif lb_value == value:
                if lb_rule in [">="] and comparator in [">"]:
                    lb_rule = comparator
        if comparator in ["<", "<=", "=", 'in']:
            # TODO: comparason based on mumps rules, not python rules
            if ub_value is None or ub_value > value:
                if comparator == 'in':
                    assert(len(value) == 1)
                    ub_value = value[0]
                else:
                    ub_value = value
                if comparator in ["=", 'in']:
                    ub_rule = "<="
                else:
                    ub_rule = comparator
            elif ub_value == value:
                if ub_rule in ["<="] and comparator in ["<"]:
                    ub_rule = comparator
    if ascending:
        return [{'from_value':lb_value, 'to_value': ub_value, 'from_rule': lb_rule, 'to_rule': ub_rule}]
    else:
        return [{'from_value':ub_value, 'to_value': lb_value, 'from_rule': ub_rule, 'to_rule': lb_rule}]

def make_plan(dbsfile, filters=None, order_by=None, limit=None, offset=0, gl_cache=None, explain=False):
    """
        Given the filters and the order_by clause
        return an iterator which produces the matching
        rowids.

        dbsfile is a DBSFile object.
        filters are a set of filters applied to the output,
        order_by controls the output order
        limit, offset extract a subset from the results
        gl_cache is used to cache globals retrieved to 
        avoid extra calls into M
    """
    pipeline = None

    dd = dbsfile.dd

    if dd.parent_dd:
        return make_subfile_plan(dbsfile, filters=filters, order_by=order_by, limit=limit, offset=offset, gl_cache=gl_cache, explain=explain)

    ### First we need a traverser. There are a number of options,
    ### file_order_traversal - order by the file records
    ### index_order_traversal - traverse the file using a traditional index

    ### Future

        ### subfile_order_traversal - go through the sub-file records
        ### subfile_index_order_traversal - find subfile records using an index
        ### file_newindex_traversal - new style index traversal
        ### others?

    gl_prefix = dbsfile.dd.m_open_form()

    ### Case 1: Straight file dump
    if not filters and not order_by:
        pipeline = file_order_traversal(gl_prefix, explain=explain)
    
    ### Case 2:  if there is no filters, but there is an order by,
    #            find an index for the order_by
    #            format of order_by: "order_by": [["_rowid", "ASC"]

    elif not filters and order_by:
        if len(order_by) > 1:
            logging.warn("Extra columns on order-by, currently ignored %s", order_by)

        if len(order_by) == 1 and order_by[0][0] == '_rowid':
            if order_by[0][1] == 'ASC':
                pipeline = file_order_traversal(gl_prefix, explain=explain)
            else:
                pipeline = file_order_traversal(gl_prefix, ascending=False, explain=explain)

        else:
            # TODO: Subfiles
            order_fieldid = order_by[0][0]
            index = _index_for_column(dd, order_fieldid)
            if index:
                pipeline = index_order_traversal(gl_prefix, index, ascending = (order_by[0][1] == 'ASC'), explain=explain)
            else:
                pipeline = file_order_traversal(gl_prefix, explain=explain)
                pipeline = sorter(pipeline, order_by, dd, gl_cache, explain=explain)

            
    ### Case 3: There are filters
    else:

        # 1. Identify the sargable columns
        sargable = _filters_to_sargable(filters)
        if not sargable:
            pipeline = file_order_traversal(gl_prefix, explain=explain)
        else:

            # 2. find columns with indexes
            #    choose the preferred index (sargable + orderable, fileorder, first index)
            if len(sargable.keys()) == 1 and sargable.keys()[0] == '_rowid':
                # direct record retrieve - indexes not necessary
                fieldid = "_rowid"
                index = None

            else:
                indices =  _possible_indices(sargable, dd, None)
                if len(indices) == 0:
                    fieldid = None
                    index = None

                elif len(indices) == 1:
                    fieldid = indices.keys()[0]
                    index = indices[fieldid][0]  # There can be more than one - why?

                else:
                    # More than one option. How to choose the best?
                    # result order, file order, other?
                    # or '=' has precendence over range?

                    # Choose first for now
                    fieldid = indices.keys()[0]
                    index = indices[fieldid][0]

            # At this point we have choosen an index. Have to choose the 
            # traversal rules, and remove the index from the filters
            if fieldid:
                index_filters = sargable[fieldid]
            else:
                index_filters = []

            # Ignoring sub-order-by rules
            if index == None and order_by and order_by[0][0] == '_rowid':
                ascending = (order_by[0][1] == 'ASC')
            elif order_by and order_by[0][0] == fieldid:
                ascending = (order_by[0][1] == 'ASC')
            else:
                ascending = True

            ranges = _ranges_from_index_filters(index_filters, ascending)

            if index == None:
                pipeline = file_order_traversal(gl_prefix, ranges=ranges, ascending=ascending, explain=explain)
            else:
                pipeline = index_order_traversal(gl_prefix, index=index, ranges=ranges, ascending=ascending, explain=explain)

        if order_by:
            pipeline = sorter(pipeline, order_by, dd, gl_cache, explain=explain)

    if filters:
        pipeline = apply_filters(pipeline, dbsfile, filters, gl_cache, explain=explain)

    if offset or limit:
        pipeline = offset_limit(pipeline, limit=limit, offset=offset, explain=explain)

    return pipeline

def _common_path(from_rowid, to_rowid):
    """
        When traversing to sub-files, you can find records
        based on a range. This function, identifies the 
        common path between the start and the end range.

        The common path can be navigated too, before a traversal
        is required.

    """
    from_parts = from_rowid.split(",")
    to_parts = to_rowid.split(",")
    shared = []
    while len(from_parts) > 0 and len(to_parts) > 0:
        f, t = from_parts.pop(), to_parts.pop()
        if f != t:
            return shared, [f] + from_parts, [t] + to_parts
        shared.insert(0, f)
    return shared, [], []

def make_subfile_plan(dbsfile, filters=None, order_by=None, limit=None, offset=0, gl_cache=None, explain=False):
    """
        Sub-files are of an arbitrary depth. 
        The primary key has to be made up of _rowid, _rowid1, _rowid2 ... _rowidn

        There are really two options here, we are pulling records
        from a single parent, or we are searching for a parent using
        a multiple field such as SSN.
    """
    ascending = True
    pipeline = None

    dd = dbsfile.dd

    # Construct a list of the parents.
    parent_dds = [dbsfile.dd]
    while dd.parent_dd:
        parent_dds.insert(0, dd.parent_dd)
        dd = dd.parent_dd

    dd = dbsfile.dd
    sargable = _filters_to_sargable(filters)

    # normally sargable will include a _parentid, which identifies
    # the parent record.
    assert '_parentid' in sargable
    parent_rule = sargable['_parentid']
    ranges = _ranges_from_index_filters(parent_rule)
    gl_prefix = dd.parent_dd.m_open_form()

    # need to study the range and get the common root. Can only differ on 
    # the last part.
    if ranges:
        r = ranges[0]
        from_rowid, to_rowid = r['from_value'], r['to_value']
        shared, from_rowid, to_rowid = _common_path(from_rowid, to_rowid)
        assert len(from_rowid) <= 1
        assert len(to_rowid) <= 1
        assert len(from_rowid) == len(to_rowid)
        if shared and len(shared) == 1 and shared[0] == '':
            pipeline = null_traversal(explain=explain)
        else:
            sf_rowid_path = []
            gl_prefix = parent_dds[0].m_open_form()
            for sf_dd, rowid in zip(parent_dds[1:], shared):
                parent_field = sf_dd.parent_dd.fields[sf_dd.parent_fieldid]
                sf_path = parent_field.storage.split(";")[0]
                gl_prefix = gl_prefix + rowid + "," + sf_path + ","
                sf_rowid_path.append(rowid)
                sf_rowid_path.append(sf_path)

            if from_rowid and to_rowid:
                r['from_value'], r['to_value'] = from_rowid, to_rowid
            else:
                ranges = None

            pipeline = file_order_traversal(gl_prefix, ranges=ranges, sf_path = sf_rowid_path, explain=explain)

        # Now, how far down are we - do we need to add a subfile traverser?

    # Get the parent record
    else:
        pipeline = file_order_traversal(gl_prefix, ranges=ranges, explain=explain)
        pipeline = subfile_traversal(pipeline, dd, explain=explain)

    return pipeline
