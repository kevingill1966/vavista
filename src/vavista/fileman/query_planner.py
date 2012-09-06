"""
    This is the query planner used by the Django clients.

    The planner should return generator object, which is used to
    suck results out. Depending on the setup, the generator
    can be used in conjunction with a result set generator,
    and aggregator or a simple count call.

    TODO: Explain
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

### The generators that implement the pipeline

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

    for (fieldname, direction) in order_by:
        fieldid = dd.attrs[fieldname]
        field = dd.fields[fieldid]
        if direction == 'ASC':
            ascending = True
        else:
            ascending = False
        fields.append(field)

    _gl = M.Globals.from_closed_form("%s)" % dd.m_open_form()[:-1])

    for rowid in stream:
        rec = _gl[rowid]
        key = [field.retrieve(rec, gl_cache) for field in fields]
        values.append((key, rowid))

    values.sort()
    if not ascending:
        values.reverse()
    for key, rowid in values:
        yield rowid

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

    dd = dbsfile.dd
    file_root = M.Globals.from_closed_form("%s)" % dd.m_open_form()[:-1])
    for rowid in stream:
        rec = file_root[rowid]
        emit = True
        for colname, comparator, value in filters:
            field = dbsfile._dd_field_byname(colname)
            db_value = field.retrieve(rec, gl_cache)

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
            yield rowid


def file_order_traversal(gl, from_rowid=None, to_rowid=None, ascending=True, from_rule=None, to_rule=None, explain=False):
    """
        Originate records by traversing the file in file order (i.e. no index)
    """
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

        yield lastrowid


def index_order_traversal(gl_prefix, index, from_value=None, to_value=None, ascending=True,
        from_rule=">=", to_rule="<", explain=False):
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

        yield(lastrowid)

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
    pipeline = []

    dd = dbsfile.dd


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
        pipeline.append(file_order_traversal(gl_prefix, explain=explain))
    
    ### Case 2:  if there is no filters, but there is an order by,
    #            find an index for the order_by
    #            format of order_by: "order_by": [["_rowid", "ASC"]

    elif not filters and order_by:
        if len(order_by) > 1:
            logging.warn("Extra columns on order-by, currently ignored %s", order_by)

        if len(order_by) == 1 and order_by[0][0] == '_rowid':
            if order_by[0][1] == 'ASC':
                pipeline.append(file_order_traversal(gl_prefix, explain=explain))
            else:
                pipeline.append(file_order_traversal(gl_prefix, ascending=False, explain=explain))

        else:
            # TODO: Subfiles
            order_col = order_by[0][0]
            order_fieldid = dd.attrs[order_col]
            index = _index_for_column(dd, order_fieldid)
            if index:
                pipeline.append(index_order_traversal(gl_prefix, index, ascending = (order_by[0][1] == 'ASC'), explain=explain))
            else:
                pipeline.append(file_order_traversal(gl_prefix, explain=explain))
                pipeline.append(sorter(pipeline[0], order_by, dd, gl_cache, explain=explain))

            
    ### Case 3: There are filters
    else:

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
            pipeline.append(file_order_traversal(gl_prefix, explain=explain))
        else:
            sargable_fields = sargable.keys()

            # 2. find columns with indexes
            #    choose the preferred index (sargable + orderable, fileorder, first index)
            if len(sargable_fields) == 1 and sargable_fields[0] == '_rowid':
                # direct record retrieve - indexes not necessary
                colname = "_rowid"
                index = None

            else:
                # There is a mis-match here colnames versus fieldids
                sargable_fieldids = dict([(dd.attrs[colname], colname) for colname in sargable_fields])
                for index in dd.indices:
                    if index.table != dd.fileid:   # indexes can be on embedded models
                        continue
                    if len(index.columns) == 1 and index.columns[0] in sargable_fieldids.keys():
                        colname = sargable_fieldids[index.columns[0]]
                        sargable[colname].append(index.name)

                unindexed = [k for k,v in sargable.items() if len(v) == 0 and k != '_rowid']
                for k in unindexed:
                    del sargable[k]

                if len(sargable) == 0:
                    colname = None
                    index = None

                elif len(sargable) == 1:
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
            lb_value, lb_rule, ub_value, ub_rule = None, None, None, None

            for colname, comparator, value in index_filters:
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

            if index == None:
                # File order traversal

                if order_by and order_by[0][0] == '_rowid':
                    ascending = (order_by[0][1] == 'ASC')
                    order_by = None
                else:
                    ascending = True
                if ascending:
                    pipeline.append(file_order_traversal(gl_prefix, from_rowid=lb_value, to_rowid=ub_value,
                        from_rule=lb_rule, to_rule=ub_rule, ascending=True, explain=explain))
                else:
                    pipeline.append(file_order_traversal(gl_prefix, from_rowid=ub_value, to_rowid=lb_value,
                        from_rule=ub_rule, to_rule=lb_rule, ascending=False, explain=explain))
            else:
                if order_by and order_by[0][0].lower() == colname.lower():
                    ascending = (order_by[0][1] == 'ASC')
                    order_by = None
                else:
                    ascending = True

                if ascending:
                    pipeline.append(index_order_traversal(gl_prefix, index=index, from_value=lb_value, to_value=ub_value,
                        from_rule=lb_rule, to_rule=ub_rule, ascending=True, explain=explain))
                else:
                    pipeline.append(index_order_traversal(gl_prefix, index=index, from_value=ub_value, to_value=lb_value,
                        from_rule=ub_rule, to_rule=lb_rule, ascending=False, explain=explain))

        if order_by:
            pipeline.append(sorter(pipeline[0], order_by, dd, gl_cache, explain=explain))

    if filters:
        pipeline.append(apply_filters(pipeline[0], dbsfile, filters, gl_cache, explain=explain))

    if offset or limit:
        pipeline.append(offset_limit(pipeline[0], limit=limit, offset=offset, explain=explain))

    return pipeline.pop()