"""
    The DBSRow maps to a single Fileman record.

    The data retrieval, insert, update and delete logic belongs here.
"""

from vavista import M
from vavista.fileman.dbsdd import FT_WP, FT_SUBFILE, FT_COMPUTED
from shared import FilemanError, ROWID, STRING, FilemanErrorNumber

class FilemanValidationError(FilemanError):
    filename, row, fieldid, value, error_code, error_msg = None, None, None, None, None, None
    err, help = None, None
    def __init__(self, **kwargs):
        for k,v in kwargs.items():
            setattr(self, k, v)
    def __str__(self):
        return """file [%s], row = [%s], fieldid = [%s], value = [%s], error_code = [%s], error_msg = [%s] help = %s""" \
            % (self.filename, self.row, self.fieldid, self.value, self.error_code, self.error_msg,
            self.help)

class FilemanLockFailed(FilemanError):
    filename, row, timeout = None, None, None
    def __init__(self, **kwargs):
        for k,v in kwargs.items():
            setattr(self, k, v)
    def __str__(self):
        return """file [%s], row = [%s], timeout = [%s]""" \
            % (self.filename, self.row, self.timeout)

def err_to_str(err):
    return '\n'.join(["%s = %s" % x for x in err.serialise()])

class DBSRow(object):
    """
        This is the key to the whole implementation.  This object maps to a single row
        in the Fileman global. You use it to retrieve fields from that row and to update
        that row. This class has to take care of conversions between Python and M, and
        to ensure that the integrity of the M data store is not violated.

        Access either by id:   row["0.1"]
        or label:              row["name"]

    """
    _changed = False
    _changed_fields = None
    _locked = False
    _dbsfile = None
    _dd = None
    _rowid = None
    _fields = None
    _fieldids = None
    _stored_data = None
    _row_tmpid = None
    _row_fdaid = None
    _internal=True

    def __init__(self, dbsfile, dd, rowid, tmpid=None, fieldids=None, internal=True):
        self._dbsfile = dbsfile
        self._dd = dd
        self._rowid = rowid
        self._internal = internal
        self._changed_fields = []

        # For subfiles, if the field name is T1, the field of interest is
        # T1 in subfile T1 (T1->.01). T1->T2 is field T2 in file T1.
        fields = self._fields = dict()
        if fieldids:
            for fieldid in fieldids:
                if type(fieldid) == tuple:
                    parent, child = fieldid
                    parent = dd.fields[parent]
                    child = parent.dd.fields[child]
                    fields[fieldid] = (parent, child)
                else:
                    fields[fieldid] = dd.fields[fieldid]
            self._fieldids = fieldids
        else:
            self._fields = dd.fields
            self._fieldids = dd.fields.keys()
            self._fieldids.sort()

        # Lazy evaluation
        if tmpid:
            self._row_tmpid = tmpid
        else:
            self._row_tmpid = "row%s" % id(self)
        self._stored_data = None

    def lock(self, timeout=5):
        """
            Lock a record.
        """
        g_path = self._dd.m_closed_form(self._rowid)

        # Set the timeout
        M.Globals["DILOCKTM"].value = timeout

        # use DILF^LOCK function to perform the lock
        M.proc("LOCK^DILF", g_path)

        # result is returned in $T
        rv, = M.mexec("set l0=$T", M.INOUT(0))
        if rv != 1:
            raise FilemanLockFailed(filename=self._dd.filename, row=self._rowid, timeout=timeout)

    def unlock(self):
        """
            Unlock the record
        """
        # Locking is done via an M level routine on the record global
        g_path = self._dd.m_closed_form(self._rowid)
        M.mexec(str("LOCK -%s" % g_path))   # TODO: mexec to take unicode
            
    @property
    def _data(self):
        # for lazy evaluation
        if self._stored_data is None:
            self._retrieve()
        return self._stored_data

    @property
    def _iens(self):
        if self._rowid is None:
            return "+1," # protocol used for inserting records, fileman pm 3-125
        else:
            if type(self._rowid) == str and self._rowid.endswith(','):
                return self._rowid
            if type(self._rowid) == float:
                # str puts a leading 0 on values between 0 and 1
                return ('%f' % self._rowid).rstrip('0').rstrip('.').lstrip('0') + ","
            else:
                return str(self._rowid) + ","

    def __str__(self):
        fields = self._dd.fields
        rv = ['DBSRow file=%s, fileid=%s, rowid=%s' % (self._dd.filename, self._dd.fileid, self._rowid)]
        keys = self.keys()
        keys.sort()
        for k in keys:
            v = self._data.get(k)
            if v:
                f = fields.get(k)
                if f:
                    fn = f.label
                else:
                    fn = "not in dd"
                if type(v) == M.Global:
                    if self._internal:
                        rv.append('%s (%s) = "%s"' % (fn, k, v['I'].value))
                    else:
                        rv.append('%s (%s) = "%s"' % (fn, k, v.value))
                else:
                        rv.append('%s (%s) = "%s"' % (fn, k, v))
        return '\n'.join(rv)

    def _field_from_id(self, fieldid):
        """
            Given a fieldid, return the data dictionary object.
        """
        return self._dd._fields[fieldid]

    def __del__(self):
        # Each time we retrieve a row, it is copied to a temporary store. 
        # This needs to be killed or we have a memory leak in GT.M
        if M == None:
            return # happens during process exit
        M.Globals[self._row_tmpid].kill()
        if self._row_fdaid:
            M.Globals[self._row_fdaid].kill()

    def _get_gl(self):
        """return a global for the current _rowid"""
        dd = self._dd
        if type(self._rowid) == list:
            path = []
            path.append(dd)
            while dd.parent_dd:
                dd = dd.parent_dd
                path.append(dd)

            root_gl = dd.m_open_form() + "%s)" % self._rowid[0]
            record = M.Globals.from_closed_form(root_gl)
            for i in self._rowid[1:]:
                record = record[i]
            return record
        else:
            gl = dd.m_open_form() + "%s)" % self._rowid
            return M.Globals.from_closed_form(gl)

    def raw_retrieve(self, cache=None):
        """
            The DBS retrieve function seems very slow so I am trying
            to retrieve the data directly from the global.

            I also want filter rows in a resultset without pulling the
            full row. 

            Finally, some data is not coming back via the DBS retrieve
            methods, i.e. the kernal intro data.

            This code should create an identical result to the retrieve() below.
        """

        # If any of the fields are a "computed", cannot use the raw retrieve.
        force_cooked = False
        for (fieldid, field) in self._fields.items():
            if type(fieldid) != tuple and field.fmql_type in [FT_COMPUTED]:
                force_cooked = True
                break
        if force_cooked:
            return self.retrieve()

        self._stored_data = result = {}

        dd = self._dd

        # verify the global exists.
        # gl = dd.m_open_form() + "%s)" % self._rowid
        # gl_rec = M.Globals.from_closed_form(gl)
        gl_rec = self._get_gl()

        if not gl_rec.exists():
            raise FilemanError("File %s, record %s, does not exist", dd.filename, self._rowid)

        for (fieldid, field) in self._fields.items():
            if type(fieldid) == tuple:                 # Subfile list
                parent, child = field
                retrieved = parent.retrieve(gl_rec, cache, [child])
                if retrieved:
                    result[fieldid] = [child.pyfrom_internal(rec[0]) for rec in retrieved]
            else:
                if field.fmql_type in [FT_SUBFILE]:    # Embedded Schema - return list of dicts
                    fields = field.fields
                    retrieved = field.retrieve(gl_rec, cache, fields, asdict=True)
                    if retrieved is not None:
                        result[fieldid] = fresult = []
                        for row in retrieved:
                            row_cooked = dict([
                                (fieldx.label, fieldx.pyfrom_internal(row[fieldx.label]))
                                for fieldx in fields])
                            row_cooked['_rowid'] = row['_rowid']
                            fresult.append(row_cooked)
                else:
                    retrieved = field.retrieve(gl_rec, cache)  # Simple value
                    if retrieved is not None:
                        result[fieldid] = field.pyfrom_internal(retrieved)

    def retrieve(self):
        """
            Retrieve values
            Internal or External
        """
        M.Globals["ERR"].kill()

        flags = 'N'    # no nulls
        if self._internal:
            flags = flags + "I"

        f = []
        for (k,v) in self._fields.items():
            if type(k) == tuple:
                if k[0]+"*" not in f:
                    f.append(k[0]+"*")
            else:
                if v.fmql_type in [FT_SUBFILE]:
                    f.append(k+"*")
                else:
                    f.append(k)
        fieldids = ";".join(f)
        fileid = self._dd.fileid
        iens = self._iens

        M.proc("GETS^DIQ",
            fileid,              # numeric file id
            iens,                # IENS
            fieldids,            # Fields to return TODO
            flags,               # Flags N=no nulls, R=return field names
            self._row_tmpid,
            "ERR")

        # Check for error
        err = M.Globals["ERR"]
        if err.exists():
            print "error retrieving %s, file %s" % (iens, fileid)
            #import pdb; pdb.set_trace()
            raise FilemanErrorNumber(dierr=err)

        self._save_tmp_global()

    def as_list(self):
        """
            Return the current result set (self._stored_data) as a sequence,
            as per the dbapi spec. The description of the rows is in the
            description property.
        """
        rv = []
        for fieldid in self._fieldids:
            rv.append(self._stored_data.get(fieldid, None))
        return tuple(rv)

    @property
    def description(self):
        """
            Describe the result set, as per the dbapi spec.

            Each of these sequences contains information describing
            one result column: 

              (name, 
               type_code, 
               display_size,
               internal_size, 
               precision, 
               scale, 
               null_ok)

            All of the fields are returned as strings for the moment.
        """
        rv = []
        for fieldid in self._fieldids:
            field = self._fields[fieldid]
            # TODO: get data from data-dictionary
            if type(field) == tuple: # subfile item
                rv.append(("%s->%s" % (field[0].label, field[1].label), STRING, None, None, None, None, True))
            else:
                rv.append((field.label, STRING, None, None, None, None, True))
        return rv

    def _save_tmp_global(self):
        """
            Extract results from the result area, and store them into _stored_data

            fileid / iens / fieldid / 'I' = value
        """
        main_fileid = str(self._dd.fileid)
        self._stored_data = {}

        root = M.Globals[self._row_tmpid]

        subfiles = {}

        for fileid, v in root.keys_with_decendants():
            # Data is stored either under a fileid or a subfileid
            if fileid == main_fileid:
                file = root[fileid]
                for iens, v in file.keys_with_decendants():
                    row = file[iens]
                    self._stored_data['_rowid'] = iens.split(",", 1)[0]
                    if self._internal:
                        for fieldid, v in row.keys_with_decendants():
                            field = self._dd.fields[fieldid]
                            self._stored_data[fieldid] = field.pyfrom_internal(row[fieldid]['I'].value)
                    else:
                        for fieldid in row.keys():
                            field = self._dd.fields[fieldid]
                            self._stored_data[fieldid] = field.pyfrom_external(row[fieldid].value)
            else:
                # This is a subfile
                if fileid in subfiles:
                    store = subfiles[fileid]
                else:
                    store = subfiles[fileid] = dict()

                sf = root[fileid]
                for iens, v in sf.keys_with_decendants():
                    row = sf[iens]
                    if iens in store:
                        rowstore = store[iens]
                    else:
                        rowstore = store[iens] = dict()


                    # TODO: convert from internal/external to python
                    for fieldid, v in row.keys_with_decendants():
                        if self._internal:
                            rowstore[fieldid] = row[fieldid]['I'].value
                        else:
                            rowstore[fieldid] = row[fieldid].value

        # Now we have extracted the subfiles - we need to move the
        # data to where we can use it easier.
        for sf, sf_data in  subfiles.items():
            sf_data = list(sf_data.items())
            sf_data.sort()

            sf_fields = []
            for k,v in self._fields.items():
                if type(k) == tuple:
                    if v[0].fmql_type in [FT_SUBFILE] and v[0].subfileid == sf:
                        sf_fields.append(k)
                elif v.fmql_type in [FT_SUBFILE] and v.subfileid == sf:
                    sf_fields.append(k)

            for fieldid in sf_fields:
                self._stored_data[fieldid] = sf_result = []

                for iens, data in sf_data:
                    if type(fieldid) == tuple:
                        sf_result.append(data.get(fieldid[1]))
                    else:
                        sf_result.append(data.get('.01', 'NO NAME'))

        self._changed = False
        self._changed_fields = []

    def _create_fda(self, values=None, include_nulls=True):
        """
            For the current record, copy all changed fields to an FDA record
            (Fileman Data Array), see programmer manual 3.2.3

            FDA_ROOT(FILE#,"IENS",FIELD#)="VALUE"

        """
        subfile_sizes = {}
        self.adjust_subfiles = None
        el_count = 0

        self._row_fdaid = row_fdaid = "fda%s" % id(self)
        fda = M.Globals[row_fdaid]
        fda.kill()
        fileid = self._dd.fileid
        iens = self._iens
        if values:
            for fieldid, value in values.items():
                field = self._fields.get(fieldid)
                if field is None:
                    if type(fieldid) == tuple and fieldid[1] == '.01':
                        field = self._fields[fieldid[0]]
                    else:
                        assert 0, "could not resolve field %s" % field
                if type(field) == tuple:
                    if not self._rowid:

                        # MULTIPLES - INSERT ONLY
                        #
                        # Create multiple values for inserts only
                        # For updates we need to deal with the multiple in a second pass. 
                        # This is because the Parent File update does not handle appends/deletes
                        # to/from the subfile.

                        parent, child = field
                        subfileid = parent.subfileid
                        sf_fieldid = fieldid[1]

                        # the same key lengths for each
                        if fieldid[0] in subfile_sizes:
                            if len(value) != subfile_sizes[fieldid[0]]:
                                raise FilemanError("Insert/Update - subfile entry sizes for subfile %s do not match" % subfileid)

                        subfile_sizes[fieldid[0]] = len(value)
                        for sf_rowid, row in enumerate(value):
                            sf_iens = "%d,%s," % (sf_rowid+1, self._rowid)
                            if self._internal:
                                fda[subfileid][sf_iens][sf_fieldid].value = row
                            else:
                                fda[subfileid][sf_iens][sf_fieldid].value = row
                            el_count += 1

                elif field.fmql_type in [FT_WP]:
                    # WP Fields
                    # See fm22_0pm.pdf, page 194
                    if self._internal:
                        mvalue = field.pyto_internal(value)
                    else:
                        mvalue = field.pyto_external(value)
                    field.validate_insert(mvalue, self._internal)
                    base = M.Globals["TMP"][str(fileid) + ".wp." + str(fieldid)]
                    base.kill()
                    for i, line in enumerate(mvalue):
                        base[str(i+1)].value = line

                    fda[fileid][iens][fieldid].value = base.closed_form
                    el_count += 1

                else:
                    if self._internal:
                        mvalue = field.pyto_internal(value)
                    else:
                        mvalue = field.pyto_external(value)
                    field.validate_insert(mvalue, self._internal)
                    if mvalue == '':
                        if include_nulls:
                            fda[fileid][iens][fieldid].value = '@'
                    else:
                        fda[fileid][iens][fieldid].value = mvalue
                    el_count += 1
        else:
            for fieldid in self._changed_fields:
                fda[fileid][iens][fieldid].value = self._getitem(fieldid).value
                el_count += 1

        if el_count:
            return row_fdaid
        else:
            return None

    def insert(self, values=None):
        """
            Create a new record. Values is a dictionary containing the values.

            TODO: Inserts to the state file are prohibited. How is this implemented.
        """
        M.Globals["ERR"].kill()
        ienid = "ien%s" % id(self)
        M.Globals[ienid].kill()

        # Create an FDA format array for fileman
        fdaid = self._create_fda(values, include_nulls=False)

        # Flags:
        # E - use external formats
        # S - do not clear the row global

        # TODO: I want the external format for validation,
        #       but the internal format for usablility
        if self._internal:
            flags = ""
        else:
            flags = "E"

        M.proc("UPDATE^DIE", flags , fdaid, ienid, "ERR")

        # Check for error
        err = M.Globals["ERR"]
        if err.exists():

            # TODO: Work out the error codes.

            # ERR.DIERR.6.PARAM.0 = "3"
            # ERR.DIERR.6.PARAM.FIELD = "1901"
            # ERR.DIERR.6.PARAM.FILE = "2"
            # ERR.DIERR.6.PARAM.IENS = "+1,"
            # ERR.DIERR.6.TEXT.1 = "The new record '+1,' lacks some required identifiers."

            raise FilemanErrorNumber(dierr=err)
        
        # What is the id of the new record?
        self._rowid = int(M.Globals[ienid]['1'].value)
        self._stored_data = None
        return self._rowid

    def update(self, values=None):
        """
            Write changed data back to the database.
            
            TODO: dbsdd validation for fields
        """
        # Create an FDA format array for fileman
        fdaid = self._create_fda(values)

        if fdaid:
            M.Globals["ERR"].kill()

            # Flags:
            # E - use external formats
            # K - lock the record
            # S - do not clear the row global
            # T - verify the data
            #TODO: I want to do validation, but use internal format throughout
            if self._internal:
                flags = ""
            else:
                flags = "ET"
            M.proc("FILE^DIE", flags, fdaid, "ERR")

            # Check for error
            err = M.Globals["ERR"]
            if err.exists():
                raise FilemanErrorNumber(dierr=err)

        # If there are subfiles, these will have to be processed.
        subfiles = set([x[0] for x in values.keys() if type(x) == tuple])
        if len(subfiles) > 0:
            for subfile in subfiles:
                self._update_subfile(subfile, [(x, values[x]) for x in values.keys() if type(x) == tuple])

    def _update_subfile(self, fieldid, sf_new_values):
        """
            Updating a subfile. This may involve inserting / deleting or just updating the data.
        """
        # Step 1 - retrieve the existing data and compare.
        subfile_dd = self._dd.fields[fieldid].dd

        M.Globals["ERR"].kill()

        flags = ''
        if self._internal:
            flags = flags + "I"

        fileid = self._dd.fileid
        iens = self._iens
        fieldids = str(fieldid) + "*"

        tmpid = self._row_tmpid + str(fieldid)

        M.proc("GETS^DIQ",
            fileid,              # numeric file id
            iens,                # IENS
            fieldids,            # Fields to return 
            flags,               # Flags N=no nulls, R=return field names
            tmpid,
            "ERR")

        # Check for error
        err = M.Globals["ERR"]
        if err.exists():
            raise FilemanErrorNumber(dierr=err)

        # Extract the result and store in rows.
        subfile_data = M.Globals[tmpid][subfile_dd._fileid]

        sf_live_data = []
        for iens in [r[0] for r in subfile_data.keys_with_decendants()]:
            row = subfile_data[iens]
            if self._internal:
                row_fieldids = [x[0] for x in row.keys_with_decendants()]
                row_data = dict([(x, row[x]['I'].value) for x in row_fieldids])
            else:
                row_fieldids = row.keys()
                row_data = dict([(x, row[x].value) for x in row_fieldids])
            sf_live_data.append((iens, row_data))

        # Convert the new data to the same format
        sf_new_data=[]
        for (f_fieldid, sf_fieldid), values in sf_new_values[:1]:
            for i, value in enumerate(values):
                iens = '%d,%s' % (i+1, self._rowid)
                d = (iens, {sf_fieldid: value})
                sf_new_data.append(d)

        for (f_fieldid, sf_fieldid), values in sf_new_values[1:]:
            for i, value in enumerate(values):
                sf_new_data[i][1][sf_fieldid] = value

        # Now we have the rows on the database. For each row, we are going
        # to update, or delete it.
        fdaid = "fda%s" % id(self)
        fda = M.Globals[fdaid]
        fda.kill()
        sf_fda = fda[subfile_dd._fileid]

        # Pass 1 - updates
        do_update = False
        for i in range(len(sf_live_data)):
            if i < len(sf_new_data):
                sf_iens, row_data = sf_live_data[i]
                n_iens, n_row_data = sf_new_data[i]
                if row_data != n_row_data:
                    # need to do an update
                    for f,v in n_row_data.items():
                        sf_fda[sf_iens][f].value = v
                        do_update = True

        if do_update:
            M.Globals["ERR"].kill()

            # Flags:
            # E - use external formats
            # K - lock the record
            # S - do not clear the row global
            # T - verify the data
            #TODO: I want to do validation, but use internal format throughout
            if self._internal:
                flags = ""
            else:
                flags = "ET"
            M.proc("FILE^DIE", flags, fdaid, "ERR")

            # Check for error
            err = M.Globals["ERR"]
            if err.exists():
                raise FilemanErrorNumber(dierr=err)

        # Pass 2 - inserts
        if len(sf_new_data) > len(sf_live_data):
            inserts = sf_new_data[len(sf_live_data):]
            fda.kill()
            sf_fda = fda[subfile_dd._fileid]
            for i, (iens, row) in enumerate(inserts):
                sf_iens = '+1,%s,' % self._rowid
                for f,v in row.items():
                    sf_fda[sf_iens][f].value = v

            M.Globals["ERR"].kill()

            # Flags:
            # E - use external formats
            # S - do not clear the row global
            if self._internal:
                flags = ""
            else:
                flags = "E"

            ienid = "ien%s" % id(self)
            M.proc("UPDATE^DIE", flags , fdaid, ienid, "ERR")

            # Check for error
            err = M.Globals["ERR"]
            if err.exists():
                raise FilemanErrorNumber(dierr=err)
                
        # pass 3 - deletes
        elif len(sf_new_data) < len(sf_live_data):
            deletes = sf_live_data[len(sf_new_data):]

            M.Globals["Y"].kill()
            for iens, data in deletes:

                # Generate a DA structure for a multiple.
                # see manual, P 2-58
                parts = [x for x in iens.split(",") if x]
                M.Globals["DA"].value = parts[0]
                for i, part in enumerate(parts[1:]):
                    M.Globals["DA"][i+1].value = part

                M.Globals["DIK"].value = self._dd.m_open_form() + "%s,%s," % (self._rowid, parts[-1])
                M.Globals["DA"][i+2].value = self._rowid

                M.proc("^DIK")
                if M.Globals["Y"] == "-1":
                    # I don't know where to look for the error message - Classic API
                    # Sets the flag, but no variables set

                    # This may just mean that the record does not exist
                    raise FilemanError("""DBSRow._update_subfile() : file [%s], fileid = [%s], rowid = [%s]"""
                        % (self._dd.filename, self._dd.fileid, self._rowid))


    def __repr__(self):
        return "<%s.%s %s, rowid=%s>" % (self.__class__.__module__, self.__class__.__name__, self._dd.filename, self._rowid)

    def delete(self):
        """
            I see no clear mechanism for doing deletes in the DBS API.

            There seems to be a call in the "classic" API:

            ^DIK
                    DIK = "The file global - open format"
                    DA = "The entry number in the file"

            TODO: Validate permissions
        """
        if not self._internal:
            raise FilemanError("You must use internal format to modify a file")

        if self._rowid is not None:
            M.Globals["Y"].kill()
            if type(self._rowid) == str and self._rowid.endswith(","):
                # Generate a DA structure for a multiple.
                # see manual, P 2-58
                parts = [x for x in self._rowid.split(",") if x]
                rowid = parts[-1]

                cf = self._dd.m_open_form()
                cf = [x for x in cf.split("(",1)[1].split(",") if x]
                cf.reverse()
                parts = parts + cf

                M.Globals["DA"].value = parts[0]
                for i, part in enumerate(parts[1:]):
                    M.Globals["DA"][i+1].value = part

                # the classic api paths are different that the DBS api paths
                M.Globals["DIK"].value = self._dd.m_open_form() + "%s," % rowid
            else:
                M.Globals["DIK"].value = self._dd.m_open_form()
                M.Globals["DA"].value = str(self._rowid)

            M.proc("^DIK")
            if M.Globals["Y"] == "-1":
                # I don't know where to look for the error message - Classic API
                # Sets the flag, but no variables set

                # This may just mean that the record does not exist
                raise FilemanError("""DBSRow.delete() : FILEMAN Error : file [%s], fileid = [%s], rowid = [%s]"""
                    % (self._dd.filename, self._dd.fileid, self._rowid))


    def traverse(self, fieldname, foreignkeyval=None, fieldnames=None):
        """
            For a pointer field, traverse to the related data

            You can only follow an internal pointer as far as I can see.
        """
        fieldid = self._dd.attrs.get(fieldname, None)
        if fieldid is None:
            raise AttributeError(fieldname)
        field_dd = self._dd.fields[fieldid]

        if foreignkeyval == None:
            if self._internal:
                try:
                    foreignkeyval = str(self._data[fieldid]['I'].value)
                    foreignkeyval = field_dd.pyfrom_internal(foreignkeyval)
                except:
                    raise FilemanError("Value Not found %s" % fieldid)
            else:
                raise FilemanError("""You must be using "Internal" access to traverse to a related file""")
        else:
            foreignkeyval = field_dd.pyfrom_external(foreignkeyval)

        return field_dd.foreign_get(foreignkeyval, internal=True, fieldnames=fieldnames)
