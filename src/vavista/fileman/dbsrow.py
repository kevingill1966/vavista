"""
    The DBSRow maps to a single Fileman record.

    The data retrieval, insert, update and delete logic belongs here.
"""

from vavista import M
from vavista.fileman.dbsdd import FT_WP, DD, FT_SUBFILE
from shared import FilemanError, ROWID, STRING
from transaction import transaction_manager as transaction

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
                    child = parent._dd.fields[child]
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
        if rowid is None or tmpid is not None:
            self._save_global(M.Globals[self._row_tmpid][self._dd.fileid][self._iens])
        elif type(rowid) == str and rowid.startswith("+"):
            self._save_global(M.Globals[self._row_tmpid][self._dd.fileid][self._iens])

    def _before_value_change(self, fieldid, global_var, value):
        """
            This should be invoked before the application modifies a variable
            in this row. This function should validate the value, apply any
            formatting rules required and notify the transactional machinary 
            that this object is changed.
        """
        # At this stage, I just want to validate against the data 
        # dictionary. At write time, the data will be fully validated.

        if not self._internal:
            M.Globals["ERR"].kill()

            # Validates single field against the data dictionary
            s0, = M.proc("CHK^DIE", self._dd.fileid, fieldid, "H",
                value, M.INOUT(""), "ERR")

            err = M.Globals["ERR"]

            # s0 should contain ^ for error, internal value for valid data
            if s0 == "^":
                error_code = err['DIERR'][1].value
                error_msg = '\n'.join([v for k,v in err['DIERR'][1]['TEXT'].items()])
                help_msg = [v for k,v in err['DIHELP'].items()]

                # Invalid data - get the error from the ERR structure
                raise FilemanValidationError(filename = self._dd.filename, row = self._rowid, 
                        fieldid = fieldid, value = value, error_code = error_code, error_msg = error_msg,
                        err = err, help=help_msg)

            # If err exists, then some form of programming error
            if err.exists():
                raise FilemanError("""DBSRow._set_value(): file [%s], fileid = [%s], rowid = [%s], fieldid = [%s], value = [%s]"""
                    % (self._dd.filename, self._dd.fileid, self._rowid, fieldid, value), str(err))

        if not self._changed:
            self._lock()
            transaction.join(self)

        if fieldid not in self._changed_fields:
            self._changed_fields.append(fieldid)

        return value

    def _lock(self, timeout=5):
        """
            Lock a global (path to a row).
            This functionality is here so that transaction management
            can remove locks on a commit, abort
        """
        if self._locked: return

        if self._rowid: # nothing to lock

            g_path = self._dd.m_closed_form(self._rowid)

            # Set the timeout
            M.Globals["DILOCKTM"].value = timeout

            # use DILF^LOCK function to perform the lock
            M.proc("LOCK^DILF", g_path)

            # result is returned in $T
            rv, = M.mexec("set l0=$T", M.INOUT(0))
            if rv != 1:
                raise FilemanLockFailed(filename=self._dd.filename, row=self._rowid, timeout=timeout)
            self._locked = 1

    def _unlock(self):
        # Locking is done via an M level routine on the record global
        if self._locked:
            g_path = self._dd.m_closed_form(self._rowid)
            M.mexec(str("LOCK -%s" % g_path))   # TODO: mexec to take unicode
            self._locked = False
            
    def _on_commit(self):
        if self._rowid and not (type(self._rowid) == str and self._rowid.startswith("+")):
            self._update()
        else:
            self._insert()
            
    def _on_abort(self):
        pass

    def _on_after_commit(self):
        self._unlock()
        self._changed = False
        self._changed_fields = []
            
    def _on_after_abort(self):
        self._unlock()
        self._changed = False
        self._changed_fields = []

        #   Any data in the object is dirty 
        #   this should force it to reload if it is accessed again
        if self._changed:
            self._stored_data = None

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

    def keys(self):
        return self._data.keys()

    def items(self):
        return self._data.items()

    def _field_from_id(self, fieldid):
        """
            Given a fieldid, return the data dictionary object.
        """
        return self._dd._fields[fieldid]

    def __getitem__(self, fieldid, default=''):
        value = self._getitem(fieldid, default=default).value
        field = self._field_from_id(fieldid)
        if self._internal:
            return field.pyfrom_internal(value)
        else:
            return field.pyfrom_external(value)
        
    def _getitem(self, fieldid, default=''):
        """
            Return a field using array notation

            print record[.01]

            Item does not exist, but is a valid fieldid, insert it.
            This occurs on an insert. The inserted field does not
            affect the transaction tracking.
        """
        fieldid = str(fieldid)
        try:
            if self._internal:
                return self._data[fieldid]['I']
            else:
                return self._data[fieldid]
        except:
            if fieldid in self._fieldids:
                v = M.Globals[self._row_tmpid][self._dd.fileid][self._iens][fieldid]
                v.value = default
                self._stored_data[fieldid] = v
                if self._internal:
                    v['I']._on_before_change = lambda g,v,fieldid=fieldid: self._before_value_change(fieldid, g, v)
                else:
                    v._on_before_change = lambda g,v,fieldid=fieldid: self._before_value_change(fieldid, g, v)
                return self._getitem(fieldid)

            raise FilemanError("""DBSRow (%s=%s): invalid attribute error""" %
                (self._dd.fileid, self._dd.filename), fieldid)

    def __getattr__(self, key):
        """
            Called for misses
        """
        fieldid = self._dd.attrs.get(key, None)
        if fieldid is not None:
            return self[fieldid]
        raise AttributeError(key)

    def __setattr__(self, key, value):
        """
            called by:

                record.FIELD = 4

            If FIELD exists, set its value
            If FIELD does not exist, and is in the data dictionary, create it.
            If FIELD does not exist, and is not the data dictionary, raise exception.
        """
        if key[0] not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789":
            return super(DBSRow, self).__setattr__(key, value)

        if not self._internal:
            raise FilemanError("You must use internal format to modify a file")

        fieldid = self._dd.attrs.get(key, None)
        if fieldid is not None:
            field = self._field_from_id(fieldid)
            if self._internal:
                mvalue = field.pyto_internal(value)
            else:
                mvalue = field.pyto_external(value)


            # Special case for WP fields. If the return value is a list,
            # then it is inserted as fields within the main field, and the
            # path to it is stored in the value.

            # See fm22_0pm.pdf, page 194
            if type(mvalue) == list and field.fmql_type == FT_WP:
                external_path = self._getitem(fieldid)
                self._getitem(fieldid).value = external_path.closed_form
                for i in range(len(mvalue)):
                    node = external_path[str(i+1)]
                    node.value = mvalue[i]
            else:
                field.validate_insert(mvalue, self._internal)
                self._getitem(fieldid).value = mvalue
            return
        raise AttributeError(key)

    def __del__(self):
        # Each time we retrieve a row, it is copied to a temporary store. 
        # This needs to be killed or we have a memory leak in GT.M
        if M == None:
            return # happens during process exit
        M.Globals[self._row_tmpid].kill()
        if self._row_fdaid:
            M.Globals[self._row_fdaid].kill()

    def _retrieve(self):
        """
            Retrieve values
            Internal or External
        """
        M.Globals["ERR"].kill()

        flags = 'N'    # no nulls
        if self._internal:
            flags = flags + "I"

        if self._dd.parent_dd:
            # TODO: this is not quite worked out - rowid in the parent and the subfile
            #       are mixed up this is not quite worked out - rowid in the parent and the subfile
            #       are mixed up
            fileid = self._dd.parent_dd.fileid
            iens = self._iens
            fieldids = str(self._dd.parent_fieldid) + "**"
        else:
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
            #fieldids = ";".join([k for (k,v) in self._fields.items() if v.fmql_type not in [FT_SUBFILE]])
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
            raise FilemanError("""DBSRow._retrieve() : FILEMAN Error : file [%s], fileid = [%s], iens = [%s], fieldids = [%s]"""
                % (self._dd.filename, fileid, iens, fieldids), str(err))

        #self._save_global(M.Globals[self._row_tmpid][self._dd.fileid][self._iens])
        try:
            self._save_tmp_global()
        except Exception, e:
            print e
            import pdb; pdb.post_mortem()

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
        print root

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
                    sf_result.append(data[fieldid[1]])

        self._changed = False
        self._changed_fields = []


    def _save_global(self, gl):
        """
            Extract the result and store in python variable
            I need to revisit this. It only copies the data when using
            external access.
        """
        if self._internal:
            # need to retrieve a sub-field
            self._stored_data = {}
            for k, v in gl.keys_with_decendants():
                self._stored_data[k] = gl[k]
        else:
            self._stored_data = dict(gl)

        self._changed = False
        self._changed_fields = []

        # Add in trigger
        for key, value in self._stored_data.items():
            if self._internal:
                value['I']._on_before_change = lambda g,v,fieldid=key: self._before_value_change(fieldid, g, v)
            else:
                value._on_before_change = lambda g,v,fieldid=key: self._before_value_change(fieldid, g, v)

    def _retrieve_subfile(self, fieldid, subfile_dd, subfile):
        """
            Retrieve all the rows of a subfile (multiple field)
        """
        M.Globals["ERR"].kill()

        flags = 'N'    # no nulls
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
            raise FilemanError("""DBSRow._retrieve_subfile() : FILEMAN Error : file [%s], fileid = [%s], iens = [%s], fieldids = [%s]"""
                % (self._dd.filename, fileid, iens, fieldids), str(err))

        # Extract the result and store in rows.
        subfile_data = M.Globals[tmpid][subfile_dd._fileid]

        rv = []
        for iens in [r[0] for r in subfile_data.keys_with_decendants()]:
            row = DBSRow(subfile, subfile_dd, iens, tmpid=tmpid)
            rv.append(row)

        return rv

    def _create_fda(self, values=None):
        """
            For the current record, copy all changed fields to an FDA record
            (Fileman Data Array), see programmer manual 3.2.3

            FDA_ROOT(FILE#,"IENS",FIELD#)="VALUE"

        """
        subfile_rowcounts = {}
        subfile_sizes = {}
        subfile_extensions = {}
        subfile_deletions = {}
        self.adjust_subfiles = None

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

                    # MULTIPLES
                    #
                    # Where the number in the multiple being updated is different from
                    # the original, there are problems. Will have to do a delete/insert
                    parent, child = field
                    subfileid = parent.subfileid
                    sf_fieldid = fieldid[1]

                    # Ensure that updates / inserts to a subfile on multiple keys, use
                    # the same key lengths for each
                    if fieldid[0] in subfile_sizes:
                        if len(value) != subfile_sizes[fieldid[0]]:
                            raise FilemanError("Insert/Update - subfile entry sizes for subfile %s do not match" % subfileid)

                    subfile_sizes[fieldid[0]] = len(value)
                    if self._rowid:
                        # If the value is longer than the current value
                        # in the sub-file, the extra values cannot be added
                        # in the update. A subsequent insert must be used.
                        #sfdd = DD(fieldid[0],
                        #def DD(filename, parent_dd=None, parent_fieldid=None, cache={}):
                        import pdb; pdb.set_trace()
                        if fieldid[0] in subfile_rowcounts:
                            rowcount = subfile_rowcounts[fieldid[0]]
                        else:
                            sfdd = DD(subfileid, parent_dd=self._dd, parent_fieldid=fieldid[0])
                            cf = sfdd._gl + "%s,0)" % self._rowid
                            sf_header = M.Globals.from_closed_form(cf).value
                            rowcount = sf_header.split("^")[3]
                            if rowcount == '':
                                subfile_rowcounts[fieldid[0]] = rowcount = 0
                            else:
                                subfile_rowcounts[fieldid[0]] = rowcount = int(rowcount)

                        if rowcount < len(value):
                            if fieldid[0] not in subfile_extensions:
                                subfile_extensions[fieldid[0]] = []
                            subfile_extensions[fieldid[0]].append((fieldid, value[rowcount:]))
                            value = value[:rowcount]
                        elif rowcount > len(value):
                            subfile_deletions[fieldid[0]] = len(value) - rowcount

                    for sf_rowid, row in enumerate(value):
                        sf_iens = "%d,%s," % (sf_rowid+1, self._rowid)
                        if self._internal:
                            fda[subfileid][sf_iens][sf_fieldid].value = row
                        else:
                            fda[subfileid][sf_iens][sf_fieldid].value = row

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

                else:
                    if self._internal:
                        mvalue = field.pyto_internal(value)
                    else:
                        mvalue = field.pyto_external(value)
                    field.validate_insert(mvalue, self._internal)
                    fda[fileid][iens][fieldid].value = mvalue
        else:
            for fieldid in self._changed_fields:
                fda[fileid][iens][fieldid].value = self._getitem(fieldid).value

        if subfile_extensions or subfile_deletions:
            # only affects update
            print subfile_extensions
            print subfile_deletions
            self.adjust_subfiles = (subfile_extensions, subfile_deletions)

        return row_fdaid

    def _insert(self, values=None):
        """
            Create a new record

            This is intended to be used during a transaction commit.
            TODO: that concept is invalid - GT.M should manage transactions.

            UPDATE^DIE(FLAGS,FDA_ROOT,IEN_ROOT,MSG_ROOT)
        """
        M.Globals["ERR"].kill()

        # Create an FDA format array for fileman
        fdaid = self._create_fda(values)
        ienid = "ien%s" % id(self)

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

            raise FilemanError("""DBSRow._update() : FILEMAN Error : file [%s], fileid = [%s], rowid = [%s]"""
                % (self._dd.filename, self._dd.fileid, self._rowid), str(err))
        
        # What is the id of the new record?
        self._rowid = int(M.Globals[ienid]['1'].value)
        self._stored_data = None
        return self._rowid

    def _update(self, values=None):
        """
            Write changed data back to the database.
            
            This is intended to be used during a transaction commit.

            TODO: dbsdd validation for fields
        """
        M.Globals["ERR"].kill()

        # Create an FDA format array for fileman
        fdaid = self._create_fda(values)

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
            raise FilemanError("""DBSRow._update() : FILEMAN Error : file [%s], fileid = [%s], rowid = [%s]"""
                % (self._dd.filename, self._dd.fileid, self._rowid), str(err))

        # TODO: This logic is wrong. It assumes that the subfile records are contiguous.
        #       Instead, I should wipe the subfiles and insert them every time.
        if self.adjust_subfiles:
            (subfile_extensions, subfile_deletions) = self.adjust_subfiles 
            for key, value in subfile_extensions.items():
                self._extend_subfile(key, value)
            for key, value in subfile_deletions.items():
                self._contract_subfile(key, value)

    def __repr__(self):
        return "<%s.%s %s, rowid=%s>" % (self.__class__.__module__, self.__class__.__name__, self._dd.filename, self._rowid)

    def _extend_subfile(self, fieldid, values):
        """
            Add new records to an existing subfile.
            values is a list if (fieldid, sf_fieldid), value tuples
        """
        import pdb; pdb.set_trace()

    def _contract_subfile(self, fieldid, count):
        """
            delete records from an existing subfile.
        """
        import pdb; pdb.set_trace()

    def delete(self):
        """
            I see no clear mechanism for doing deletes in the DBS API.

            There seems to be a call in the "classic" API:

            ^DIK
                    DIK = "The file global - open format"
                    DA = "The entry number in the file"

            TODO: Queue for Txn Commit
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

    def subfile_cursor(self, fieldname):
        """
            Provide a cursor to traverse a multi field.
            Multi fields can have numerous attributes, so they cannot be translated to simple list. 

            Subfiles are stored in the main file. They have a logical id, so that a 
            data dictionary can be stored, but they are not stored under this id.
        """
        from vavista.fileman.dbsfile import DBSFile

        fieldid = self._dd.attrs.get(fieldname, None)
        if fieldid is None:
            raise AttributeError(fieldname)

        field_dd = self._dd.fields[fieldid]
        glindex = field_dd.storage.split(';')[0]

        # get the file header from the subfile
        gl = self._dd.m_open_form()
        header_gl = gl + str(self._rowid) + "," + glindex +",0)"
        if M.Globals.from_closed_form(header_gl).exists():
            subfile_header = M.Globals.from_closed_form(header_gl).value
            filename, subfileid_with_flags, lastnum, rowcount = subfile_header.split("^")
            if not rowcount or int(rowcount) == 0:
                return []

            # Create dbsrows for each record in the sub-file
            subfileid = ''.join([c for c in subfileid_with_flags if c in "0123456789."])
            subfile_dd = DD(subfileid, parent_dd=self._dd, parent_fieldid=fieldid)
            subfile = DBSFile(subfile_dd, internal=self._dbsfile.internal)

            return self._retrieve_subfile(fieldid, subfile_dd, subfile)

        else:
            return []

    def subfile_new(self, fieldname):
        """
            Create a new record in a sub-file.
        """
        from vavista.fileman.dbsfile import DBSFile

        fieldid = self._dd.attrs.get(fieldname, None)
        if fieldid is None:
            raise AttributeError(fieldname)

        # get the file header from the subfile
        gl = self._dd.m_open_form()
        header_gl = gl + str(self._rowid) + "," + str(fieldid) +",0)"

        subfile_header = M.Globals.from_closed_form(header_gl).value
        filename, subfileid, lastnum, rowcount = subfile_header.split("^")
        if not rowcount or int(rowcount) == 0:
            return []

        subfile_dd = DD(subfileid, parent_dd=self._dd, parent_fieldid=fieldid)
        subfile = DBSFile(subfile_dd, internal=self._dbsfile.internal)

        # this is not terribly clear - the iens should be
        # +1,rowid, I think the order is the reverse order of the path
        return DBSRow(subfile, subfile_dd, rowid='+1,%s,' % fieldid)

