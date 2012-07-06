
"""
This is intended to provide a wrapping around the
fileman DBS API. This is a traditional data access API.

To understand the data dictionary, you have to go to the globals.

for file 999900

    zwrite ^DIC(999900,*)
    zwrite ^DD(999900,*)

Type Spec : all fields are optional.

    [*]         - the field is screened
    [I]         - something to do with labs - dont know what (TODO)
    [M]         - I see this on multiple entry subfiles - (TODO)
    [R]         - Mandatory
    [P|D|N|F|S|C|V|K]
                - Field Type:
                    P = pointer to a file
                    D = Datetime
                    N = Numeric
                    F = Text
                    S = Set
                    C = Computed
                    V = VPointer (more data held in fields)
                    K = Mumps Code
                    missing = either WP field or subfile
    [nnn]       - Id of file pointed to or subfile id
    [P]         - if reference is multiple, a subfile is created to 
                  manage the references. Subfile spec describes relationship with real file.
    [Jn,m]      - number spec, n = field length, m = digits after decimal point
    [']         - Laygo set to NO
    [X]         - The type spec is not editable
    [O]         - (TODO)
    [a]         - audit
"""

from vavista import M

#---- [ Data Dictionary ]------------------------------------------------------------------

# As per fm22_0um.pdf page 9-3
FT_DATETIME=1
FT_NUMERIC=2
FT_SET=3
FT_TEXT=4
FT_WP=5 
FT_COMPUTED=6
FT_POINTER=7
FT_VPOINTER=8
FT_MUMPS=9 
FT_SUBFILE=10 

class Field(object):
    fmql_type = None
    label = None
    fieldid = None

    # Storage is the global sub-index and either a "PIECE" within that value (1-99)
    # A NUMBER FROM 1 TO 99 OR AN $EXTRACT RANGE (E.G., "E2,4")
    storage = None

    mandatory = False
    screened = False
    details = None
    m_valid = None
    title = None
    fieldhelp = None

    def __init__(self, fieldid, label, fieldinfo):
        self.label = label
        self.fieldid = fieldid
        self._fieldinfo = fieldinfo # should not be used (debugging data)
        try:
            self.storage = fieldinfo[3]
        except:
            pass

        # Use the values in typespec to extract parameterise
        typespec = fieldinfo[1]
        if typespec and typespec[0] == '*':
            self.screened = True
            typespec = typespec[1:]

        if typespec and typespec[0] == 'R':
            self.mandatory = True
            typespec = typespec[1:]

        self.details = fieldinfo[2]
        if len(fieldinfo) > 4 and fieldinfo[4]:
            self.m_valid = fieldinfo[4]
        
        self.init_type(fieldinfo)

    def init_type(self, fieldinfo):
        # Implemented in subclass if required
        pass

    def __str__(self, msgs=[]):
        msgs = [] + msgs
        if self.mandatory: msgs.append("(mandatory)")
        if self.screened: msgs.append("(screened)")
        if self.storage: msgs.append("location=%s" % self.storage)
        if self.details: msgs.append("details=%s" % self.details)
        if self.m_valid: msgs.append("valid=%s" % self.m_valid)
        if self.title: msgs.append("title='%s'" % self.title)
        if self.fieldhelp: msgs.append("fieldhelp='%s'" % self.fieldhelp)
        msgs.append("flags=%s" % self._fieldinfo[1])
        return "%s(%s=%s) %s" % (self.__class__.__name__, self.fieldid, self.label, " ".join(msgs))

    @classmethod
    def isa(cls, flags):
        """
            Determine whether the flags spec provided represents that type of Field

            There are a variable number of flags before the type.
            [*] - field is screened
            [I] - don't know yet - something to do with labs
            [R] - mandatory
        """

        # Strip leading, non-type specific flags
        screened = '*IR'
        for i in range(len(flags)):
            if flags and flags[0] in screened:
                flags = flags[1:]
            else:
                break
        
        return cls.c_isa(flags)

class FieldDatetime(Field):
    fmql_type = FT_DATETIME

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'D'
 
class FieldNumeric(Field):
    """
        Numeric fields have type specifiers - min/max digits
        J18,8 = maximum number of characters 18.
                number after the decimal point 8
                => 9 before it.
        The validation routine describes the min and max values, 

        Example (dumped wiht do ^%G:
            ^DD(999900,6,0)="f7^RNJ18,8^^2;2^K:+X'=X!(X>999999999)!(X<0)!(X?.E1"".""9N.N) X"
    """
    fmql_type = FT_NUMERIC

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'N'
 
class FieldText(Field):
    fmql_type = FT_TEXT

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'F'

class FieldSet(Field):
    fmql_type = FT_SET
    def init_type(self, fieldinfo):
        self.details = [i.split(":",1) for i in fieldinfo[2].split(';') if i]

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'S'

class FieldWP(Field):
    fmql_type = FT_WP

    @classmethod
    def c_isa(cls, flags):
        n=""
        for c in flags:
            if c not in "0123456789.": break
            n = n + c
        if len(n) > 0:
            s0, = M.func("$$VFILE^DILFD", n)
            return s0 == "0"
        return False

class FieldComputed(Field):
    fmql_type = FT_COMPUTED

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'C'

class FieldPointer(Field):
    fmql_type = FT_POINTER

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'P'

class FieldVPointer(Field):
    fmql_type = FT_VPOINTER

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'V'

class FieldMUMPS(Field):
    fmql_type = FT_MUMPS

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'K'

class FieldSubfile(Field):
    fmql_type = FT_SUBFILE

    @classmethod
    def c_isa(cls, flags):
        n=""
        for c in flags:
            if c not in "0123456789.": break
            n = n + c
        if len(n) > 0:
            s0, = M.func("$$VFILE^DILFD", n)
            return s0 != "0"
        return False

# Simple lookup mechanism for parsing fields
FIELD_TYPES = [FieldText, FieldDatetime, FieldNumeric, FieldSet, FieldWP, FieldPointer,
    FieldVPointer, FieldMUMPS, FieldComputed, FieldSubfile]

class Index(object):
    name = table = columns = None
    def __init__(self, name, table, columns):
        self.name = name
        self.table = table
        self.columns = columns
    def __str__(self):
        return "Index(%s) on table %s, columns %s" % (self.name, self.table, self.columns)

class FilemanError(Exception):
    pass

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

class _DD(object):
    """
        Load the data dictionary for a FILE
    """
    _fileid = None
    _indices = _fields = None
    filename = None
    attrs = None

    def __init__(self, filename):
        self.filename = filename

    def _clean_label(self, s):
        "The field names should match those used by fm projection - i.e. uppercase"
        s = s.upper()
        s = ''.join([c for c in s if c in "_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "])
        s = s.replace(' ', '_')
        return s

    @property
    def fileid(self):
        """
            Look up the ^DIC array and find the file number for the specified file, 
            e.g. FILE = 1 - result is a string.
        """
        if self._fileid is None:
            rv = M.mexec('''set s1=$order(^DIC("B",s0,0))''', self.filename, M.INOUT(""))[0]
            if rv != '':
                self._fileid = rv
        return self._fileid

    @property
    def indices(self):
        """
            Return a list of the indices
            # Indices are recorded as

            GTM>zwrite ^DD(200,0,"IX",*)
            ^DD(200,0,"IX","A",200,2)=""
            ^DD(200,0,"IX","A16",200,8980.16)=""
            ^DD(200,0,"IX","AASWB",200,654)=""
        """
        if self._indices is None:
            self._indices = i = []

            indexid = "0"
            global_name = '^DD(%s,0,"IX","0")' % self.fileid
            prefix = '^DD(%s,0,"IX",' % self.fileid
            while 1:
                global_name = M.mexec('set s0=$query(%s)' % global_name, M.INOUT(""))[0]
                if not global_name or not global_name.startswith(prefix):
                    break
                print global_name
                suffix = global_name[len(prefix):-1]
                parts = suffix.split(",")
                idx_name = parts[0][1:-1]
                idx_table = parts[1]
                idx_columns = parts[2:]
                index = Index(idx_name, idx_table, idx_columns)
                i.append(index)

        return self._indices

    @property
    def fields(self):
        """
            Return information about the dd fields
        """
        if self._fields is None:
            M.mexec('set U="^"') # DBS Calls Require this
            f = self._fields = {}
            attrs = self.attrs = {}
            fieldid = "0"
            while 1:
                # Subscript 0 is field description, .1 is the title, 3 is help
                fieldid, info, title, fieldhelp = M.mexec(
                    """set s0=$order(^DD(s2,s0)) Q:s0'=+s0  s s1=$G(^DD(s2,s0,0)),s3=$G(^DD(s2,s0,.1)),s4=$G(^DD(s2,s0,3))""",
                    M.INOUT(str(fieldid)), M.INOUT(""), str(self._fileid), M.INOUT(""), M.INOUT(""))
                if fieldid == "" or fieldid[0] not in "0123456789.":
                    break

                info = info.split("^") 
                label = self._clean_label(info[0])
                try:
                    ftype = info[1]
                except:
                    ftype = None
                if ftype:
                    finst = None
                    for klass in FIELD_TYPES:
                        if klass.isa(ftype):
                            finst = f[fieldid] = klass(fieldid, label, info)
                            attrs[label] = fieldid
                            break
                    if finst is None:
                        print finst, "FIELD [%s], spec [%s] was not identified" % (label, ftype)
                        continue
                    finst.title = title
                    finst.fieldhelp = fieldhelp
                else:
                    assert finst, "FIELD [%s] %s has no fieldspec" % (label, info)

        return self._fields

    def __str__(self):
        rv = ["Data Dictionary for %s (%s)" % (self.filename, self.fileid)]
        for fieldid in sorted(self.fields.keys()):
            rv.append('\t' + str(self.fields[fieldid]))
        for index in self.indices:
            rv.append('\t' + str(index))

        return '\n'.join(rv)


def DD(filename, cache={}):
    """
        Simple mechanism to cache DD objects.
    """
    if filename not in cache:
        cache[filename] = _DD(filename)
    return cache[filename]
    
#---- [ Data Access ]------------------------------------------------------------------

class DBSRow(object):
    """
        This is the key to the whole implementation.  This object maps to a single row
        in the Fileman global. You use it to retrieve fields from that row and to update
        that row. This class has to take care of conversions between Python and M, and
        to ensure that the integrity of the M data store is not violated.

        Access either by id:   row["0.1"]
        or label:              row["name"]

        TODO: lots
        I need to add update logic to the rows so I can write to them
        What about the other values, e.g. subfiles, wp files - do they come across.
    """
    _changed = False
    _dbsfile = None
    _dd = None
    _rowid = None
    _fields = None
    _stored_data = None
    _row_tmpid = None

    def __init__(self, dbsfile, dd, rowid, fieldids):
        self._dbsfile = dbsfile
        self._dd = dd
        self._rowid = rowid

        if fieldids:
            self._fields = dict([(k,v) for (k,v) in dd.fields if v.fieldid in fieldids])
        else:
            self._fields = dd.fields

        # Lazy evaluation
        self._stored_data = None

    def _before_value_change(self, fieldid, global_var, value):
        """
            This should be invoked before the application modifies a variable
            in this row. This function should validate the value, apply any
            formatting rules required and notify the transactional machinary 
            that this object is changed.
        """
        # At this stage, I just want to validate against the data 
        # dictionary. At write time, the data will be fully validated.

        g = M.Globals()
        g["ERR"].kill()

        # Validates single field against the data dictionary
        s0, = M.proc("CHK^DIE", self._dd.fileid, fieldid, "H",
            value, M.INOUT(""), "ERR")

        err = g["ERR"]

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

        return value

    @property
    def _data(self):
        # for lazy evaluation
        if self._stored_data is None:
            self.retrieve()
        return self._stored_data

    @property
    def _iens(self):
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
                rv.append('%s (%s) = "%s"' % (fn, k, v))
        return '\n'.join(rv)

    def keys(self):
        return self._data.keys()

    def items(self):
        return self._data.items()

    def __getitem__(self, fieldid, default=None):
        try:
            return self._data[str(fieldid)]
        except:
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
        fieldid = self._dd.attrs.get(key, None)
        if fieldid is not None:
            self[fieldid].value = value
            return
        raise AttributeError(key)

    def __del__(self):
        # Each time we retrieve a row, it is copied to a temporary store. 
        # This needs to be killed or we have a memory leak in GT.M
        g = M.Globals()
        g[self._row_tmpid].kill()

    def _retrieve(self):
        """
            Retrieve values
            Internal or External
        """
        g = M.Globals()
        g["ERR"].kill()

        self._row_tmpid = "row%s" % id(self)
        M.proc("GETS^DIQ",
            self._dd.fileid,      # numeric file id
            self._iens,           # IENS
            "*",                 # Fields to return TODO
            "N",                # Flags N=no nulls, R=return field names
            self._row_tmpid,
            "ERR")

        # Check for error
        err = g["ERR"]
        if err.exists():
            raise FilemanError("""DBSRow._retrieve() : FILEMAN Error : file [%s], fileid = [%s], rowid = [%s], fieldids = [%s]"""
                % (self._dd.filename, self._dd.fileid, self._rowid, "*"), str(err))

        # Extract the result and store in python variable
        self._stored_data = dict(g[self._row_tmpid][self._dd.fileid][self._iens])
        self._changed = False

        # Add in trigger
        for key, value in self._stored_data.items():
            value._on_before_change = lambda g,v,fieldid=key: self._before_value_change(fieldid, g, v)

    def _update(self):
        """
            Write changed data back to the database.
            
            This is intended to be used during a transaction commit.
        """
        g = M.Globals()
        g["ERR"].kill()

        # Flags:
        # E - use external formats
        # K - lock the record
        # S - do not clear the row global
        # T - verify the data
        M.proc("FILE^DIE", "EKST" , self._row_tmpid, "ERR")

        # Check for error
        err = g["ERR"]
        if err.exists():
            raise FilemanError("""DBSRow._update() : FILEMAN Error : file [%s], fileid = [%s], rowid = [%s]"""
                % (self._dd.filename, self._dd.fileid, self._rowid), str(err))

class DBSFile(object):
    """
        This class provides mechanisms to return rows.
        Currently only "get" is provided which returns one row identified by an id.
        I want to provide search functionality in this class.
    """
    dd = None

    def __init__(self, dd):
        self.dd = dd
        assert (dd.fileid is not None)

    def __str__(self):
        return "DBSFILE %s (%s)" % (self.dd.filename, self.dd.fileid)

    def get(self, rowid, fieldids=None):
        """
            The logic to retrieve and update the row is in the DBSRow class.
            This call constructs a DBSRow class, and verifies that
            the row exists in the database.
        """
        record = DBSRow(self, self.dd, rowid, fieldids=fieldids)
        record._retrieve() # raises exception on failure
        return record
            
class DBS(object):

    def __init__(self, DUZ, DT, isProgrammer=False):
        """
        """
        self.DUZ = DUZ
        self.DT = DT
        self.isProgrammer = isProgrammer

    def list_files(self):
        """
            Oddly, I cannot see how to list the files using the DBS API.
            This is required for debugging etc.
        """
        M.mexec('''set DUZ=s0,U="^"''', self.DUZ)
        if self.isProgrammer:
            M.mexec('''set DUZ(0)="@"''')
        rv = []
        s0 = "0"
        while s0 != "":
            s0, name = M.mexec(
                '''set s0=$order(^DIC(s0)) Q:s0'=+s0  I $D(^DIC(s0,0))&$D(^DIC(s0,0,"GL"))&$$VFILE^DILFD(s0) S s1=$P(^DIC(s0,0),U,1)''',
                M.INOUT(s0), M.INOUT(""))
            if name:
                rv.append((name, s0))
        return rv

    def get_file(self, name):
        """
            Look up the ^DIC array and find the file number for the specified file, 
            e.g. FILE = 1 - result is a string.
        """
        dd = DD(name)
        if dd.fileid is None:
            raise FilemanError("""DBS.get_file() : File not found [%s]""" % name)
        return DBSFile(dd)

    def dd(self, name):
        """
            Provided to expose the data dictionary via api
        """
        return DD(name)
