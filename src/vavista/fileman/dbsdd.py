"""
Fileman : Data Dictionary Functions

This is intended to provide a wrapping around the
fileman DBS API. This is a traditional data access API.

To understand the data dictionary, you have to go to the globals.

for file 999900

    zwrite ^DIC(999900,*)
    zwrite ^DD(999900,*)

Type Spec : all fields are optional.

    From programmers manuall 14.9.2

    [*]         - If there is a screen associated with a pointer or set of codes data type.
    [A]         - For multiples, a user entering a new subentry is not Asked for verification.
    [I]         - Data is uneditable
    [M]         - For Multiples, after selecting or adding a subentry, the user is asked for another subentry.
    [R]         - Mandatory
    [P|D|N|F|S|BC|C|Cm|V|K]
                - Field Type:
                    P = pointer to a file
                    D = Datetime
                    N = Numeric
                    F = Text
                    S = Set of Codes
                    C = Computed
                    Cm = Computed Multiline
                    BC = Boolean Computed
                    DC = Date Computed
                    V = Variable Pointer (more data held in fields)
                    K = Mumps Code
                    W = The data is Word-processing.
                    WL = The Word-processing data is normally printed in Line mode (i.e., without word wrap).
                    missing = either WP field or subfile
    [nnn]       - Id of file pointed to or subfile id
    [P]         - if reference is multiple, a subfile is created to 
                  manage the references. Subfile spec describes relationship with real file.
    [Pn]        - The data is a Pointer reference to file "n".
    [Pn']       - LAYGO to the Pointed-to file is not allowed.
    [Jn]        - number spec, n = print length
    [Jn,d]      - number spec, n = print length, d = digits after decimal point
    [X]         - The type spec is not editable. Editing is not allowed under the Modify File Attributes
                    option [DIMODIFY] because INPUT transform has been modified by the Input Transform
                    (Syntax) option [DIITRAN] the Utility Functions menu [DIUTILITY].
    [O]         - The field has an OUTPUT transform.
    [a]         - The field has been marked for auditing all the time.
    [e]         - The auditing is only on edit or delete.
    [x]         - Word-processing text that contains the vertical bar "|" will be displayed exactly as they
                    stored, (i.e., no window processing will take place).

"""

import datetime

from vavista import M

from shared import  FilemanError


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

    def pyfrom_internal(self, s):
        return s

    def pyfrom_external(self, s):
        return s

    def pyto_internal(self, s):
        if s == None:
            s = ""
        return s

    def pyto_external(self, s):
        if s == None:
            s = ""
        return s

class FieldDatetime(Field):
    """
        The formats are hard-coded. I assume that there is an variable
        to control the date formatting. Need to check that when parsing
        TODO: Timezones
        TODO: DST
    """
    fmql_type = FT_DATETIME
    fm_date_format = "%b %d, %Y"
    fm_datetime_format = "%b %d, %Y@%H:%M:%S"

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'D'

    def pyfrom_internal(self, s):
        if not s:
            return None

        # Internal format is YYYMMDD.HHMMSS
        parts = s.split(".")
        yr = int(parts[0][:3]) + 1700
        mth = int(parts[0][3:5])
        day = int(parts[0][5:7])
        if len(parts) > 1:
            hr = int(parts[1][0:2])
            mins = int(parts[1][2:4])
            secs = int(parts[1][4:6])
            d = datetime.datetime(yr,mth,day,hr,mins,secs)
        else:
            d = datetime.date(yr,mth,day)
        return d

    def pyfrom_external(self, s):
        if s == "":
            return None
        if s.find('@') == -1:
            d = datetime.datetime.strptime(s, self.fm_date_format).date()
        else:
            d = datetime.datetime.strptime(s, self.fm_datetime_format)
        return d

    def pyto_internal(self, s):
        if s == None:
            return ""
        if type(s) == datetime.datetime:
            return "%03.3d%02.2d%02.2d.%02.2d%02.2d%02.2d" % (
                    s.year - 1700, s.month, s.day, s.hour, s.minute, s.second)
        else:
            return "%03.3d%02.2d%02.2d" % (s.year - 1700, s.month, s.day)

    def pyto_external(self, s):
        if s == None:
            return ""
        if type(s) == datetime.datetime:
            return s.strftime(self.fm_datetime_format)
        elif type(s) == datetime.date:
            return s.strftime(self.fm_date_format)
        else:
            # Allow T-2 through
            return str(s)

 
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
    format_info = None

    def init_type(self, fieldinfo):
        self.format_info = (18, 8)
        if fieldinfo[1].find("J") != -1:
            ts = fieldinfo[1].split("J",1)[1]
            ts = ts.split(",")
            ts0 = int(ts[0])
            ts1 = []
            if len(ts) > 1:
                for c in ts[1]:
                    if c not in "0123456789": break
                    ts1.append(c)
                if len(ts1) == 0:
                    ts1 = 0
                else:
                    ts1 = int(''.join(ts1))
            self.format_info = ts0, ts1

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'N'

    def pyfrom_internal(self, s):
        if s == "":
            s = 0
        if self.format_info[1]:
            return float(s)
        else:
            return int(s)

    def pyfrom_external(self, s):
        return self.pyfrom_internal(s)

    def pyto_internal(self, s):
        if type(s) == float:
            return "%.*f" % (self.format_info[1], s)
        else:
            return "%d" % s

    def pyto_external(self, s):
        return self.pyto_internal(s)

 
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
    """
        WP fields are stored with a max line length of 245 charachters. Longer lines
        are split.

        TODO: the flag to wrap or not is on the field level record of the subfile.
        ('^DD(9999907.01,.01,0)', 'wp1^Wx^^0;1^Q'),  # no-wrap
        ('^DD(9999907.02,.01,0)', 'wp2^WL^^0;1^Q'),  # word-wrap
    """
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

    def init_type(self, fieldinfo):
        " Extract the wrap specification "
        super(FieldWP, self).init_type(fieldinfo)
        fs = M.Globals["^DD"][fieldinfo[1]][".01"][0].value
        self.wrapinfo = fs.split("^")[1]


    def pyfrom_internal(self, s):
        if s == "":
            return None
        # For the first pass, assume an internal subfile, i.e. the
        # data is stored on as sub-items on the main item.
        # the value s will contain a closed format global to the actual record.
        gl = M.Globals.from_closed_form(s)
        return '\n'.join([v.decode('utf8') for (k,v) in gl.items() if k != 'I'])

    def pyfrom_external(self, s):
        return self.pyfrom_internal(s)

    def pyto_internal(self, s):
        if s is None:
            return []
        rv = []
        for part in s.split('\n'):
            if type(part) == unicode:
                part = part.encode('utf8')
            else:
                pass
            for off in range(0, len(part), 245):
                rv.append(part[off:off+245])
        return rv

    def pyto_external(self, s):
        return self.pyto_internal(s)


class FieldComputed(Field):
    fmql_type = FT_COMPUTED

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'C'

class FieldPointer(Field):
    fmql_type = FT_POINTER
    laygo = True

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'P'

    def init_type(self, fieldinfo):
        " Extract the remote file id "
        super(FieldPointer, self).init_type(fieldinfo)
        self.foreign_fileid = fieldinfo[1][1:]
        if self.foreign_fileid[-1] == "'":
            self.laygo = False
            self.foreign_fileid = self.foreign_fileid[:-1]

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

class _DD(object):
    """
        Load the data dictionary for a FILE
    """
    _fileid = None
    _gl = None
    _indices = _fields = None
    filename = None
    attrs = None

    def __init__(self, filename):
        # the filename cound be a file number - if it starts with a a numeric
        if filename[0] in "0123456789":
            fileid = filename
            dic_header = M.Globals["^DIC"][fileid]["0"].value
            if dic_header is not None:
                filename = dic_header.split("^")[0]
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
            rv = M.mexec('''set s1=$order(^DIC("B",s0,0))''', str(self.filename), M.INOUT(""))[0]
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

            global_name = '^DD(%s,0,"IX","0")' % self.fileid
            prefix = '^DD(%s,0,"IX",' % self.fileid
            while 1:
                global_name = M.mexec('set s0=$query(%s)' % global_name, M.INOUT(""))[0]
                if not global_name or not global_name.startswith(prefix):
                    break
                suffix = global_name[len(prefix):-1]
                parts = suffix.split(",")
                idx_name = parts[0][1:-1]
                idx_table = parts[1]
                idx_columns = parts[2:]
                index = Index(idx_name, idx_table, idx_columns)
                i.append(index)

        return self._indices

    @property
    def new_indices(self):
        """
            TODO: Merge with indices to make something more useful.

            New style indices are stored in an INDEX table
            There is an index "B" which links to the File Id

            GTM>zwrite ^DD("IX","B",200,*)
            ^DD("IX","B",200,3)=""
            ^DD("IX","B",200,5)=""

            GTM>zwrite ^DD("IX",3,*)
            ^DD("IX",3,0)="200^AVISIT^This is a regular index of the remote DUZ and Station number.^R^^R^IR^W^200.06^^^^^S"
        """
        from vavista.fileman import connect
        c = connect("0","")
        f = c.get_file("INDEX")
        rv = []
        for k, rowid in f.traverser("B", from_value=self.fileid, to_value=self.fileid):
            rv.append(f.get(rowid))
        return rv

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

                info = info.split("^", 5) 
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

    def __repr__(self):
        return "<%s.%s (%s=%s)>" % (self.__class__.__module__, self.__class__.__name__, self.fileid, self.filename)

    def m_closed_form(self, rowid):
        """
            Return the closed form for a record from this file.
        """
        if self._gl is None:
            self._gl = str(M.Globals["^DIC"][self.fileid][0]["GL"].value)
        return "%s%s)" % (self._gl, rowid)

    def m_open_form(self):
        """
            Return the closed form for a record from this file.
        """
        if self._gl is None:
            self._gl = str(M.Globals["^DIC"][self.fileid][0]["GL"].value)
        return self._gl


def DD(filename, cache={}):
    """
        Simple mechanism to cache DD objects.
    """
    if filename not in cache:
        file_dd = _DD(filename)
        cache[file_dd.filename] = _DD(filename)
        filename = file_dd.filename
    return cache[filename]
    
