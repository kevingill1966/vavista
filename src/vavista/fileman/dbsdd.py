"""
Fileman : Data Dictionary Functions

This is intended to provide a wrapping around the
fileman DBS API. This is a traditional data access API.

To understand the data dictionary, you have to go to the globals.

for file 999900

    zwrite ^DIC(999900,*)
    zwrite ^DD(999900,*)

Type Spec : all fields are optional.

    From programmers manual 14.9.2

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

Indices (Traditional):

    The index definitions are stored on the data-dictionary record for the field,
    in a subfile, id = 1, example from the newperson file...

        ('^DD(200,.01,1,0)', '^.1^^-1')
        ('^DD(200,.01,1,1,0)', '200^B')
        ('^DD(200,.01,1,1,1)', 'S ^VA(200,"B",$E(X,1,30),DA)=""')
        ('^DD(200,.01,1,1,2)', 'K ^VA(200,"B",$E(X,1,30),DA)')

        ('^DD(200,.01,1,2,0)', '200^AE^MUMPS')
        ('^DD(200,.01,1,2,1)', 'S X1=$P($G(^VA(200,DA,1)),"^",8) I X1="" S $P(^VA(200,DA,1),"^",7,8)=DT_"^"_DUZ')
        ('^DD(200,.01,1,2,2)', 'Q')
        ('^DD(200,.01,1,2,3)', 'Stuffing Creator and date')
        ('^DD(200,.01,1,2,"%D",0)', '^^1^1^2990617^^')
        ('^DD(200,.01,1,2,"%D",1,0)', 'This X-ref stuffs the DATE ENTERED and CREATOR fields on a new entry.')
        ('^DD(200,.01,1,2,"DT")', '2990617')

        ('^DD(200,.01,1,5,0)', '200^BS5^MUMPS')
        ('^DD(200,.01,1,5,1)', 'Q:$P($G(^VA(200,DA,1)),U,9)\']""  S ^VA(200,"BS5",$E(X,1)_$E($P(^(1),U,9),6,9),DA)=""')
        ('^DD(200,.01,1,5,2)', 'Q:$P($G(^VA(200,DA,1)),U,9)\']""  K ^VA(200,"BS5",$E(X,1)_$E($P(^(1),U,9),6,9),DA)')
        ('^DD(200,.01,1,5,3)', 'Special BS5 lookup X-ref')
        ('^DD(200,.01,1,5,"%D",0)', '^^3^3^2990617^^')
        ('^DD(200,.01,1,5,"%D",1,0)', "This X-ref builds the 'BS5' X-ref on name changes.")
        ('^DD(200,.01,1,5,"%D",2,0)', 'The BS5 is the first letter of the last name concatinated with the last')
        ('^DD(200,.01,1,5,"%D",3,0)', 'four digits of the SSN.')

    The logic of the indices appears to be imperative. These are triggers to be executed
    when the field is changed, i.e. Cross references, so not all entries in this subfile
    are indices.

    There is a second listing of cross references in the DD, 

        ('^DD(200,0,"IX","AC",200,.01)', '')
        ('^DD(200,0,"IX","AC",200,14.9)', '')
        ('^DD(200,0,"IX","AE",200,.01)', '')
        ('^DD(200,0,"IX","AG",200,.01)', '')
        ('^DD(200,0,"IX","AH",200,.01)', '')
        ('^DD(200,0,"IX","ASX",200,.01)', '')
        ('^DD(200,0,"IX","B",200,.01)', '')     ** VALID **
        ('^DD(200,0,"IX","BS5",200,.01)', '')
        ('^DD(200,0,"IX","MA",200,.01)', '')
        ...

    There is no-data in these indexes where they do not match with cross-references on 
    the field.

    AND OF COURSE THERE IS THE NEW STYLE INDEXES !!

"""

import datetime

from vavista import M

from shared import  FilemanError, valid_rowid


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

def leading_number(val):
    """
        Extract the leading number from a string.
        Terminate when an non-numeric is reached
    """
    n=""
    for c in val:
        if c not in "0123456789.":
            break
        n = n + c
    return n

def strip_leading_chars(val):
    """
        Strip the leading characters from a string, so that the number is located
    """
    for i, c in enumerate(val):
        if c in "0123456789.":
            return val[i:]
    return ""

class Field(object):
    fmql_type = None
    label = None
    fieldid = None

    # Storage is the global sub-index and either a "PIECE" within that value (1-99)
    # A NUMBER FROM 1 TO 99 OR AN $EXTRACT RANGE (E.G., "E2,4")
    storage = None

    mandatory = False
    prompt_for_more = False
    screened = False
    details = None
    m_valid = None
    title = None
    fieldhelp = None
    fileid = None

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

        if typespec and typespec[0] == 'M':
            self.prompt_for_more = True
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

    def describe(self):
        rv = {}
        rv['mandatory'] = self.mandatory
        rv['screened'] = self.screened
        rv['storage'] = self.storage
        rv['details'] = self.details
        rv['m_valid'] = self.m_valid
        rv['title'] = self.title
        rv['fieldhelp'] = self.fieldhelp
        rv['fieldinfo'] = self._fieldinfo
        rv['flags'] = self._fieldinfo[1]
        rv['name'] = self.label
        rv['fieldid'] = self.fieldid
        rv['fmql_type'] = self.fmql_type
        return rv

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
        screened = '*IRM'
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

    def foreign_get(self, s, internal=True):
        """
            Return the foreign record for this field.
        """
        raise FilemanError("""No Foreign File for this class""", self.__class__)

    def validate_insert(self, s, internal=True):
        """
            Mandatory field checking.
        """
        if self.mandatory and (s is None or s == ""):
            raise FilemanError("""Field is mandatory""")

    def fm_validate_insert(self, value):
        """
            This validator checks the field using the Fileman logic.
            Since it expects value to be in Fileman External format
            and we are using Internal Format, it is of limited use.

            Also, I don't know how it will work on a sub-file.
        """
        M.Globals["ERR"].kill()

        # Validates single field against the data dictionary
        s0, = M.proc("CHK^DIE", self.fileid, self.fieldid, "H",
            value, M.INOUT(""), "ERR")

        err = M.Globals["ERR"]

        # s0 should contain ^ for error, internal value for valid data
        if s0 == "^":
            error_code = err['DIERR'][1].value
            error_msg = '\n'.join([v for k,v in err['DIERR'][1]['TEXT'].items()])
            help_msg = [v for k,v in err['DIHELP'].items()]

            raise FilemanError(
                """DBSDD.fm_validate_insert(): fileid = [%s], fieldid = [%s], value = [%s], error_code = [%s], error_msg = [%s], help = [%s]"""
                % (self.fileid, self.fieldid, value, error_code, error_msg, help_msg))

        # If err exists, then some form of programming error
        if err.exists():
            raise FilemanError("""DBSDD.fm_validate_insert(): fileid = [%s], fieldid = [%s], value = [%s], err = [%s]"""
                % (self.fileid, self.fieldid, value, '\n'.join(err)))

    def retrieve(self, gl_rec, cache):
        """
            Retrieve the item from the global. If there is a cache,
            check to see if the required record is in the cache
            before pulling it from M
        """
        storage = self.storage

        # Fieldid .001 is an odd case. This field provides use overrides
        # on default rules for the rowid field. As such, no storage is specified
        # for now, I am ignoring this field.
        if storage == ' ':
            return gl_rec.path[-1]

        gl_id, value = None, None
        gbl, piece =  storage.split(';')
        gl_piece = gl_rec[gbl]
        if cache != None:
            gl_id = gl_piece.closed_form
            value = cache.get(gl_id)
        if value == None:
            value = gl_piece.value
            if cache != None:
                cache[gl_id] = value
        if not value:
            return None
        if piece.startswith('E'):
            # Extract Storage - programmers manual 15.3.2
            e_off, e_end = piece[1:].split(',')
            return value[int(e_off) -1: int(e_end)]
        else:
            parts = value.split("^")
            piece = int(piece)
            if len(parts) < piece:
                return None
            return parts[piece-1]

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

    def describe(self):
        rv = super(FieldDatetime, self).describe()
        return rv

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
            try:
                hr = int(parts[1][0:2])
            except:
                hr = 0
            try:
                mins = int(parts[1][2:4])
            except:
                mins = 0
            try:
                secs = int(parts[1][4:6])
            except:
                secs = 0
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

    def describe(self):
        rv = super(FieldNumeric, self).describe()
        rv['format_info'] = self.format_info
        return rv

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
        if s == None or s == "":
            s = 0
        if self.format_info[1]:
            return float(s)
        else:
            return int(s)

    def pyfrom_external(self, s):
        return self.pyfrom_internal(s)

    def pyto_internal(self, s):
        if type(s) == float:
            f = s
            if self.format_info[1]:
                return "%.*f" % (self.format_info[1], f)
            return "%d" % f
        else:
            # Verify that this is a valid number
            try:
                f = float(s)
            except:
                raise FilemanError("""[%s] is not a valid number""" % s)
            if self.format_info[1]:
                return "%.*f" % (self.format_info[1], f)
            return "%d" % f

    def pyto_external(self, s):
        return self.pyto_internal(s)

    def validate_insert(self, s, internal=True):
        """
            Verify that the code is a valid code.

            I limit the validation to whether the input value is a numeric.
            I silently ignore rounding problems. The reason for this is that
            floating point numbers often don't round well,

            >>> 1.1 % 1 == .1
            False

        """
        super(FieldNumeric, self).validate_insert(s, internal)  # mandatory check
        if s:
            try:
                float(s)
            except:
                raise FilemanError("""[%s] is not a valid number""" % s)

class FieldText(Field):
    fmql_type = FT_TEXT

    @classmethod
    def c_isa(cls, flags):
        if len(flags) == 0:
            # name field can be defined without a type.
            return True
        return flags and flags[0] == 'F'


class FieldSet(Field):
    fmql_type = FT_SET
    def init_type(self, fieldinfo):
        self.details = [i.split(":",1) for i in fieldinfo[2].split(';') if i]

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'S'

    def validate_insert(self, s, internal=True):
        """
            Verify that the code is a valid code.
        """
        super(FieldSet, self).validate_insert(s, internal)  # mandatory check
        if s and s not in [d[0] for d in self.details]:
            valid = []
            for k,v in self.details:
                valid.append("%s=%s" % (k, v))
            raise FilemanError("""Value [%s] is not valid. must be one of: %s""" % (s, ", ".join(valid)))

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
        n = leading_number(flags)
        if len(n) > 0:
            s0, = M.func("$$VFILE^DILFD", n)
            return s0 == "0"
        return False

    def init_type(self, fieldinfo):
        " Extract the wrap specification "
        super(FieldWP, self).init_type(fieldinfo)
        subfileid = leading_number(strip_leading_chars(fieldinfo[1]))
        fs = M.Globals["^DD"][subfileid][".01"][0].value
        self.wrapinfo = fs.split("^")[1]

    def pyfrom_internal(self, s):
        if s == "":
            return None
        if type(s) == list:
            return '\n'.join([rec.decode('utf8') for rec in s])

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

    def retrieve(self, gl_rec, cache):
        """
            Retrieve the WP field the global. 
            Ignoring cache for now
        """
        rv = []

        storage = self.storage
        gl_id, value = None, None
        gbl, piece =  storage.split(';')
        wp_file = gl_rec[gbl]

        for (recno, value) in wp_file.keys_with_decendants():
            if recno > '0': # can have odd header values.
                rv.append(wp_file[recno][0].value)

        return rv

class FieldComputed(Field):
    fmql_type = FT_COMPUTED

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'C'

class FieldPointer(Field):
    fmql_type = FT_POINTER
    laygo = True

    _ffile = None
    _dd = None

    def describe(self):
        rv = super(FieldPointer, self).describe()
        rv['laygo'] = self.laygo
        return rv

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'P'

    def init_type(self, fieldinfo):
        " Extract the remote file id "
        super(FieldPointer, self).init_type(fieldinfo)
        self.foreign_fileid = leading_number(strip_leading_chars(fieldinfo[1]))
        if fieldinfo[1].find("'") != -1:
            self.laygo = False

    def foreign_get(self, s, internal=True, fieldnames=None):
        """
            Retrieve a remote record using a Pointer type
        """
        if self._ffile and not fieldnames:
            ff = self._ffile
        else:
            # cannot use the cached version, as it may not consistent fieldnames
            from vavista.fileman.dbsfile import DBSFile
            ff = DBSFile(self.dd, internal=internal, fieldnames=fieldnames)
            if not fieldnames:
                self._ffile = ff
        return ff.get(s)

    @property
    def dd(self):
        if self._dd is None:
            self._dd = DD(self.foreign_fileid)
        return self._dd

    def validate_insert(self, s, internal=True):
        """
            Prevent insert if remote record does not exist.

            This is important since we are using internal format
        """
        super(FieldPointer, self).validate_insert(s, internal)  # mandatory check
        if s and internal: # external validated via pyto_exernal()
            remote_gl = self.dd.m_open_form()
            cf = remote_gl + str(s) + ")"
            if not M.Globals.from_closed_form(cf).exists():
                raise FilemanError("""Remote record [%s] does not exist. Missing global=[%s]""" % (s, cf))

    def pyto_external(self, s):
        """
            I need to retrieve the remote record, so that fileman can un-retrieve it !!!
        """
        if s:
            remote_gl = self.dd.m_open_form()
            cf = remote_gl + str(s) + ")"
            g = M.Globals.from_closed_form(cf)
            if not g.exists():
                raise FilemanError("""Remote record [%s] does not exist. Missing global=[%s]""" % (s, cf))
            value = g[0].value
            s = value.split("^", 1)[0]
        return s

    def pyto_internal(self, s):
        """

        """
        return s

class FieldVPointer(Field):
    """
        VPOINTER types reference one of a number of remote files. The data is stored
        in the format "1;DIZ(9999921,", so that the remote record header ^DIZ(9999921,1,0)
        can be queried quickly without understanding the data dictionary.

        This format is ugly. I map the internal format to 

            VP1.1 where VP1 is the configured ID for the file DIZ(9999921,

    """
    fmql_type = FT_VPOINTER

    # map fieldid to fileid, prompt text, prefix, laygo? ?, openform
    remotefiles = None

    # map open form version of file root to prefix
    of_map = None
    _ffile = None
    _dd = None

    def describe(self):
        rv = super(FieldVPointer, self).describe()
        rv['remotefiles'] = self.remotefiles
        rv['of_map'] = self.of_map
        return rv

    def init_type(self, fieldinfo):
        " Extract the remote file specifications "
        super(FieldVPointer, self).init_type(fieldinfo)
        self.remotefiles = remotefiles = {}
        self.of_map = of_map = {}
        self._ffile = {}
        self._dd = {}

        for remotefileid in [k for k, v in M.Globals["^DD"]["9999923"][1]["V"].keys_with_decendants() if k[0] in "123456789"]:
            spec = M.Globals["^DD"]["9999923"][1]["V"][remotefileid][0].value.split("^")

            # Now I need the remote file dd so that I can look up the Global for that file
            file_dd = DD(spec[0])
            open_form = file_dd.m_open_form()
            of_map[open_form] = spec[3]

            # remote file information
            remotefiles[spec[3]] = (spec[0], spec[1], spec[2], spec[3], spec[4], open_form)

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'V'

    def pyfrom_internal(self, s):
        if s == "":
            return None

        key, remote_gl = s.split(";", 1)
        rf = self.remotefiles[self.of_map["^" + remote_gl]][3]
        return "%s.%s" % (rf, key)

    def pyto_internal(self, s):
        """
            I am doing validation here - where else?
            TODO: Move the validation to insert logic.
        """
        if s is None:
            return ""
        ref, key = s.split(".", 1)
        remote_gl = self.remotefiles[ref][5]
        return "%s;%s" % (key, remote_gl[1:])

    def validate_insert(self, s, internal=True):
        """
            Verify that an insert / modify attempt is valid. 

            Ensure that the foreign key exists.
            
            This works on the Mumps Internal format.

            For efficiency, generate the closed form of the target, and look-up
            based on that, rather than navigating via a file.
        """
        super(FieldVPointer, self).validate_insert(s, internal)  # mandatory check
        if s:
            key, remote_gl = s.split(";", 1)

            cf = "^" + remote_gl + str(key) + ")"
            if not M.Globals.from_closed_form(cf).exists():
                raise FilemanError("""Remote record [%s] does not exist. Missing global=[%s]""" % (s, cf))

    def foreign_get(self, s, internal=True, fieldnames=None):
        """
            Retrieve a remote record using a VPointer type

            s is in the "internal" format of the vpointer.
            This is not the stored value.

        """
        ref, key = s.split(".", 1)

        if ref in self._ffile and not fieldnames:
            ffile = self._ffile[ref]
        else:
            from vavista.fileman.dbsfile import DBSFile
            foreign_fileid = self.remotefiles[ref][0]
            dd = DD(foreign_fileid)
            ffile = DBSFile(dd, internal=internal, fieldnames=fieldnames)
            if not fieldnames:
                self._ffile[ref] = ffile
        return ffile.get(key)

class FieldMUMPS(Field):
    fmql_type = FT_MUMPS

    @classmethod
    def c_isa(cls, flags):
        return flags and flags[0] == 'K'

    def validate_insert(self, s, internal=True):
        """
            No need to validate the mumps code here. It is validated 
            via the low-level DBS code on a commit.
        """
        super(FieldMUMPS, self).validate_insert(s, internal)  # mandatory check

        # Ask Fileman to validate the value
        self.fm_validate_insert(s)

class FieldSubfile(Field):
    fmql_type = FT_SUBFILE
    _dd = None
    _subfileid = None

    def describe(self):
        dd = self.dd
        rv = super(FieldSubfile, self).describe()
        rv['subfileid'] = self.subfileid
        rv['children'] = dd.describe()
        return rv

    @classmethod
    def c_isa(cls, flags):
        n = leading_number(strip_leading_chars(flags))
        if len(n) > 0:
            s0, = M.func("$$VFILE^DILFD", n)
            return s0 != "0"
        return False

    @property
    def subfileid(self):
        if self._subfileid == None:
            self._subfileid = leading_number(strip_leading_chars(self._fieldinfo[1]))
        return self._subfileid

    @property
    def dd(self):
        if self._dd is None:
            self._dd = DD(self.subfileid)
        return self._dd

    @property
    def fields(self):
        """
            Return a list of the fields in the subfile.
        """
        return [f[1] for f in sorted(self.dd.fields.items())]

    def retrieve(self, gl_rec, cache, fields=None, asdict=False):
        """
            Retrieve a subfile - the entire subfile is returned.
            Fields is the list of fields of interest.
        """
        rv = []

        storage = self.storage
        gl_id, value = None, None
        gbl, piece =  storage.split(';')
        sub_file = gl_rec[gbl]

        # Ignore the header for now.

        # Gets hit with nested subfiles.
        if fields == None:
            asdict = True
            fields = self.fields

        fieldnames = [f.label for f in fields]

        for (rowid, value) in sub_file.keys_with_decendants():
            if not valid_rowid(rowid):
                break
            row = []
            sub_file_row = sub_file[rowid]
            for field in fields:
                row.append(field.retrieve(sub_file_row, cache))
            if asdict:
                row = dict(zip(fieldnames, row))
                row['_rowid'] = rowid
            rv.append(row)

        return rv


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
    def __unicode__(self):
        return str(self)

class AttrResolver:
    """
        A name "T1" maps to a file field
        A name "T1->T2" maps to a subfile field - as a list
    """
    def __init__(self, dd):
        self._dd = dd
    def __getitem__(self, k):
        if k.find("->") == -1:
            res = self._dd.fieldnames[k.lower()]
            if self._dd.fields[res].fmql_type == FT_SUBFILE:
                #return (res, ".01")
                return res
            else:
                return res
        f_field, sf_field = k.split('->')
        f_field = self._dd.fieldnames[f_field.lower()]
        dd = self._dd.fields[f_field].dd
        sf_field = dd.attrs[sf_field]
        return (f_field, sf_field)
    def get(self, k, default=None):
        try:
            return self.__getitem__(k)
        except:
            return default

class _DD(object):
    """
        Load the data dictionary for a FILE
    """
    _fileid = None
    _cache_gl = None
    _indices = _fields = None
    _new_indices = None
    filename = None
    fieldnames = None
    _attrs = None
    parent_dd = None
    parent_fieldid = None

    def __init__(self, filename, parent_dd=None, parent_fieldid=None):
        """
            If this is a subfile, there is no DIC entry. Pass in the parent
            dd and the parent fieldid to work out the DIC information.
        """

        # the filename cound be a file number - if it starts with a a numeric
        if filename[0] in ".0123456789":
            self._fileid = fileid = filename
            if parent_dd:
                self.parent_dd = parent_dd
                self.parent_fieldid = parent_fieldid

                # for a subfile there is no-name.
                filename = ""

                # work out the base global from the parent
                #parent_gl = str(M.Globals["^DIC"][self.fileid][0]["GL"].value)
                self._cache_gl = parent_dd._gl + str(parent_fieldid) + ","

            elif M.Globals["^DD"][fileid][0]["UP"].exists():
                # Subfile being composed directly. - I don't know the field
                # id in the parent.
                parent_fileid = M.Globals["^DD"][fileid][0]["UP"].value
                parent_dd = self.parent_dd = DD(parent_fileid)

                for fieldid, field in parent_dd.fields.items():
                    if field.fmql_type == FT_SUBFILE and field.subfileid == fileid:
                        self.parent_fieldid = fieldid
                        break

            elif M.Globals["^DIC"][fileid]["0"].exists():
                dic_header = M.Globals["^DIC"][fileid]["0"].value
                filename = dic_header.split("^")[0]
        self.filename = filename

    def _clean_label(self, s):
        "The field names should match those used by fmql - i.e. lowercase"
        s = s.lower()
        s = ''.join([c for c in s if c in "_abcdefghijklmnopqrstuvwxyz0123456789 "])
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

    def indices_for_column(self, colname):
        """
            Return the indices which index the named column
        """
        rv = []

        fieldid = self.attrs[colname]

        for index in self.indices:
            if index.columns[0] == fieldid:
                rv.append(index)

        # TODO: New indices

        return rv

    @property
    def indices(self):
        """
            Return a list of the indices

            To be valid, the index must be both in the IX

            ^DD(200,0,"IX","AASWB",200,654)=""

        """
        if self._indices is None:
            i = []

            # TODO: this is not right for multi-column keys
            # TODO: new style indexes

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

            # A second list, gives indices for a field
            columns = {}
            for idx in i:
                for c in idx.columns:
                    columns[c] = 1

            # Now trawl the listed columns in the data dictionary, and load their
            # cross references.
            cr_names = {}
            for c in columns.keys():
                idx_root = M.Globals["^DD"][self.fileid][c][1]
                if not idx_root[0].exists():
                    continue
                for cr_id, val in idx_root.keys_with_decendants():
                    if float(cr_id) > 0:
                        cr_header = idx_root[cr_id][0].value
                        parts = cr_header.split("^")
                        if len(parts) == 2 and parts[1]:   # if more than 2 parts, assume MUMPs trigger
                            f = cr_names.get(parts[1], list())
                            f.append(c)
                            cr_names[parts[1]] = f

            # Now, just delete items from the index list if they are not in cr_names
            self._indices = []
            for index in i:
                cr = cr_names.get(index.name)
                if cr:
                    # verify columns - lots of errors in real systems
                    if len(cr) == len(index.columns):
                        invalid = False
                        for c in cr:
                            if c not in index.columns:
                                invalid = True
                                continue
                        if not invalid:
                            self._indices.append(index)

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
        if self._new_indices is None:
            from vavista.fileman import connect
            c = connect("0","")
            f = c.get_file("INDEX")
            self._new_indices = list(f.traverser("B", from_value=self.fileid, to_value=self.fileid))
        return self._new_indices

    @property
    def attrs(self):
        """name/id map"""
        if self._attrs is None:
            self._init_fields()
            self._attrs = AttrResolver(self)
        return self._attrs

    @property
    def fields(self):
        """
            Return information about the dd fields
        """
        if self._fields is None:
            self._init_fields()
        return self._fields

    def _init_fields(self):
        """
            Return information about the dd fields
        """
        if self._fields is None:
            M.mset('U', "^") # DBS Calls Require this
            f = self._fields = {}
            attrs = self.fieldnames = {}
            fieldid = "0"
            while 1:
                # Subscript 0 is field description, .1 is the title, 3 is help
                fieldid, info, title, fieldhelp = M.ddwalk(self._fileid, fieldid)
                #fieldid, info, title, fieldhelp = M.mexec(
                #    """set s0=$order(^DD(s2,s0)) Q:s0'=+s0  s s1=$G(^DD(s2,s0,0)),s3=$G(^DD(s2,s0,.1)),s4=$G(^DD(s2,s0,3))""",
                #    M.INOUT(str(fieldid)), M.INOUT(""), str(self._fileid), M.INOUT(""), M.INOUT(""))
                if fieldid == "" or fieldid[0] not in "0123456789.":
                    break

                info = info.split("^", 4) 
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
                            klass.fileid = self.fileid
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
        if self._gl.endswith(","):
            return "%s%s0)" % (self._gl, rowid)
        else:
            return "%s%s)" % (self._gl, rowid)

    def m_open_form(self):
        """
            Return the closed form for a record from this file.
        """
        return self._gl

    @property
    def _gl(self):
        if self._cache_gl == None:
            self._cache_gl = str(M.Globals["^DIC"][self.fileid][0]["GL"].value)
        return self._cache_gl

    def describe(self, fieldids=None):
        """
            Return a description of the table. This is to support the
            generation of forms.
        """
        if not fieldids:
            fieldids = self.fields.keys()
        fieldids.sort()
            
        fields = []
        for fieldid in fieldids:
            fi = self.fields[fieldid].describe()
            fi['fieldhelp2'] = self.fieldhelp2(fieldid)
            fields.append(fi)

        return {'fields': fields, 'fileid': self.fileid, 'filename': self.filename,
                'description': self.filedescription()}

    def fieldhelp2(self, fieldid):
        """
            Text displayed for two '??'
        """
        txt = []
        dd_desc = M.Globals["^DD"][self.fileid][fieldid][21]
        for k,v in dd_desc.keys_with_decendants():
            txt.append(dd_desc[k][0].value)
        return '\n'.join(txt)

    def filedescription(self):
        """
            Grab the file description from the ^DIC record
            M.Globals['^DIC'][fileid]['%D'].keys_with_decendants()
            see: fm programmers guide 14.3
        """
        txt = []
        dd_desc = M.Globals["^DIC"][self.fileid]['%D']
        for k,v in dd_desc.keys_with_decendants():
            v = dd_desc[k][0].value
            if v:
                txt.append(v)
        return '\n'.join(txt)

def DD(filename, parent_dd=None, parent_fieldid=None, cache={}):
    """
        Simple mechanism to cache DD objects.
    """
    dd = cache.get(filename)

    if dd is None or (parent_dd and parent_fieldid and (dd.parent_dd != parent_dd or parent_fieldid != dd.parent_fieldid)):
        file_dd = _DD(filename, parent_dd=parent_dd, parent_fieldid=parent_fieldid)
        cache[file_dd.fileid] = file_dd
        if file_dd.filename:
            cache[file_dd.filename] = file_dd
    return cache[filename]
    
