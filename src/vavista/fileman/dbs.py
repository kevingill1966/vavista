
# This is intended to provide a wrapping around the
# fileman DBS API. This is a traditional data access API.

from vavista import M

class FilemanError(Exception):
    pass

class DBSFile(object):
    name = None
    fileid = None
    def __init__(self, name, fileid):
        # TODO - data dictionary
        self.name = name
        self.fileid = fileid
    def get(self, rowid, fieldids=None):
        """
            Retrieve a row using the rowid.
            Return a dictionary of fieldids mapped to values
            If fieldids is not None, then we are only interested in the
            named fields.
            Do not load sub-tables.

            TODO: lots
        """
        M.mexec("kill ROW,ERR")
        iens = str(rowid) + ","
        M.proc("GETS^DIQ",
            self.fileid,         # numeric file id
            iens,                # IENS
            "*",                 # Fields to return
            "NR",                # Flags N=no nulls, R=return field names
            "ROW",
            "ERR")

        # Check for error
        g = M.Globals()
        err = g["ERR"]
        if err.exists():
            raise FilemanError("""DBSFIle.get() : FILEMAN Error processing request \nfileid = [%s], rowid = [%s], fieldids = %s"""
                % (self.fileid, rowid, fieldids), str(err))

        # Return result
        result = g["ROW"][self.fileid][iens]
        return result.items()

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
    def fileid(self, name):
        """
            Look up the ^DIC array and find the file number for the specified file, 
            e.g. FILE = 1 - result is a string.
        """
        rv = M.mexec('''set s1=$order(^DIC("B",s0,0))''', name, M.INOUT(""))[0]
        if rv == '':
            return None
        return rv
    def get_file(self, name):
        """
            Look up the ^DIC array and find the file number for the specified file, 
            e.g. FILE = 1 - result is a string.
        """
        fileid = self.fileid(name)
        if fileid is None:
            raise FilemanError("""DBS.get_file() : File not found [%s]""" % name)
        return DBSFile(name, fileid)


