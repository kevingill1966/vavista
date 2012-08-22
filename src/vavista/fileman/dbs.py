
"""
This is intended to provide a wrapping around the
fileman DBS API. This is a traditional data access API.

This top level provides tools to access files and 
data dictionary.
"""

from vavista import M

from shared import FilemanError
from dbsdd import DD
from dbsfile import DBSFile

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
        M.mset('DUZ',self.DUZ)
        M.mset('U', "^")
        if self.isProgrammer:
            M.mset('DUZ(0)', "@")
        rv = []
        s0 = "0"
        while s0 != "":
            s0, name = M.mexec(
                '''set s0=$order(^DIC(s0)) Q:s0'=+s0  I $D(^DIC(s0,0))&$D(^DIC(s0,0,"GL"))&$$VFILE^DILFD(s0) S s1=$P(^DIC(s0,0),U,1)''',
                M.INOUT(s0), M.INOUT(""))
            if name:
                rv.append((name, s0))
        return rv

    def get_file(self, name, internal=True, fieldids=None, fieldnames=None):
        """
            Look up the ^DIC array and find the file number for the specified file, 
            e.g. FILE = 1 - result is a string.
        """
        dd = DD(name)
        if dd.fileid is None:
            raise FilemanError("""DBS.get_file() : File not found [%s]""" % name)
        return DBSFile(dd, internal=internal, fieldids=fieldids, fieldnames=fieldnames)

    def dd(self, name):
        """
            Provided to expose the data dictionary via api
        """
        return DD(name)
