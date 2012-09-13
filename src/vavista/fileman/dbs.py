
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

from clientserver import FilemandClient

class DBSFileRemote:
    """
        Object to handle the communications with one remote file.
        Used in place of DBSFile in the client.
    """
    _description = None
    _fm_description = None
    _fileid = None

    def __init__(self, remote, name, internal=True, fieldnames=None, fieldids=None):
        self.remote = remote
        rv = self.remote.get_file(name=name, internal=internal, fieldnames=fieldnames, fieldids=fieldids)
        self.handle = rv['handle']

    @property
    def description(self):
        if self._description is None:
            self._description = self.remote.dbsfile_description(self.handle)
        return self._description

    @property
    def fileid(self):
        if self._fileid is None:
            self._fileid = self.remote.dbsfile_fileid(self.handle)
        return self._fileid

    @property
    def fm_description(self):
        if self._fm_description is None:
            self._fm_description = self.remote.dbsfile_fm_description(self.handle)
        return self._fm_description

    def get(self, rowid, asdict=False):
        return self.remote.dbsfile_get(self.handle, rowid, asdict=asdict)

    def insert(self, **kwargs):
        return self.remote.dbsfile_insert(self.handle, **kwargs)

    def update(self, **kwargs):
        return self.remote.dbsfile_update(self.handle, **kwargs)

    def lock(self, _rowid, timeout=5):
        return self.remote.dbsfile_lock(self.handle, _rowid=_rowid, timeout=timeout)

    def unlock(self, _rowid):
        return self.remote.dbsfile_unlock(self.handle, _rowid=_rowid)

    def delete(self, _rowid=None, filters=None):
        return self.remote.dbsfile_delete(self.handle, _rowid=_rowid, filters=filters)

    def traverser(self, index, from_value=None, to_value=None, ascending=True, from_rule=None, to_rule=None,
            raw=False, limit=100, offset=None, asdict=False, filters=None, order_by=None):
        return self.remote.dbsfile_traverser(self.handle,
            index, from_value=from_value, to_value=to_value, ascending=ascending,
            from_rule=from_rule, to_rule=to_rule, raw=raw, limit=limit, offset=offset, asdict=asdict,
            filters=filters, order_by=order_by)

    def count(self, limit=None):
        return self.remote.dbsfile_count(self.handle, limit=limit)

    def query(self, limit=100, offset=None, asdict=False, filters=None, order_by=None):
        return self.remote.dbsfile_query(self.handle, limit=limit, offset=offset, asdict=asdict,
            filters=filters, order_by=order_by)

class DBS(object):

    def __init__(self, DUZ, DT, isProgrammer=False, remote=False, host='', port=9010):
        """
        """
        if DUZ is None:
            self.DUZ = "0"
        else:
            self.DUZ = DUZ
        if DT is None:
            self.DT = ""
        else:
            self.DT = DT
        self.isProgrammer = isProgrammer and True or False

        if remote:
            self.remote = FilemandClient(host, port)
            self.remote.connect(DUZ=DUZ, DT=DT, isProgrammer=isProgrammer)
        else:
            self.remote = None

    def list_files(self):
        """
            Oddly, I cannot see how to list the files using the DBS API.
            This is required for debugging etc.
        """
        if self.remote:
            return self.remote.list_files()

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
        if self.remote:
            return DBSFileRemote(self.remote, name, internal=internal, fieldnames=fieldnames, fieldids=fieldids)

        if name.find('::') >= 0:
            # This is a full path name to a subfile. 
            dd = DD(subfile_path=name)
        else:
            # top-level file - dd entry defines the storage.
            dd = DD(name)

        if dd.fileid is None:
            raise FilemanError("""DBS.get_file() : File not found [%s]""" % name)
        return DBSFile(dd, internal=internal, fieldids=fieldids, fieldnames=fieldnames)

    def dd(self, name):
        """
            Provided to expose the data dictionary via api
        """
        return DD(name)

