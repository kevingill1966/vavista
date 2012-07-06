"""
    DBSFile maps to a single file.

    File level features such as retrieve or create a row,
    index traversal should be implemented here.
"""

from dbsrow import DBSRow

class DBSFile(object):
    """
        This class provides mechanisms to return rows.
        Currently only "get" is provided which returns one row identified by an id.
        I want to provide search functionality in this class.
    """
    dd = None
    internal = True
    fieldids = None

    def __init__(self, dd, internal=True, fieldids=None):
        self.dd = dd
        self.internal = internal
        self.fieldids = fieldids
        assert (dd.fileid is not None)

    def __str__(self):
        return "DBSFILE %s (%s)" % (self.dd.filename, self.dd.fileid)

    def get(self, rowid):
        """
            The logic to retrieve and update the row is in the DBSRow class.
            This call constructs a DBSRow class, and verifies that
            the row exists in the database.
        """
        record = DBSRow(self, self.dd, rowid, fieldids=self.fieldids, internal=self.internal)
        record._retrieve() # raises exception on failure
        return record

    def new(self):
        """
            The logic to create a new row.
        """
        record = DBSRow(self, self.dd, rowid=None, fieldids=self.fieldids, internal=self.internal)
        return record
