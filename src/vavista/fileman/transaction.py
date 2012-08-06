"""
    Fileman Transaction Management

    The fileman api works by copying a record from a mumps global
    area to an application global array and decoding the stored
    record to a standard array. The extracted area can be edited
    using standard array notation.

    At some point the programmer decides to write changes
    back to the stored record. At this point fileman validation,
    security rules, auditing rules, indexing rules, computations
    etc are executed. The data is converted from "external" to
    "internal" format and written to the appropriate global.

    This transaction manager is the python tool for tracking
    changed objects and writing them back to the global store.

    It would be nice to do consistency checking to ensure that
    another user has not updated data in-between. Perhaps locks?

    When an DBSRow object changes (or is save()'ed, i.e. inserted)
    a flag on that record _changed is set, and it "joins" the
    transaction.

    The row must implemented two methods:
        _on_abort:  Called to execute the abort
        _on_commit: Called to execute the commit

    Other methods which may be implemented:
        _on_before_abort:  called prior to abort
        _on_after_abort:   called after to abort
        _on_before_commit:  called prior to commit
        _on_after_commit:   called after to commit

    The transaction manager supports hooks:

        on_before_XXXXX, on_after_XXXXX
        These are called before and after begins, commits and aborts.

"""

from vavista import M

class TransactionManager:
    tracking = []
    in_transaction = False
    transaction_id = None

    # Hooks for application logic
    on_before_begin, on_after_begin = [], []
    on_before_commit, on_after_commit = [], []
    on_before_abort, on_after_abort = [], []

    def begin(self, label="python"):
        "It is not necessary to call this"
        if self.in_transaction:
            return # called implicitly

        for fn in self.on_before_begin: fn() # hooks

        assert len(self.tracking) == 0, "There have been some changes before the begin call"

        self.transaction_id = M.tstart(label)
        self.in_transaction = True

        for fn in self.on_after_begin: fn() # hooks

    def join(self, dbrow):
        if not self.in_transaction:
            self.begin()

        assert hasattr(dbrow, "_on_abort"), "Managed object must have abort handler"
        assert hasattr(dbrow, "_on_commit"), "Managed object must have commit handler"

        self.tracking.append(dbrow)
        dbrow._changed = True

    def abort(self):
        try:
            for fn in self.on_before_abort: fn() # hooks

            for dbrow in self.tracking:
                row_handler = getattr(dbrow, "_on_before_abort", None)
                if row_handler:
                    row_handler()
            for dbrow in self.tracking:
                row_handler = getattr(dbrow, "_on_abort", None)
                if row_handler:
                    row_handler()

            if self.in_transaction:
                M.trollback()
                self.in_transaction = False

            for dbrow in self.tracking:
                row_handler = getattr(dbrow, "_on_after_abort", None)
                if row_handler:
                    row_handler()

            for fn in self.on_after_abort: fn() # hooks
        finally:
            self.tracking = []

    def commit(self):
        try:
            try:
                for fn in self.on_before_commit: fn() # hooks

                for dbrow in self.tracking:
                    row_handler = getattr(dbrow, "_on_before_commit", None)
                    if row_handler:
                        row_handler()
                for dbrow in self.tracking:
                    dbrow._on_commit()

                for dbrow in self.tracking:
                    row_handler = getattr(dbrow, "_on_after_commit", None)
                    if row_handler:
                        row_handler()
            except Exception, e:
                M.trollback()
                raise

            if self.in_transaction:
                M.tcommit()

            for fn in self.on_after_commit: fn() # hooks
        finally:
            self.tracking = []
            self.in_transaction = False


# Singleton
transaction_manager = TransactionManager()
