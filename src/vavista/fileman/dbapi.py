#
# This is a second attempt at a fileman API.
#
# My first attempt worked on an active record pattern. The
# attempt had a couple of problems. Most noticably that getting
# to serialise over a network connection would be tedious
#
# I am restarting based on a DBAPI complaint apprach. In this 
# first pass, I will implement a very trivial interface, with
# a view to maybe using gadfly or MySQL storage as a platform
# in the future
#
# http://www.python.org/dev/peps/pep-0249/
#
# I want to model the multiple fields on the postgres arrays

class Cursor:
    """
        These objects represent a database cursor, which is used to manage
        the context of a fetch operation. Cursors created from the same
        connection are not isolated, i.e., any changes done to the
        database by a cursor are immediately visible by the other
        cursors. Cursors created from different connections can or can not
        be isolated, depending on how the transaction support is
        implemented (see also the connection's .rollback() and .commit()
        methods).
    """

    @property
    def description (self):
        """
          
            This read-only attribute is a sequence of 7-item
            sequences.  

            Each of these sequences contains information describing
            one result column: 

              (name, 
               type_code, 
               display_size,
               internal_size, 
               precision, 
               scale, 
               null_ok)

            The first two items (name and type_code) are mandatory,
            the other five are optional and are set to None if no
            meaningful values can be provided.

            This attribute will be None for operations that
            do not return rows or if the cursor has not had an
            operation invoked via the .execute*() method yet.
            
            The type_code can be interpreted by comparing it to the
            Type Objects specified in the section below.
        """

    @property
    def rowcount(self):
        """
            This read-only attribute specifies the number of rows that
            the last .execute*() produced (for DQL statements like
            'select') or affected (for DML statements like 'update' or
            'insert'). [9]
            
            The attribute is -1 in case no .execute*() has been
            performed on the cursor or the rowcount of the last
            operation is cannot be determined by the interface. [7]

            Note: Future versions of the DB API specification could
            redefine the latter case to have the object return None
            instead of -1.
        """

    def close(self):
        """
            Close the cursor now (rather than whenever __del__ is
            called).  The cursor will be unusable from this point
            forward; an Error (or subclass) exception will be raised
            if any operation is attempted with the cursor.
        """
            
    def execute(self, operation, parameters=None):
        """
            Prepare and execute a database operation (query or
            command).  Parameters may be provided as sequence or
            mapping and will be bound to variables in the operation.
            Variables are specified in a database-specific notation
            (see the module's paramstyle attribute for details). [5]
            
            A reference to the operation will be retained by the
            cursor.  If the same operation object is passed in again,
            then the cursor can optimize its behavior.  This is most
            effective for algorithms where the same operation is used,
            but different parameters are bound to it (many times).
            
            For maximum efficiency when reusing an operation, it is
            best to use the .setinputsizes() method to specify the
            parameter types and sizes ahead of time.  It is legal for
            a parameter to not match the predefined information; the
            implementation should compensate, possibly with a loss of
            efficiency.
            
            The parameters may also be specified as list of tuples to
            e.g. insert multiple rows in a single operation, but this
            kind of usage is deprecated: .executemany() should be used
            instead.
            
            Return values are not defined.
        """

    def executemany(self, operation, seq_of_parameters):
        """
            Prepare a database operation (query or command) and then
            execute it against all parameter sequences or mappings
            found in the sequence seq_of_parameters.
            
            Modules are free to implement this method using multiple
            calls to the .execute() method or by using array operations
            to have the database process the sequence as a whole in
            one call.
            
            Use of this method for an operation which produces one or
            more result sets constitutes undefined behavior, and the
            implementation is permitted (but not required) to raise 
            an exception when it detects that a result set has been
            created by an invocation of the operation.
            
            The same comments as for .execute() also apply accordingly
            to this method.
            
            Return values are not defined.
        """

    def fetchone(self):
        """
          
            Fetch the next row of a query result set, returning a
            single sequence, or None when no more data is
            available. [6]
            
            An Error (or subclass) exception is raised if the previous
            call to .execute*() did not produce any result set or no
            call was issued yet.
        """

    def fetchmany(self, size=None)
        """
            Fetch the next set of rows of a query result, returning a
            sequence of sequences (e.g. a list of tuples). An empty
            sequence is returned when no more rows are available.
            
            The number of rows to fetch per call is specified by the
            parameter.  If it is not given, the cursor's arraysize
            determines the number of rows to be fetched. The method
            should try to fetch as many rows as indicated by the size
            parameter. If this is not possible due to the specified
            number of rows not being available, fewer rows may be
            returned.
            
            An Error (or subclass) exception is raised if the previous
            call to .execute*() did not produce any result set or no
            call was issued yet.
            
            Note there are performance considerations involved with
            the size parameter.  For optimal performance, it is
            usually best to use the arraysize attribute.  If the size
            parameter is used, then it is best for it to retain the
            same value from one .fetchmany() call to the next.
        """

    def fetchall(self) :
        """
            Fetch all (remaining) rows of a query result, returning
            them as a sequence of sequences (e.g. a list of tuples).
            Note that the cursor's arraysize attribute can affect the
            performance of this operation.
            
            An Error (or subclass) exception is raised if the previous
            call to .execute*() did not produce any result set or no
            call was issued yet.
        """

    arraysize = 1
        """
            This read/write attribute specifies the number of rows to
            fetch at a time with .fetchmany(). It defaults to 1
            meaning to fetch a single row at a time.
            
            Implementations must observe this value with respect to
            the .fetchmany() method, but are free to interact with the
            database a single row at a time. It may also be used in
            the implementation of .executemany().
        """

    def setinputsizes(self, sizes):
        """
            This can be used before a call to .execute*() to
            predefine memory areas for the operation's parameters.
            
            sizes is specified as a sequence -- one item for each
            input parameter.  The item should be a Type Object that
            corresponds to the input that will be used, or it should
            be an integer specifying the maximum length of a string
            parameter.  If the item is None, then no predefined memory
            area will be reserved for that column (this is useful to
            avoid predefined areas for large inputs).
            
            This method would be used before the .execute*() method
            is invoked.
            
            Implementations are free to have this method do nothing
            and users are free to not use it.
        """
        pass
            
    def setoutputsize(self, size ,column=None)
        """

            Set a column buffer size for fetches of large columns
            (e.g. LONGs, BLOBs, etc.).  The column is specified as an
            index into the result sequence.  Not specifying the column
            will set the default size for all large columns in the
            cursor.
            
            This method would be used before the .execute*() method
            is invoked.
            
            Implementations are free to have this method do nothing
            and users are free to not use it.
        """
        pass
            
class Connection:

    """
        Establish a database connection.
    """
    DUZ = None
    DT = None
    isProgrammer = None
    def __init__(self, DUZ, DT, isProgrammer=False):
        """
            For now there is no security. I think I need to manage security
            on a per-request / connected user / session basis, but web
            applications normally use a single login.

            todo other network type stuff
        """
        self.DUZ = DUZ
        self.DT = DT
        self.isProgrammer = isProgrammer

    def close(self):
        """
            Close the connection now (rather than whenever __del__ is
            called).  The connection will be unusable from this point
            forward; an Error (or subclass) exception will be raised
            if any operation is attempted with the connection. The
            same applies to all cursor objects trying to use the
            connection.  Note that closing a connection without
            committing the changes first will cause an implicit
            rollback to be performed.
        """
        pass

    def commit(self):
        """
            Commit any pending transaction to the database. Note that
            if the database supports an auto-commit feature, this must
            be initially off. An interface method may be provided to
            turn it back on.
            
            Database modules that do not support transactions should
            implement this method with void functionality.
        """
        pass

    def rollback(self):
        """
            This method is optional since not all databases provide
            transaction support. [3]
            
            In case a database does provide transactions this method
            causes the database to roll back to the start of any
            pending transaction.  Closing a connection without
            committing the changes first will cause an implicit
            rollback to be performed.
        """
        pass

    def cursor(self):
        """
            Return a new Cursor Object using the connection.  If the
            database does not provide a direct cursor concept, the
            module will have to emulate cursors using other means to
            the extent needed by this specification.  [4]
        """
        pass


def connect(DUZ, DT, isProgrammer=False):
    """
        Constructor for creating a connection to the database.
        Returns a Connection Object. It takes a number of
        parameters which are database dependent. [1]
    """
    return Connection(DUZ, DT, isProgrammer)



apilevel='2.0'
"""
    String constant stating the supported DB API level.
    Currently only the strings '1.0' and '2.0' are allowed.
    
    If not given, a DB-API 1.0 level interface should be
    assumed.
"""

threadsafety=1
"""
    Integer constant stating the level of thread safety the
    interface supports. Possible values are:

        0     Threads may not share the module.
        1     Threads may share the module, but not connections.
        2     Threads may share the module and connections.
        3     Threads may share the module, connections and
              cursors.

    Sharing in the above context means that two threads may
    use a resource without wrapping it using a mutex semaphore
    to implement resource locking. Note that you cannot always
    make external resources thread safe by managing access
    using a mutex: the resource may rely on global variables
    or other external sources that are beyond your control.
"""

paramstyle
"""
    String constant stating the type of parameter marker
    formatting expected by the interface. Possible values are
    [2]:

        'qmark'         Question mark style, 
                        e.g. '...WHERE name=?'
        'numeric'       Numeric, positional style, 
                        e.g. '...WHERE name=:1'
        'named'         Named style, 
                        e.g. '...WHERE name=:name'
        'format'        ANSI C printf format codes, 
                        e.g. '...WHERE name=%s'
        'pyformat'      Python extended format codes, 
                        e.g. '...WHERE name=%(name)s'
"""
