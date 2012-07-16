
vavista.M (api abstracting access to M globals and Code)
========================================================

Calling Code
------------

The module provides three functions for calling M code from your python code.::

    mexec : low level function - requires an understanding of mumps to use it
    proc  : execute a mumps "procedure". 
    func  : execute a mumps "func". 

Two "marker" classes are also provided::

    INOUT : marks the parameter as being a return value also
    REF : marks the parameter as being used as a reference rather than a value

func
----

Calls M functions. The return value is a tuple containing the return value and
all of the INOUT parameters.

Intrinsic Function::

        func("$ASCII", "Beethoven")
        #returns ("66",)

The above call invokes the M intrinsic function "ASCII" (intrinsic functions are 
called with a "$" prefix). The return code is the only item in the resulting tuple.

Extrinsic Function::

        func("$$testfunc^vavistagtm", INOUT(""), INOUT(0), INOUT(0.0))
        #returns ("99", "testfunc", 3333, 444.44)

This code invokes an "extrinsic" function, i.e. one defined in the application
(intrinsic functions are called with a "$$" prefix). This code return the return
value of the function, plus three parameters which have been configured as return
values.

proc
----

Executes an M procedure. An M procedure is an executable block of M code. It 
starts with a label (may take parameters), and returns with a "quit". There is
no return value.

Example::

        proc("testproc^vavistagtm", INOUT(""), INOUT(0), INOUT(0.0))
        #returns ("testproc", 1111, 222.22)

mexec
-----

Executes a piece of M code, using the M xecute command. The code is limited to
a single line.

Example::

        mexec('write "Hello World!",!')

Both proc and func use the mexec call to execute code.

Any parameters passed to the mexec command are passed on to the context in which
the code is executed. The parameters are interpreted left to right. In the M context
the parameters are named s0-s7 for strings, l0-l7 for longs and d0-d7 for doubles.
These are the only types handled at the moment. TODO: unicode.

Example (with parameter)::

        mexec('write "s0=",s0,", l0=",l0,", d0=",d0!', "A string", 11, 3.14)

Example (set a value)::

        mexec('set MYVAR="derefme"')

Example (get a value)::

        mexec('set s0=MYVAR', INOUT(""))
        # returns ("derefme",)

References
----------

For the proc() and func() functions, there is a facility to mark a parameter as a
reference rather than a value.

Example::

        mexec('set MYVAR="derefme"')
        rv = proc("testref^vavistagtm", INOUT(""), REF("MYVAR"))
        # returns ("derefme", ) 

Globals
-------

M language variables are a multi-level key-value store. By default variables are
global. If the variable name is prefix with a "^", that variable is persistent.

Examples::

        set A=1         # simple global
        set ^A=1        # simple global (but persistent)
        set A(0,0,0)=1  # value stored in a deep decendent of the global

The Globals class abstracts the access to the global variables in M.

Example::

    TODO: Finish this section.
    
        # Note globals commencing with ^ are persistent
        # all indexes are strings

        from = vista.M import Globals
        Globals['^gl']['0'].keys()        # only keys with values
        Globals['^gl']['0'].items()       # key / value return (no decendant info)
        Globals['^gl']['0'].value         # single value
        Globals['^gl']['0'].has_value()
        Globals['^gl']['0'].has_decendants()
        Globals['^gl']['0'].keys_with_decendants()

        Globals['^gl']['0'].kill()         # kill value and decendants

