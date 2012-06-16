
vavista

The concept is to create a python namespace containing tools for working with
vista. The initial tools will be to call mumps code from python, and to 
read, write, lock and delete globals.

import vavista.M                # Mumps API
import vavista.fileman          # work with fileman files (new APIs and utilities)

vista.M
-------

    Low Level API

        vista.M.exec(Code, parameters)
        vista.M.proc(ProcedureName, parameters)
        vista.M.func(FunctionName, parameters)

    High Level API

        # Note globals commencing with ^ are persistent
        # all indexes are strings

        g = vista.M.Globals()
        g['^gl']['0'].keys()        # only keys with values
        g['^gl']['0'].items()       # key / value return (no decendant info)
        g['^gl']['0'].value         # single value
        g['^gl']['0'].has_value()
        g['^gl']['0'].has_decendants()
        g['^gl']['0'].keys_with_decendants()
        g['^gl']['0'].items_with_decendants()

        g['^gl']['0'].kill()         # kill value and decendants

