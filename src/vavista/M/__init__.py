
import os

# If the GTMCI environment variable is not configured, then set it to
# point to the callin file. Otherwise assume that the user knows what
# they are doing.
if os.getenv("GTMCI") == None:
    GTMCI = __path__[0].rsplit("/",1)[0] + "/_gtm/calltab.ci"
    os.putenv("GTMCI", GTMCI)

gtmroutines = os.getenv("gtmroutines", "")
if gtmroutines.find("vavista/src/_gtm") == -1:
    gtmroutines = gtmroutines + " " + __path__[0].rsplit("/",1)[0] + "/_gtm"
    os.putenv("gtmroutines", gtmroutines)

# At some later stage will try cache and gtm
import vavista._gtm as _mumps

INOUT=_mumps.INOUT
mexec=_mumps.mexec
    
class Global(object):
    def __init__(self, path):
        self.path = path
    def __getitem__(self, key):
        return Global(self.path + [str(key)])
    @property
    def value(self):
        if len(self.path) == 1:
            s0 = self.path[0]
        else:
            s0 = '%s("%s")' % (self.path[0], '","'.join(self.path[1:]))
        s0, = mexec("set s0=@s0", INOUT(s0))
        return s0

class Globals(object):
    """
    The Globals class provides a starting point for accessing Mumps Globals
    (both temporary and persistent) from python code.

    g = Globals()
    g.keys()   # list the globals
    g['^donkey'] # return a global object representing the path ^donkey
    """
    def keys(self, include_tmp=False):
        pers, tmp = [], []
        s0 = "^%"
        while 1:
            s0, = mexec("set s0=$order(@s0)", INOUT(s0))
            if not s0:
                break
            pers.append(s0)
        if include_tmp:
            # TODO: this is returning the stack variables not the globals
            s0 = "%"
            while 1:
                s0, = mexec("set s0=$order(@s0)", INOUT(s0))
                if not s0:
                    break
                tmp.append(s0)
        return pers + tmp

    def __getitem__(self, key):
        # TODO - key must begin with ^ or alpha numeric
        return Global([key])


