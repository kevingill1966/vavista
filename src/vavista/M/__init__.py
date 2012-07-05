
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

class REF(object):
    def __init__(self, value):
        self.value = value

def proc(procedure, *inparams):
    iStr, iInt, iDbl = 0, 0, 0
    params = []
    callparams = []
    for p in inparams:
        inout, var, ref = "", "", ""
        if type(p) == REF:
            p = p.value
            ref = "@"
        if type(p) == INOUT:
            v = p.value
            inout = "."
        else:
            v = p
        if type(v) == str:
            # TODO: unicode
            var = "s%d" % iStr
            iStr += 1
        elif type(v) in [int, long]:
            var = "l%d" % iInt
            iInt += 1
        elif type(v) == float:
            var = "d%d" % iDbl
            iDbl += 1
        else:
            assert(0)
        params.append(p)
        callparams.append("%s%s%s" % (ref, inout, var))

    if len(callparams):
        cmd = "do %s(%s)" % (procedure, ",".join(callparams))
    else:
        cmd = "do %s" % procedure
    #print "mexec(", cmd, params, ")"
    return mexec(cmd, *params)
    

def func(procedure, *inparams):
    iStr, iInt, iDbl = 1, 0, 0
    params = [INOUT("")]
    callparams = []
    for p in inparams:
        inout, var, ref = "", "", ""
        if type(p) == REF:
            p = p.value
            ref = "@"
        if type(p) == INOUT:
            v = p.value
            inout = "."
        else:
            v = p
        if type(v) == str:
            # TODO: unicode
            var = "s%d" % iStr
            iStr += 1
        elif type(v) in [int, long]:
            var = "l%d" % iInt
            iInt += 1
        elif type(v) == float:
            var = "d%d" % iDbl
            iDbl += 1
        else:
            assert(0)
        params.append(p)
        callparams.append("%s%s%s" % (ref, inout, var))

    if len(callparams):
        cmd = "set s0=%s(%s)" % (procedure, ",".join(callparams))
    else:
        cmd = "set s0=%s()" % procedure
    #print "mexec(", cmd, params, ")"
    return mexec(cmd, *params)
    
class Global(object):
    def __init__(self, path):
        self.path = path
    def __getitem__(self, key):
        return Global(self.path + [str(key)])

    def printable(self):
        rv = []
        try:
            rv.append(self.value)  # may not have value
        except:
            pass
        for k, v in self.items():
            rv.append('%s.%s = "%s"' % (".".join(self.path), k, v))
        for k in self.keys_with_decendants():
            children = self[k].printable()
            if children:
                rv = rv + children
        return rv
    
    def __str__(self):
        return '\n'.join([str(s) for s in self.printable()])
    def __repr__(self):
        return "<%s.%s: %s%s=%s >" % (self.__class__.__module__, self.__class__.__name__, 
            self.path[0], self.path[1:], str(self))

    def get_value(self):
        if len(self.path) == 1:
            s0 = self.path[0]
        else:
            s0 = '%s("%s")' % (self.path[0], '","'.join(self.path[1:]))
        s0, = mexec("set s0=@s0", INOUT(s0))
        return s0
    def set_value(self, s1):
        if len(self.path) == 1:
            s0 = self.path[0]
        else:
            s0 = '%s("%s")' % (self.path[0], '","'.join(self.path[1:]))
        mexec("set @s0=s1", s0, s1)
    value = property(get_value, set_value)

    def kill(self):
        if len(self.path) == 1:
            s0 = self.path[0]
        else:
            s0 = '%s("%s")' % (self.path[0], '","'.join(self.path[1:]))
        mexec("kill @s0", s0)

    def keys(self):
        """
            This returns the keys which have a value (not those with decendants but without values).
        """
        if len(self.path) > 1:
            path = '%s("%s",s0)' % (self.path[0], '","'.join(self.path[1:]))
        else:
            path = '%s(s0)' % (self.path[0])
        s0 = ""
        rv = []
        while 1:
            s0, l0 = mexec('set s0=$order(%s),l0=0 if s0\'="" set l0=$data(%s) ' % (path, path), INOUT(s0), INOUT(0))
            if s0:
                if l0 & 1:
                    rv.append(s0)
            else:
                break
        return rv

    def items(self):
        """
            This returns the keys, values which have a value (not those with decendants but without values).
        """
        if len(self.path) > 1:
            path = '%s("%s",s0)' % (self.path[0], '","'.join(self.path[1:]))
        else:
            path = '%s(s0)' % (self.path[0])

        s0 = ""
        rv = []
        while 1:
            s0, l0, s1 = mexec('set s0=$order(%s),l0=0 if s0\'="" set l0=$data(%s),s1=$GET(%s) ' % (path, path, path), INOUT(s0), INOUT(0), INOUT(""))
            if s0:
                if l0 & 1:
                    rv.append((s0, s1))
            else:
                break
        return rv

    def keys_with_decendants(self):
        """
            This returns the keys which have decendants (not those with values but without decendants).
        """
        if len(self.path) > 1:
            path = '%s("%s",s0)' % (self.path[0], '","'.join(self.path[1:]))
        else:
            path = '%s(s0)' % (self.path[0])
        s0 = ""
        rv = []
        while 1:
            s0, l0 = mexec('set s0=$order(%s),l0=0 if s0\'="" set l0=$data(%s) ' % (path, path), INOUT(s0), INOUT(0))
            if s0:
                if l0 & 10:
                    rv.append(s0)
            else:
                break
        return rv
    def exists(self):
        if len(self.path) == 1:
            s0 = self.path[0]
        else:
            s0 = '%s("%s")' % (self.path[0], '","'.join(self.path[1:]))
        l0, = mexec("set l0=$data(@s0)", s0, INOUT(0))
        return l0 > 0
    def has_value(self):
        if len(self.path) == 1:
            s0 = self.path[0]
        else:
            s0 = '%s("%s")' % (self.path[0], '","'.join(self.path[1:]))
        l0, = mexec("set l0=$data(@s0)", s0, INOUT(0))
        return l0 & 1
    def has_decendants(self):
        if len(self.path) == 1:
            s0 = self.path[0]
        else:
            s0 = '%s("%s")' % (self.path[0], '","'.join(self.path[1:]))
        l0, = mexec("set l0=$data(@s0)", s0, INOUT(0))
        if l0 & 10 :
            return 1
        return 0


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


