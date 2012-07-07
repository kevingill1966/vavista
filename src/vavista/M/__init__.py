
import os
import logging

logger = logging.getLogger("vavista.M")

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
        if type(v) in [str, unicode]:
            var = "s%d" % iStr
            iStr += 1
            if type(p) == unicode:
                p = p.encode('utf-8')
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

    # If _on_before_change is not None, it will be called when an attempt is made to
    # set the value. This is primarily intended for use with fileman.
    # _on_before_change raises an exception if validation fails.
    # _on_before_change must return the data to be inserted
    _on_before_change = None
    _children = None

    def __init__(self, path):
        self.path = path
        self._children = {}

    def __getitem__(self, key):
        if key not in self._children:
            self._children[key] = Global(self.path + [str(key)])
        return self._children[key]
    
    def printable(self):
        rv = []
        for k, v in self.serialise(trim_path=0):
            rv.append('%s = "%s"' % (".".join(self.path), v))
        return rv
    
    def __str__(self):
        return '\n'.join([unicode(s) for s in self.printable()])

    def __repr__(self):
        # I am encoding this in UTF-8 because pdb uses this to display variables
        # which causes unicode errors with non-ASCII values.
        return "<%s.%s: %s%s=%s >" % (self.__class__.__module__, self.__class__.__name__, 
            self.path[0], self.path[1:], unicode(self).encode('utf-8'))

    def get_value(self):
        if len(self.path) == 1:
            s0 = self.path[0]
        else:
            s0 = '%s("%s")' % (self.path[0], '","'.join(self.path[1:]))
        s1, = mexec("set s1=@s0", s0, INOUT(""))
        if s1:
            try:
                s1 = s1.decode('utf-8')
            except:
                logger.exception("Global.get_value (%s) Unicode decode error on [%s]", s0, s1)
                raise

        return s1

    def set_value(self, s1):
        if self._on_before_change:
            s1 = self._on_before_change(self, s1)

        if len(self.path) == 1:
            s0 = self.path[0]
        else:
            s0 = '%s("%s")' % (self.path[0], '","'.join(self.path[1:]))

        if type(s1) == unicode:
            try:
                s1 = s1.encode('utf-8')
            except:
                logger.exception("Global.set_value (%s) Unicode decode error on [%s]", s0, s1)
                raise

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

    def values(self):
        """
            This returns the values which have a value (not those with decendants but without values).
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
                    rv.append(s1)
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

    def deserialise(self, serialised_form):
        """
            Create a global from a serialised format
        """

    def serialise(self, trim_path=None):
        """
            Convert a global to a serialised format.
            returns a list of (key, value) pairs

            By default leading path parts are trimmed to simplify
            copying part of the global structure to another part.

            Converts to (path, value) pairs. You can convert it to
            a wire format, e.g. json

            g = Globals()
            source = g["^DIC"]["999900"]
            ser = source.serialise(0)
            json.dumps(ser)
            for k, v in ser: print k, " = ", v
        """
        rv = []

        if trim_path == None:
            trim_path = len(self.path)
        path = self.path[trim_path:]

        try:
            rv.append((path, self.value))
        except:
            pass

        decendants = self.keys_with_decendants()
        for k, v in self.items():
            if k not in decendants:
                rv.append((path + [k], v))
        for k in self.keys_with_decendants():
            children = self[k].serialise(trim_path=trim_path)
            if children:
                rv = rv + children
        return rv
    
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


