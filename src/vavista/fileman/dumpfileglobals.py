#!/usr/bin/env python
# TODO: Copy this out to a installable script
# TODO: Data Only / Definition Only options

# This script generates a python script which can be used to 
# insert the file.

import sys

from vavista.M import Globals

def dump(name, fhOut):
    g = Globals["^DIC"]["B"][filename]
    if len(g.keys()) == 0:
        sys.stderr.write("File not found: %s\n" % filename)
        return

    DIC = g.serialise()
    fileid = g.keys()[0]
    g = Globals["^DIC"][fileid].serialise()
    DIC = g + DIC

    DIZ = Globals["^DIZ"][fileid].serialise()
    DD = Globals["^DD"][fileid].serialise()
    g = Globals["^DD"]["IX"]["B"][fileid]
    IX = g.serialise()
    IX = IX + Globals["^DD"]["IX"]["BB"][fileid].serialise()
    IX = IX + Globals["^DD"]["IX"]["AC"][fileid].serialise()
    IX = IX + Globals["^DD"]["IX"]["F"][fileid].serialise()

    indexes = g.keys()
    for indexid in indexes:
        IX = IX + Globals["^DD"]["IX"][indexid].serialise()
        index_names = Globals["^DD"]["IX"]["BB"][fileid].keys()
        for name in index_names:
            IX = IX + Globals["^DD"]["IX"]["IX"][name][indexid].serialise()

    fhOut.write("#!/bin/env python\n\n")
    fhOut.write("import sys\nfrom vavista.M import Globals\n\nfilename = '%s'\n\n" % filename)

    for name, value in [("DIC", DIC), ("DIZ", DIZ), ("DD", DD), ("IX", IX)]:
        fhOut.write("%s = [\n" % name)
        for f in value:
            fhOut.write("\t(%s, %s),\n" % (repr(f[0]), repr(f[1])))
        fhOut.write("]\n\n")

    fhOut.write("""
def createFile():
\t# This creates a file
\tg = Globals["^DIC"]["B"][filename]
\tif len(g.keys()) != 0:
\t\tsys.stderr.write("File already exists: %s\\n" % filename)
\t\tsys.exit(1)
\tGlobals.deserialise(DIC)
\tGlobals.deserialise(DD)
\tGlobals.deserialise(DIZ)
\tGlobals.deserialise(IX)
""")

    fhOut.write("""
def deleteFile():
\t# This deletes a file
""")
    fhOut.write("""\tGlobals["^DIC"]["%s"].kill()\n""" % fileid)
    fhOut.write("""\tGlobals["^DIC"]["B"]["%s"].kill()\n""" % filename)
    fhOut.write("""\tGlobals["^DD"]["%s"].kill()\n""" % fileid)
    fhOut.write("""\tGlobals["^DIZ"]["%s"].kill()\n""" % fileid)
    for indexid in indexes:
        fhOut.write("""\tGlobals["^DD"]["IX"]["%s"].kill()\n""" % indexid)
        index_names = Globals["^DD"]["IX"]["BB"][fileid].keys()
        for name in index_names:
            fhOut.write("""\tGlobals["^DD"]["IX"]["IX"]["%s"]["%s"].kill()\n""" % (name, indexid))
    if indexes:
        fhOut.write("""\tGlobals["^DD"]["IX"]["B"]["%s"].kill()\n""" % fileid)
        fhOut.write("""\tGlobals["^DD"]["IX"]["BB"]["%s"].kill()\n""" % fileid)
        fhOut.write("""\tGlobals["^DD"]["IX"]["AC"]["%s"].kill()\n""" % fileid)
        fhOut.write("""\tGlobals["^DD"]["IX"]["F"]["%s"].kill()\n""" % fileid)
    fhOut.write('\n\nif __name__ == "__main__":\n')
    fhOut.write("\tif len(sys.argv) == 2 and sys.argv[1] == '-k': deleteFile()\n")
    fhOut.write("\tcreateFile()\n")

if __name__ == "__main__":
    filename = sys.argv[1]
    dump(filename, sys.stdout)
