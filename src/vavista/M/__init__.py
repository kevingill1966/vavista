
import os

print "importing M"

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
    

