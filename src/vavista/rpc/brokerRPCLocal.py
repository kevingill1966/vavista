
# This module contains the customisations of the brokerRPC.py classes
# that I am using to implement my API. The idea is that I can keep 
# brokerRPC.py unchanged.

# The modifications here are:
#     implement a logger using standard python logging
#     implement some classes so that I can pass different parameter 
#         types, specifically reference types.

import logging

from brokerRPC import VistARPCConnection as VistARPCConnectionStd

#------ Parameter Management ----------------------------------------------
class Param:
    end_marker = "f"  # End

class PLiteral(Param):
    type_marker = "0" # Type of RPC: Literal
    def __init__(self, value):
        if value is None:
            self.value = ""
        else:
            self.value = str(value)
    def serialise(self, protocol="vista"):
        rv  = self.type_marker
        rv += str(len(self.value)).zfill(3) + str(self.value) # L-PACK
        rv += self.end_marker
        return rv

class PReference(PLiteral):
    type_marker = "1" # Type of RPC: Reference

class PList(Param):
    type_marker = "2" # Type of RPC: array
    def __init__(self, value):
        """
            Value is either should be iterable or provide an "items"
            method which returns an iterable.
            Expect a list of tuple pairs or a dict.
        """
        self.value = value

    def serialise(self, protocol="vista"):
        if hasattr(self.value, "items"):
            iterator = self.value.items()
        else:
            iterator = iter(self.value)

        rv = self.type_marker
        between_values = False # keep track of where to put the t's
        for key, val in iterator:
            if between_values:
                rv += "t" # t is the delimiter b/n each key,val pair
            else:
                between_values = True
            rv += str(len(str(key))).zfill(3) + str(key) # L-PACK
            rv += str(len(str(val))).zfill(3) + str(val) # L-PACK
        rv += self.end_marker
        return rv

# I cannot see where this is implemented in the client side. Logic is
# in XWBPRS.m
#class PGlobal(PLiteral):
#   type_marker = "3" # Type of RPC: Global

class PEncoded(Param):
    """Value was encoded by the caller"""
    def __init__(self, value):
        self.value = value
    def serialise(self, protocol="vista"):
        return self.value

#------ Map logging to Python logging -------------------------------------
class RPCLogger:
    def __init__(self, debug=False):
        self.logger = logging.getLogger("vavista.rpc")
        if debug:
            handler = logging.StreamHandler()
            handler.setLevel(logging.DEBUG)
            self.logger.setLevel(logging.DEBUG)
            self.logger.addHandler(handler)
    def logInfo(self, tag, msg):
        self.logger.info("%s %s", tag, msg)
    def logError(self, tag, msg):
        self.logger.error("%s %s", tag, msg)

#------ Customised version of FMQL connection -----------------------------
class VistARPCConnection(VistARPCConnectionStd):
    # I sub-classed this to rewrite the makeRequest method
    def __init__(self, host, port, access, verify, context, debug=False):
        return VistARPCConnectionStd.__init__(self, host, port, access, verify, context, RPCLogger(debug))

    def makeRequest(self, name, params, isCommand=False):
        """ 
        Format a the RPC request to send to VISTA:
        name = Name of RPC
        params = comma delimit list of paramters
        isCommand = reserved for internal use. If you really want to know, it's for connecting or disconnecting.
        """

        # Header saying that
        # 1. We are doing NS broker '[XWB]'
        # 2. We are running V 1
        # 3. We are running Type 1
        # 4. Envelope size is 3 (i.e. max message is 999; the longest number we can fit in 3 chars)
        # 5. XWBPRT (whatever that is) is 0
        protocoltoken = "[XWB]1130"
        
        if isCommand:   # Are we executing a command?
            commandtoken = "4"
        else:
            commandtoken = "2" + chr(1) + "1"
        
        namespec = chr(len(name)) + name    # format name S-PACK
        
        paramsspecs = "5" # means that what follows is Params to RPC

        if not len(params):  # if no paramters do this and done
            paramsspecs += "4" + "f"
        else: # if there are paramters
            for param in params:
                if isinstance(param, Param):
                    paramsspecs += param.serialise()
                else:
                    if type(param) == dict:
                        paramsspecs += PList(param).serialise()
                    else:
                        paramsspecs += PLiteral(param).serialise()

        endtoken = chr(4)
        return protocoltoken + commandtoken + namespec + paramsspecs + endtoken     

    def invoke(self, name, *params, **kwargs):
        """
            Invoke a rpc. Same as raw function, except I use python to build argument list.
        """
        return_formatter = kwargs.get('return_formatter', None)
        rv = self.invokeRPC(name, params)
        if return_formatter:
            return return_formatter(rv)
        else:
            return rv


    def l_invoke(self, name, *params):
        """
            Invoke an RPC. Same as invoke above, except it attempts to split the return
            value into a list using DOS line feeds "\r\n" which are used in Vista
        """
        def formatter(rv):
            if rv:
                rv = rv.split('\r\n')
                if rv and not rv[-1]:
                    rv = rv[:-1]
                return rv
            else:
                return []
        return self.invoke(name, *params, return_formatter=formatter)
