# Simplistic API for calling RPCs with a Vista Server

from brokerRPCLocal import VistARPCConnection, PLiteral, PList, PReference, PEncoded

def connect(host, port, access, verify, context, debug=False):
    return VistARPCConnection(host, port, access, verify, context)

