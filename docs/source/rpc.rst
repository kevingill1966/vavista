rpc (Python access to VistA RPCs)
=================================

Credit
------

This code belongs to Caregraph.org's FMQL product. I put it here for ease of
reuse.

Application Context
-------------------

In order to access RPCs in Vista, you need to have a valid Application Context Id. 

The application context provides the security management for RPCs. The context must
exist in the table "OPTION" (19), with the Type = "Broker (client/server)" (B). 

I created an option called "RPC DEMO". I did not assign it any RPCs, so only
public RPCs should be available. ::

    Select OPTION NAME: RPC DEMO
    Not a known package or a local namespace.
      Are you adding 'RPC DEMO' as a new OPTION (the 10852ND)? No// y  (Yes)
         OPTION MENU TEXT: RPC DEMO
    MENU TEXT: RPC DEMO// 
    TYPE: B  Broker (Client/Server)


RPC Port
--------

9210 - where is this configured ?? TODO RPC BROKER SITE PARAMETERS seems do describe
other sites configurations, not this sites.

How RPCs are Configured in Vista
--------------------------------

VistA provides a RPC (remote procedure call) broker. This sits on an agreed port.
Clients connect to the broker and create a session. In the GT.M context, a server
is forked to process the request. The server waits, receives a request, processes
it and returns a response. The mechanism is single-threaded and synchronous.

The available RPCs are listed in Fileman file "REMOTE PROCEDURE" (8994).

TODO: rpc availability what is public??

The RPC may be restricted to one application. TODO: how???

How to set up package: TODO

The Option allows you to assign restricted RPCs to the application context.

All results from RPCs are returned as a single string.
RPCs have the following return types::

       1        SINGLE VALUE (string)
       2        ARRAY (string split by \r\n)
       3        WORD PROCESSING (string split by \r\n)
       4        GLOBAL ARRAY (string split by \r\n)
       5        GLOBAL INSTANCE (string)

Parameters can be::

       1        LITERAL
       2        LIST
       3        WORD PROCESSING
       4        REFERENCE

See: http://apidocs.medsphere.org/openvista-server/latest/

VistARPCConnection
------------------
::
    from vavista import rpc
    c = rpc.connect(hostname, port, access-code, verify-code, context, debug=False)

        - vista's security (access, verify)
        - application context - See note above
        - the debug flag is useful for interactive use.

This function creates a connection to the VISTA RPC server.

Methods::

    c.invokeRPC(rpcid, [parameters])

This method calls the rpc named, and returns a raw version of the response.

    c.invoke(rpcid, [parameters])

This method trys to convert the response to a list where it is an array. 


Example Code
------------

From prompt::

    $ python
    >>> from vavista.rpc import connect
    >>> c = connect('localhost', 9210, "VISTAIS#1", "#1ISVISTA", "RPC DEMO")
    >>> print c.invoke("XWB EGCHO STRING", ["THIS IS A STRING"])
    ['THIS IS A STRING']
    >>> print c.invoke("XWB EGCHO LIST")[:4]
    ['List Item #1', 'List Item #2', 'List Item #3', 'List Item #4']

    # this is not working...
    >>> print c.invoke("XWB EGCHO SORT LIST", ["HI", 'X(12)="",X(23)=""'])


Simple script::
    import getopt, sys

    from vavista.rpc import connect

    context = "RPC DEMO"   # see not above about creating this option.

    opts, args = getopt.getopt(sys.argv[1:], "")
    if len(args) < 4:
        print "Enter <host> <port> <access> <verify>"
        return

    host, port, access, verify = args[0], int(args[1]), args[2], args[3]

    c = connect(host, port, access, verify, context)

    # Prints out "THIS IS A STRING"
    print c.invoke("XWB EGCHO STRING", ["THIS IS A STRING"])

    # This "list" RPC returns a list of items delimited by the DOS line ending
    l = c.invoke("XWB EGCHO LIST")
    print [row for row in l.split("\r\n") if row]

    # This "list" RPC returns a list of items delimited by the DOS line ending
    l = c.invoke("XWB EGCHO BIG LIST")
    print len([row for row in l.split("\r\n") if row])

    # I don't know the parameter passing conventions - how standard are they
    # this one expects an array
    #l = c.invoke("HO SORT LIST", ["LO", [])
