
import logging

from brokerRPC import VistARPCConnection

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

class _RPCConnection(VistARPCConnection):
    def invoke(self, name, params=[]):
        """
            Try to understand the return values.
        """
        rv = self.invokeRPC(name, params)
        if rv:
            if rv.find('\r\n'):
                rv = rv.split('\r\n')
                if rv and not rv[-1]:
                    rv = rv[:-1]
        return rv

def connect(host, port, access, verify, context, debug=False):
    return _RPCConnection(host, port, access, verify, context, RPCLogger(debug))

