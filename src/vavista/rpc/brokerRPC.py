#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Affero General Public License version 3 (AGPL) 
# as published by the Free Software Foundation.
# (c) 2010-2011 caregraf.org
#

"""
 Broker Connection

 Module to provide access to VistA RPCs through either the new style
 VA Broker or IHS's CIA Broker. It provides thread-safe access through a Pool
 class.

 Base Connection for VistA and CIA Brokers with specializations for the 
 particulars of those brokers.
 
"""

__author__ =  'Caregraf'
__copyright__ = "Copyright 2010-2011, Caregraf"
__credits__ = ["Sam Habiel", "Jon Tai", "Andy Purdue", "Jeff Apple", "Ben Mehling"]
__license__ = "AGPL"
__version__=  '0.9'
__status__ = "Production"

import StringIO
import re
import time
import socket
from random import randint

class RPCConnection(object):

	"""
	Hardcoded in VistA/RPMS access code.
	"""
	CIPHER = [
		"wkEo-ZJt!dG)49K{nX1BS$vH<&:Myf*>Ae0jQW=;|#PsO`'%+rmb[gpqN,l6/hFC@DcUa ]z~R}\"V\\iIxu?872.(TYL5_3",
		"rKv`R;M/9BqAF%&tSs#Vh)dO1DZP> *fX'u[.4lY=-mg_ci802N7LTG<]!CWo:3?{+,5Q}(@jaExn$~p\\IyHwzU\"|k6Jeb",
		"\\pV(ZJk\"WQmCn!Y,y@1d+~8s?[lNMxgHEt=uw|X:qSLjAI*}6zoF{T3#;ca)/h5%`P4$r]G'9e2if_>UDKb7<v0&- RBO.",
		"depjt3g4W)qD0V~NJar\\B \"?OYhcu[<Ms%Z`RIL_6:]AX-zG.#}$@vk7/5x&*m;(yb2Fn+l'PwUof1K{9,|EQi>H=CT8S!",
		"NZW:1}K$byP;jk)7'`x90B|cq@iSsEnu,(l-hf.&Y_?J#R]+voQXU8mrV[!p4tg~OMez CAaGFD6H53%L/dT2<*>\"{\\wI=",
		"vCiJ<oZ9|phXVNn)m K`t/SI%]A5qOWe\\&?;jT~M!fz1l>[D_0xR32c*4.P\"G{r7}E8wUgyudF+6-:B=$(sY,LkbHa#'@Q",
		"hvMX,'4Ty;[a8/{6l~F_V\"}qLI\\!@x(D7bRmUH]W15J%N0BYPkrs&9:$)Zj>u|zwQ=ieC-oGA.#?tfdcO3gp`S+En K2*<",
		"jd!W5[];4'<C$/&x|rZ(k{>?ghBzIFN}fAK\"#`p_TqtD*1E37XGVs@0nmSe+Y6Qyo-aUu%i8c=H2vJ\\) R:MLb.9,wlO~P",
		"2ThtjEM+!=xXb)7,ZV{*ci3\"8@_l-HS69L>]\\AUF/Q%:qD?1~m(yvO0e'<#o$p4dnIzKP|`NrkaGg.ufCRB[; sJYwW}5&",
		"vB\\5/zl-9y:Pj|=(R'7QJI *&CTX\"p0]_3.idcuOefVU#omwNZ`$Fs?L+1Sk<,b)hM4A6[Y%aDrg@~KqEW8t>H};n!2xG{",
		"sFz0Bo@_HfnK>LR}qWXV+D6`Y28=4Cm~G/7-5A\\b9!a#rP.l&M$hc3ijQk;),TvUd<[:I\"u1'NZSOw]*gxtE{eJp|y (?%",
		"M@,D}|LJyGO8`$*ZqH .j>c~h<d=fimszv[#-53F!+a;NC'6T91IV?(0x&/{B)w\"]Q\\YUWprk4:ol%g2nE7teRKbAPuS_X",
		".mjY#_0*H<B=Q+FML6]s;r2:e8R}[ic&KA 1w{)vV5d,$u\"~xD/Pg?IyfthO@CzWp%!`N4Z'3-(o|J9XUE7k\\TlqSb>anG",
		"xVa1']_GU<X`|\\NgM?LS9{\"jT%s$}y[nvtlefB2RKJW~(/cIDCPow4,>#zm+:5b@06O3Ap8=*7ZFY!H-uEQk; .q)i&rhd",
		"I]Jz7AG@QX.\"%3Lq>METUo{Pp_ |a6<0dYVSv8:b)~W9NK`(r'4fs&wim\\kReC2hg=HOj$1B*/nxt,;c#y+![?lFuZ-5D}",
		"Rr(Ge6F Hx>q$m&C%M~Tn,:\"o'tX/*yP.{lZ!YkiVhuw_<KE5a[;}W0gjsz3]@7cI2\\QN?f#4p|vb1OUBD9)=-LJA+d`S8",
		"I~k>y|m};d)-7DZ\"Fe/Y<B:xwojR,Vh]O0Sc[`$sg8GXE!1&Qrzp._W%TNK(=J 3i*2abuHA4C'?Mv\\Pq{n#56LftUl@9+",
		"~A*>9 WidFN,1KsmwQ)GJM{I4:C%}#Ep(?HB/r;t.&U8o|l['Lg\"2hRDyZ5`nbf]qjc0!zS-TkYO<_=76a\\X@$Pe3+xVvu",
		"yYgjf\"5VdHc#uA,W1i+v'6|@pr{n;DJ!8(btPGaQM.LT3oe?NB/&9>Z`-}02*%x<7lsqz4OS ~E$\\R]KI[:UwC_=h)kXmF",
		"5:iar.{YU7mBZR@-K|2 \"+~`M%8sq4JhPo<_X\\Sg3WC;Tuxz,fvEQ1p9=w}FAI&j/keD0c?)LN6OHV]lGy'$*>nd[(tb!#"]

	def __init__(self, host, port, access, verify, context, logger, endMark, poolId):
		"""
		- host/port
		- vista's security (access, verify)
		- a logger that implements logError and logInfo
		- endMark marks end of message
		- poolId is a connection pool's id for a connection. This is used by the logger.
		"""

		self.logger = logger
		
		self.host = host
		self.port = port
		self.access = access
		self.verify = verify
		self.context = context
		self.endMark = endMark

		self.poolId = poolId

		self.sock = None
	
	def invokeRPC(self, name, params):
		"""	
		Invoke an RPC. If the connection is closed, try to reopen it once. If fail
		again then raise an exception. This takes care of connection time outs
		which happens with the CIA Broker. Note CIA Broker does support a Ping but
		this approach avoids hogging connections when traffic is low.
		"""
		if not self.sock:
			self.logger.logInfo("RPCConnection", "Connecting %d as Socket not initialized" % self.poolId)
			self.connect()
		# CIA closes socket in two ways. Elegantly after 2 minutes or so of idleness and abruptly leading to Errno 10053
		e = None
		try:
			# make request AFTER (re)connect. CIA must know UCI.
			request = self.makeRequest(name, params)
			self.sock.send(request)
			msg = self.readToEndMarker()
		except socket.error as e:
			msg = ""
		# remote end closed so reconnect and retry.
		if not len(msg):
			self.logger.logInfo("RPCConnection", "Forced to reconnect connection %d after reply failed (%s))" % (self.poolId, str(e) if e else "empty reply"))
			self.connect()
			request = self.makeRequest(name, params)
			self.sock.send(request)
			msg = self.readToEndMarker()
		return msg

	def connect(self):
		"""
		(Re)connect/activate the connection. Sets up basic pipe. The hand shake
		depends on this class' subclass
		"""
		if self.sock:
			self.sock.close()
		# Setup the connection
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock.connect((self.host, self.port))
		self.logger.logInfo("RPCConnection", "Connecting to %s %d - Step1 for %d ..." % (self.host, self.port, self.poolId))
		
	def encrypt(cls, val):
		ra = randint(0, 18)
		rb = randint(0, 18)
		while ((rb == ra) or (rb == 0)):
			rb = randint(0, 18)
		cra = RPCConnection.CIPHER[ra]
		crb = RPCConnection.CIPHER[rb]
		cval = chr(ra + 32)
		for i in range(len(val)):
			c = val[i]
			index = cra.find(c)
			if index == -1:
				# no str(c)
				cval += str(c)
			else:
				cval += str(crb[index])
		cval += chr(rb + 32)
		return cval.encode("utf-8")
		
	def readToEndMarker(self):
		"""
		Endmarker:
		- VISTA: chr(4)
		- CIA: chr(255)
		"""
		msgChunks = []
		noChunks = 0
		msg = ""
		while 1:
			# TBD: Interplay with FMQL Response size settings. Seems to send just that amount
			# Note: must make big enough so error strings below are fetched.
			# TBD: interplay setting here and in FMQL (Node Size is 201)
			msgChunk = self.sock.recv(256)
			# Connection closed
			# Note: don't differentiate connection closed and no chunks sent from connection just dropped
			if not msgChunk:
				break
			if not len(msgChunks):
				# \x00\x00 in VistA. CIA uses ID\x00
				# but some connect handshake lack this
				if msgChunk[0] == "\x00": # smh fix
					msgChunk = msgChunk[2:]
			noChunks += 1
			if msgChunk[-1] == self.endMark:
				msgChunks.append(msgChunk[:-1])
				break
			msgChunks.append(msgChunk)
		if len(msgChunks):
			msg = "".join(msgChunks)
		self.logger.logInfo("RPCConnection", "Message of length %d received in %d chunks on connection %d" % (len(msg), noChunks, self.poolId))
		return msg

class VistARPCConnection(RPCConnection):

	def __init__(self, host, port, access, verify, context, logger, poolId=-1):
		# End Token for messages is chr(4)
		RPCConnection.__init__(self, host, port, access, verify, context, logger, chr(4), poolId)

	def connect(self):
		"""
		How VistA Broker connects
		"""
	
		# 1. Basic Connect
		RPCConnection.connect(self)

		# 2. TCP 
		tcpConnect = self.makeRequest("TCPConnect", [socket.gethostbyname(socket.gethostname()), "0", "FMQL"], True)
		self.sock.send(tcpConnect)
		connectReply = self.readToEndMarker()
		if not re.match(r'accept', connectReply):
			raise Exception("VistARPCConnection", connectReply)

		# 3. Sign on
		signOn = self.makeRequest("XUS SIGNON SETUP", [])
		self.sock.send(signOn)
		connectReply = self.readToEndMarker() # assume always ok
		accessVerify = self.encrypt(self.access + ";" + self.verify)
		login = self.makeRequest("XUS AV CODE", [accessVerify])
		self.sock.send(login)
		connectReply = self.readToEndMarker()
		if re.search(r'Not a valid ACCESS CODE/VERIFY CODE pair', connectReply):
			raise Exception("VistARPCConnection", connectReply)
			
		# 4. Context (per connection. CIA has it per request)
		eMSGCONTEXT = self.encrypt(self.context)
		ctx = self.makeRequest("XWB CREATE CONTEXT", [eMSGCONTEXT])
		self.sock.send(ctx)
		connectReply = self.readToEndMarker()
		self.logger.logInfo("CONNECT", "context reply is %s" % connectReply)
		if re.search(r'Application context has not been created', connectReply) or re.search(r'does not exist on server', connectReply):
			raise Exception("VistARPCConnection", connectReply)
		self.logger.logInfo("VistARPCConnection", "Handshake complete for connection %d" % self.poolId)

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
		
		namespec = chr(len(name)) + name	# format name S-PACK
		
		paramsspecs = "5" # means that what follows is Params to RPC
		
		if not len(params):  # if no paramters do this and done
			paramsspecs += "4" + "f"
		else: # if there are paramters
			for param in params:
				if type(param) is not dict:
					paramsspecs += "0" # Type of RPC: Literal
					paramsspecs += str(len(param)).zfill(3) + str(param) # L-PACK
					paramsspecs += "f"  # End
				else: # we are in a dictionary
					paramsspecs += "2" # Type of RPC: List
					paramIndex = 1 # keep track of where to put the t's
					for key,val in param.items():
						if paramIndex > 1: paramsspecs += "t" # t is the delimiter b/n each key,val pair
						paramsspecs += str(len(str(key))).zfill(3) + str(key) # L-PACK
						paramsspecs += str(len(str(val))).zfill(3) + str(val) # L-PACK
						paramIndex += 1
					paramsspecs += "f" # close list

		endtoken = chr(4)
		return protocoltoken + commandtoken + namespec + paramsspecs + endtoken	 
		
class CIARPCConnection(RPCConnection):

	def __init__(self, host, port, access, verify, context, logger, poolId=-1):
		"""
		"CG FMQL QP USER" for FMQL, "CIAV VUECENTRIC" for VUECENTRIC is context
		"""
		RPCConnection.__init__(self, host, port, access, verify, context, logger, chr(255), poolId)
		# Sequence number for requests: 
		# - Loops from 1 to 255. Note CIA Broker does allow up to 255 outstanding requests per connection.
		# - I am not using this facility. I treat CIA Broker like VistA new style broker.
		# - Am not doing PINGs either to keep connections alive. If move to support many requests per 
		#   connection ie. that permission to request and not connection itself is queued, then should
		#   not need pings.	
		self.sequence = 0

		# Need for first request sent in connect
		self.uid = "" 

	def connect(self):
		"""
		CIA form of CONNECT				
		"""

		# 1. Basic Connect
		RPCConnection.connect(self)

		# 2. From CIAConnectAction
		uci = ""
		# MSC OVID: set invalid address for callback so that fails right away though manual says won't try this if not in debug mode.
		myAddress = "NOTVALID";
		self.logger.logInfo("CIACONNECT", "Sending CIA Connect")
		ciaConnect = self.__makeCIARequest("C", {"IP": myAddress, "UCI": uci, "DBG": "0", "LP": "0", "VER": "1.6.5.26"})
		self.sock.send(ciaConnect)
		connectReply = self.readToEndMarker()
		# 1^0^1.1^^1
		self.logger.logInfo("CIACONNECT", "STEP 1 SUCCESS: " + connectReply)
			
		# 3. Sign on Request (Access, Verify)
		# Note: CIABroker does in one step; VistABroker takes 3. Also Context
		# is per Request in CIA Broker. It is per connection in VistA Broker.
		accessVerify = self.encrypt(self.access + ";" + self.verify)
		"CIANBRPC AUTH"
		computerName = socket.gethostname()
		self.uid = ""
		ciaConnect = self.makeRequest("CIANBRPC AUTH", ["CIAV VUECENTRIC", computerName, "", accessVerify])
		self.sock.send(ciaConnect)
		connectReply = self.readToEndMarker()
		replyLines = connectReply.split("\r")
		if not (len(replyLines) > 1 and re.match(r'\d+\^', replyLines[1])):
			eMsg = "STEP 2 FAIL"
			self.logger.logError("CIACONNECT", eMsg)
			raise Exception("CIACONNECT", eMsg)
		self.uid = re.match(r'([^\^]+)', replyLines[1]).group(1)		
		self.logger.logInfo("CIACONNECT", "STEP 2 SUCCESS - Connected. UID %s" % self.uid)

	# Note: unlike VistA broker, context is per request, not fixed in connection
	# However the logic here fixes it per connection.
	def makeRequest(self, rpcName, params):
		rpcParams = {"CTX": self.context, "UID": self.uid, "VER": "0", "RPC": rpcName}
		for i in range(len(params)):
			rpcParams[str(i+1)] = params[i]
		return self.__makeCIARequest("R", rpcParams)

	# rtype: C for Connect, R for RPC
	# http://www.mail-archive.com/python-list@python.org/msg229980.html
	# http://docs.python.org/howto/unicode.html
	def __makeCIARequest(self, rtype, params):

		headerToken = "{CIA}"
		
		# 1 byte token
		EODToken = chr(255)

		# 1 byte sequence		
		self.sequence += 1
		if self.sequence == 256:
			self.sequence = 1
		sequence = chr(self.sequence)
		
		if rtype == "R":
			brtype = chr(82)
		else:
			brtype = chr(67)
       
		# 1 byte rtype
		
		# Assemble Parameters (only do string parameters. Add ARRAY in next phase)
		paramsspecs = ""
		for paramId, paramValue in params.iteritems():
			paramsspecs += self.__byteIt(paramId) + chr(0) + self.__byteIt(paramValue)	

		return headerToken + EODToken + sequence + brtype + paramsspecs + EODToken
		
	# Return byte array of length and string val per the CIA Broker encoding scheme
	def __byteIt(self, strVal):
		slen = len(strVal)
		# remainder if /16
		low = slen % 16
		# A right shift by n bits is defined as division by pow(2, n) [ie./ /16]
		slen = slen >> 4
		bytes = bytearray()
		highCount = 0
		while slen != 0:
			bytes.append(slen & 0xFF) 
			slen = slen >> 8
			highCount += 1
		fbytes = bytearray()
		# No bytes after this one in first four bits. Left over in second. If < 16, then only one byte overall.
		fbytes.append((highCount << 4) + low)
		# Reverse from last byte (highest order) to first (lowest)
		# ie. big endian
		for idx in reversed(xrange(0, len(bytes))):
			fbytes.append(bytes[idx])
		fbytes.extend(bytearray(strVal))
		return fbytes
		
# ############################## RPCConnection Pool ###################

"""
A connection pool for accessing VistA RPC's from threaded environments like FMQL's Apache resident FMQL Query Processor. The queue manages a list of connections, only using the number required by an application up to the maximum number.

Future - look at:
- context manager: http://jessenoller.com/2009/02/03/get-with-the-program-as-contextmanager-completely-different/
"""
import Queue
class RPCConnectionPool:

	# - for running in WSGI, set poolSize == number of threads expected in a process. 
	# - brokerType is "VistA" or "CIA"
	def __init__(self, brokerType, poolSize, host, port, access, verify, context, logger):	
		self.logger = logger
		# Queue is LIFO and thread safe. Means threads share a limited set
		# of connections and will only use what their pace requires ie. if
		# pool size is five, that doesn't mean five active connections. May
		# just use and reuse the first one or two over and over.
		# http://docs.python.org/library/queue.html
		self.__connectionQueue = Queue.LifoQueue()
		self.__prebuildConnections(brokerType, poolSize, host, port, access, verify, context)

	# Build but don't apply connections. RPCConnection will 
	# apply itself as needed
	def __prebuildConnections(self, brokerType, poolSize, host, port, access, verify, context):
		for i in range(poolSize, 0, -1): # reverse order so numbers match for LIFO
			if brokerType == "CIA":
				connection = CIARPCConnection(host, port, access, verify, context, self.logger, i)
			else: # default is "VistA"
				connection = VistARPCConnection(host, port, access, verify, context, self.logger, i)
			self.__connectionQueue.put(connection)
		self.logger.logInfo("CONN POOL", "Initialized %d connections" % poolSize)
		self.poolSize = poolSize
		
	def invokeRPC(self, name, params):
	
		# Block until connection is available
		connection = self.__connectionQueue.get()
		  
		try:
			 reply = connection.invokeRPC(name, params)
		except Exception as e:	 
			 # Note: retry (reset connection) happens in RPCConnection. If get here then bigger problem.
			 self.logger.logError("CONN POOL", "Basic connectivity problem. Connection was refused so RPC invocation failed.")
			 raise e

		self.__connectionQueue.put(connection)
		  
		return reply

	# Really for testing. Force preconnection of a certain number
	def preconnect(self, number):
		if number > self.poolSize:
			number = self.poolSize
		connections = []
		for i in range(number):
			connection = self.__connectionQueue.get()
			connection.connect()
			connections.append(connection)
		for i in range(number):
			self.__connectionQueue.put(connections[i])

# ################################ Basic Test ###########################

import threading

# Used to test the connection pool.
class ThreadedRPCInvoker(threading.Thread):

	def __init__(self, pool, requestName, requestParameters):
		threading.Thread.__init__(self)
		self.pool = pool
		self.requestName = requestName
		self.requestParameters = requestParameters
		
	def run(self):
		print "Sending another request ..."
		reply = self.pool.invokeRPC(self.requestName, self.requestParameters)
		print "First part of reply: %s" % reply[0:50]

class RPCLogger:
	def __init__(self):
		pass
	def logInfo(self, tag, msg):
		self.__log(tag, msg)
	def logError(self, tag, msg):
		self.__log(tag, msg)
	def __log(self, tag, msg):
		print "BROKERRPC -- %s %s" % (tag, msg)

import getopt, sys
import json
import time
def main():
	opts, args = getopt.getopt(sys.argv[1:], "")
	if len(args) < 4:
		print "Enter <host> <port> <access> <verify>"
		return
		
	# VERY BASIC:
	connection = VistARPCConnection(args[0], int(args[1]), args[2], args[3], "CG FMQL QP USER", RPCLogger())
	reply = connection.invokeRPC("CG FMQL QP", ["OP:DESCRIBE^TYPE:2^ID:9"])
	json.loads(reply)
	print reply[0:31]

	# 10 and 20 ie. pool size 10, request number 20. Can interplay. Should see some connection come more to the fore.
	# Should see, full size isn't 
	pool = RPCConnectionPool("VistA", 30, args[0], int(args[1]), args[2], args[3], "CG FMQL QP USER", RPCLogger())
	pool.preconnect(5)
	for i in range(20):
		trpcInvoker = ThreadedRPCInvoker(pool, "CG FMQL QP", ["OP:DESCRIBE^TYPE:2^ID:9"])
		trpcInvoker.start()
	
if __name__ == "__main__":
	main()
