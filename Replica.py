
from Messenger import MessageType
import Messenger
import socket
import Acceptor
import Proposer
import threading
import time
import copy
import select
from Util import printd

'''
	Each replica will have an acceptor and a learner service associated with it
	for implementing Paxos
'''

class Replica():

	#idCounter = 0 # Increment id for each new replica

	def __init__ (self, idnum, ip, port, server_pairs, proposer=False):
		self.chat_log = [] # List of strings for each message
		self.idnum = idnum
		self.ip = ip
		self.port = int(port)
		#self.idnum = Replica.idCounter # id for each replica (from 0-2f)
		#Replica.idCounter += 1

		self.other_replicas = [x for x in server_pairs if x != (ip, port)] # List of tuples (ip, port)

		self.acceptor = Acceptor.Acceptor()
		# TODO: self.learner = Learner()
		if proposer:
			self.proposer = Proposer.Proposer()
		else:
			self.proposer = None # Only one proposer at a time


	def start_replica (self):

		self.serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.serversocket.bind(('', self.port)) # Set up server socket to receive and send messages
		self.serversocket.listen(5)

		self.connections_list = [] # List of sockets to clients
		t1 = threading.Thread(target=self.setup_server, args=(len(self.other_replicas[int(self.idnum):]),))
		t1.start()

		self.connect_to_replicas(self.other_replicas[:int(self.idnum)])

		t1.join() # Wait for all connections to be established

		t1 = threading.Thread(target=self.wait_for_message)
		t1.start()

		if self.proposer:
			self.proposer.send_iamleader_message(str(self.idnum))

		t1.join()


	def connect_to_replicas (self, replicas):
		i = 0
		print str(self.idnum) + " will connect to " + str(replicas)
		for replica in replicas:
			connected = False
			while not connected:
			    try:
			    	# RIP an hour...
			    	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # This is a bug if we move this out... see man connect for python
			    	s.connect((replica[0], int(replica[1]))) # Connect to (ip, port)
			    	connected = True
			    except Exception as e:
			    	time.sleep(0) # yield thread

			print str(self.idnum) + " Successfuly connected on (ip, port) " + str(replica)
			self.connections_list.append(s)

		# socket connections list should now be set up
		if self.proposer:
			self.proposer.set_socket_list(self.connections_list)
		self.acceptor.set_socket_list(self.connections_list)

		print str(len(replicas)) + " connections setup for " + str(self.idnum)


	def setup_server (self, numb_replicas):
		i = 0
		while i < numb_replicas: # We know we should accept connections from all other replicas
			(clientsocket, address) = self.serversocket.accept()
			self.connections_list.append(clientsocket)
			print str(self.idnum) + " Accepted connection " + str(clientsocket.getsockname()) #print (clientsocket, address)
			i += 1

		print "Server is setup for " + str(self.idnum)


	def add_msg_to_chat_log (self, msg):
		self.chat_log.append(msg)
		# TODO: just for debugging, later remove this
		printd_chat_log()


	def add_proposer (self, proposer):
		self.proposer = proposer


	def send_message (self, msg):
		print "Len of socket list is " +str(len(sconnections_list))
		Messenger.broadcast_message(self.connections_list, msg)


	def wait_for_message (self):
		while 1: # Should just continue to wait for messages
			rd, wd, ed = select.select(self.connections_list, [], [])

			# Handle received messages
			for s in rd:
				#Messenger.recv_header(s)
				self.recv_message(s)
				#printd(s)


	def recv_message (self, socket):
		msg = Messenger.recv_message(socket)
		cmd, info = msg.split(":",1)
		args = info.split(",")
		printd("cmd = {}".format(cmd))
		printd("info = {}".format(info))
		if cmd == MessageType.REQUEST.value:
			# from: Client
			# to:   Proposer
			# args: value
			self.proposer.acceptRequest(args[0], socket)
			printd("Received request message")
			pass
		elif cmd == MessageType.I_AM_LEADER.value:
			# from: Proposer
			# to:   Acceptor
			# args: leader idnum
			self.acceptor.acceptLeader(args[0], socket)
			printd("Received I am Leader message")
			pass
		elif cmd == MessageType.YOU_ARE_LEADER.value:
			# from: Acceptor
			# to:   Proposer
			# args: current value, previous leader
			printd("Received You are Leader message")
			pass
		elif cmd == MessageType.COMMAND.value:
			# acceptor should decide to accept leader command or not, then broadcast accept message to all learners
			# from: Proposer
			# to:   Acceptor
			# args: leaderNum, seqNum, value
			self.acceptor.acceptValue(args[0], args[1], args[2], socket)
			printd("Received command message")
			pass
		elif cmd == MessageType.ACCEPT.value:
			#
			# from: Acceptor
			# to:   Learner
			printd("Received accept message")
			pass
		else:
			printd("The replica " + str(self.idnum) + " did not recognize the message " + str(cmd))


	def print_chat_log (self):
		print('\n'.join(self.chat_log))
