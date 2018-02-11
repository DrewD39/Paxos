
from Messenger import MessageType
import Messenger
import socket
import Proposer
import threading
import time
import copy
import select

'''
	Each replica will have an acceptor and a learner service associated with it
	for implementing Paxos
'''

class Replica():

	#idCounter = 0 # Increment id for each new replica

	def __init__ (self, id, ip, port, server_pairs, proposer=False):
		self.chat_log = [] # List of strings for each message
		self.id = id
		self.ip = ip
		self.port = int(port)
		#self.id = Replica.idCounter # id for each replica (from 0-2f)
		#Replica.idCounter += 1
		
		self.other_replicas = [x for x in server_pairs if x != (ip, port)] # List of tuples (ip, port)

		# TODO: self.acceptor = Acceptor()
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
		t1 = threading.Thread(target=self.setup_server, args=(len(self.other_replicas[int(self.id):]),))
		t1.start()
		
		self.connect_to_replicas(self.other_replicas[:int(self.id)])

		t1.join() # Wait for all connections to be established

		t1 = threading.Thread(target=self.wait_for_message)
		t1.start()

		if self.proposer:
			self.proposer.send_leader_message("hello mate")

		t1.join()


	def connect_to_replicas (self, replicas):
		i = 0
		print str(self.id) + " will connect to " + str(replicas)
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

			print str(self.id) + " Successfuly connected on (ip, port) " + str(replica)
			self.connections_list.append(s)

		# socket connections list should now be set up
		if self.proposer:
			self.proposer.set_socket_list(self.connections_list)

		print str(len(replicas)) + " connections setup for " + str(self.id)


	def setup_server (self, numb_replicas):
		i = 0
		while i < numb_replicas: # We know we should accept connections from all other replicas 
			(clientsocket, address) = self.serversocket.accept()
			self.connections_list.append(clientsocket)
			print str(self.id) + " Accepted connection " + str(clientsocket.getsockname()) #print (clientsocket, address)
			i += 1

		print "Server is setup for " + str(self.id)


	def add_msg_to_chat_log (self, msg):
		self.chat_log.append(msg)
		# TODO: just for debugging, later remove this
		print_chat_log()


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


	def recv_message (self, socket):
		msg = Messenger.recv_message(socket)
		
		cmd = msg.split(":")[0]
		if cmd == MessageType.REQUEST.value:
			# Proposer should now propose here
			print "Received request message"
			pass
		elif cmd == MessageType.I_AM_LEADER.value:
			# Proposer should now send message
			print "Received I am Leader message"
			pass
		elif cmd == MessageType.YOU_ARE_LEADER.value:
			# Acceptor should now send message
			print "Received You are Leader message"
			pass
		elif cmd == MessageType.COMMAND.value:
			# proposer should now send message
			print "Received command message"
			pass
		elif cmd == MessageType.ACCEPT.value:
			# Acceptor should now send message
			print "Received accept message"
			pass
		else:
			print "The replica " + str(self.id) + " did not recognize the message " + str(cmd)


	def print_chat_log (self):
		print '\n'.join(self.chat_log)

