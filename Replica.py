
from Messenger import MessageType
import Messenger
import socket
import Acceptor
import Proposer
import threading
import time
import copy
import select
import sys
import Learner
from Util import printd
import signal, os


'''
	Each replica will have an acceptor and a learner service associated with it
	for implementing Paxos
'''

class Replica():

	timeout = 10 # value in seconds
	active = True # Replica is up and running

	def __init__ (self, idnum, ip, port, server_pairs, semaphore, proposer=False):

		self.idnum = idnum
		self.ip = ip
		self.port = int(port)
		self.semaphore = semaphore # This semaphore is shared with all processes to wait till all connections are made

		self.majority = (len(server_pairs) // 2) + 1 # interger division rounds down, so add one

		self.other_replicas = [x for x in server_pairs if x != (ip, port)] # List of tuples (ip, port)

		self.learner = Learner.Learner(self.majority, self.idnum)
		self.acceptor = Acceptor.Acceptor(self.idnum, self.learner)

		self.proposer = None


	def start_replica (self):

		self.serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.serversocket.bind(('', self.port)) # Set up server socket to receive and send messages
		self.serversocket.listen(5)

		self.connections_list = [] # List of sockets to other replicas
		t1 = threading.Thread(target=self.setup_server, args=(len(self.other_replicas[int(self.idnum):]),))
		t1.start()

		self.connect_to_replicas(self.other_replicas[:int(self.idnum)])

		t1.join() # Wait for all connections to be established

		if self.idnum == 0: # Only one proposer at a time
			self.initialize_proposer()
			self.should_kill = True # Also we'll try killing the first leader
		elif self.idnum == 1:
			self.should_kill = True
		else:
			self.should_kill = False

		self.acceptor.set_socket_list(self.connections_list)

		self.semaphore.release() # Tell the main process we're done setting up our connections

		# Here we will start two threads, one to wait for messages from replicas and one to wait
		# for connections from clients
		t2 = threading.Thread(target=self.wait_for_message)
		t3 = threading.Thread(target=self.wait_for_client_connections)

		t2.start()
		t3.start()

		t2.join()
		t3.join()


	def initialize_proposer (self):
		self.proposer = Proposer.Proposer(self.idnum, self.majority, self.acceptor)
		# socket connections list should now be set up
		self.proposer.set_socket_list(self.connections_list)
		self.proposer.send_iamleader_message()


	def wait_for_client_connections (self):
		while 1:
			(clientsock, address) = self.serversocket.accept()
			printd("Accept new client on socket " + str(clientsock.getsockname()))
			ct = threading.Thread(target=self.client_thread, args=(clientsock,))
			ct.start()


	def wait_for_message (self):
		while 1 and self.active: # Should just continue to wait for messages
			rd, wd, ed = select.select(self.connections_list, [], [], self.timeout)

			if len(rd) == 0: # We haven't received a message in a while...
				if int(self.acceptor.selected_leaderNum) + 1 == int(self.idnum): # If we're the next in line
					printd("Creating a new proposer on replica " + str(self.idnum))
					self.initialize_proposer()

			# Handle received messages
			for s in rd:
				self.recv_message(s)
				#printd(s)


	def client_thread (self, clientsocket):
		while 1 and self.active: # Should just continue to wait for messages
			rd, wd, ed = select.select([clientsocket], [], [])

			# Handle received messages
			for s in rd:
				self.recv_message(s)

	def connect_to_replicas (self, replicas):
		i = 0
		printd(str(self.idnum) + " will connect to " + str(replicas))
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

			printd(str(self.idnum) + " successfuly connected on (ip, port) " + str(replica))
			self.connections_list.append(s)

		printd(str(len(replicas)) + " connections setup for " + str(self.idnum))


	def setup_server (self, numb_replicas):
		i = 0
		while i < numb_replicas: # We know we should accept connections from all other replicas
			(clientsocket, address) = self.serversocket.accept()
			self.connections_list.append(clientsocket)
			printd(str(self.idnum) + " Accepted connection " + str(clientsocket.getsockname())) #print (clientsocket, address)
			i += 1

		printd("Server is setup for " + str(self.idnum))

	def recv_message (self, origin_socket):
		if self.active:
			msg = Messenger.recv_message(origin_socket)
			#printd("Message is " + str(msg))
			cmd, info = msg.split(":",1)
			args = info.split(",")

			printd("Replica: {} received cmd = {}, info={}".format(self.idnum, MessageType(cmd).name, info))

			if cmd == MessageType.REQUEST.value:
				# from: Client
				# to:   Proposer
				# args: client_name, client_seqnum, value
				# TODO: definite bug when there's multiple clients but for now we'll leave it
				if self.proposer:
					self.proposer.acceptRequest(origin_socket, args[0], args[1], args[2])

				# printd("Received request message")

			elif cmd == MessageType.I_AM_LEADER.value:
				# from: Proposer
				# to:   Acceptor
				# args: leaderNum
				self.acceptor.acceptLeader(args[0], origin_socket)

			elif cmd == MessageType.YOU_ARE_LEADER.value:
				# from: Acceptor
				# to:   Proposer
				# args: prev_leaderNum, req_id, seq_number, current value
				#printd("Received You are Leader message")
				if not self.proposer:
					raise RuntimeError("Non-proposer recieved YOU_ARE_LEADER msg")
				if self.proposer:
					self.proposer.newFollower(args[0], args[1], args[2])

			elif cmd == MessageType.COMMAND.value:
				# acceptor should decide to accept leader command or not, then broadcast accept message to all learners
				# from: Proposer
				# to:   Acceptor
				# args: leaderNum, req_id, seqNum, value
				self.acceptor.accept_value(args[0], args[1], args[2], args[3])
				printd("Received command message to replica id " + str(self.idnum) + " has leader id " + str(int(self.acceptor.selected_leaderNum)))

			elif cmd == MessageType.ACCEPT.value:
				# Acceptor should now send message
				# from: Acceptor
				# to: Learner
				# info: leaderNum, req_id, sequence number, value
				#printd(str(self.idnum) + " sending accept message to learner with args " + str(args[0]) + " : " + str(args[1]))
				accepted = self.learner.acceptValue(args[0], args[1], args[2], args[3])
				if accepted == True: # True == majority achieved; False == no majority
					if self.proposer:
						self.proposer.reply_to_client(args[1], args[3])
						# This is just a test to try killing the primary again and again
						if self.should_kill == True:
							printd("MANUALLY SHUTTING DOWN REPLICA " + str(self.idnum))
							self.active = False

			elif cmd == MessageType.NACK.value:
				# from: Acceptor
				# to: Proposer
				# info: highest_leader_num
				self.proposer.set_leader_num(args[0])
			else:
				printd("The replica " + str(self.idnum) + " did not recognize the message " + str(cmd))
