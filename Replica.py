
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

	timeout = 1 # value in seconds
	active = True # Replica is up and running
	def __init__ (self, idnum, ip, port, server_pairs, semaphore, test_cases, proposer=False):

		self.idnum = idnum
		self.ip = ip
		self.port = int(port)
		self.semaphore = semaphore # This semaphore is shared with all processes to wait till all connections are made

		self.num_replicas = len(server_pairs)
		self.majority = (len(server_pairs) // 2) + 1 # interger division rounds down, so add one

		self.other_replicas = [x for x in server_pairs if x != (ip, str(port))] # List of tuples (ip, port)

		printd("\nREPLICA {}\nServer Pairs: {}\nOther Replicas: {}".format(self.idnum,server_pairs,self.other_replicas))


		self.learner = Learner.Learner(self.num_replicas, self.majority, self.idnum)
		self.acceptor = Acceptor.Acceptor(self.idnum, self.learner)
		self.learner.set_acceptor(self.acceptor) # This is nauseating...

		self.proposer = None
		self.should_kill = False

		self.skips = []
		self.kills = []

		for case in test_cases:
			if case[0] == "kill":	# if kill case active, put the seq_num into kills list
				self.kills.append(case[1])
			if case[0] == "skip":	# if skip test case active, put the seq_num into skips list
				self.skips.append(case[1])

		self.kill_next_round = False


	def start_replica (self):

		self.serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.serversocket.bind(('', self.port)) # Set up server socket to receive and send messages
		self.serversocket.listen(5)

		self.connections_list = [] # List of sockets to other replicas
		# print("\n\nrep {}, other_replics: {} \n(len = {})\n".format(self.idnum,self.other_replicas[int(self.idnum):],len(self.other_replicas[int(self.idnum):])))
		t1 = threading.Thread(target=self.setup_server, args=(len(self.other_replicas[int(self.idnum):]),))
		t1.start()

		self.connect_to_replicas(self.other_replicas[:int(self.idnum)])

		t1.join() # Wait for all connections to be established

		if self.idnum == 0: # Only one proposer at a time
			self.initialize_proposer()

		self.learner.set_socket_list(self.connections_list)
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


	def send_heartbeat (self):
		msg = "{}:{}".format(MessageType.HEARTBEAT.value, None)
		while 1:
			Messenger.broadcast_message(self.connections_list, msg)
			time.sleep(self.timeout / 2)


	def initialize_proposer (self):
		self.proposer = Proposer.Proposer(self.idnum, self.majority, self.acceptor, self.kills, self.skips)
		# socket connections list should now be set up
		self.proposer.set_socket_list(self.connections_list)
		self.learner.set_proposer(self.proposer) # Throwing up again...
		self.proposer.send_iamleader_message()


	def wait_for_client_connections (self):
		while 1:
			(clientsock, address) = self.serversocket.accept()
			printd("Accept new client on socket " + str(clientsock.getsockname()))
			# Receive clients name and add it to learners name->socket mapping
			clientname = Messenger.recv_message(clientsock)
			printd("Replica {} received connection to client {}.".format(self.idnum, clientname))
			self.learner.add_client(clientname, clientsock)
			ct = threading.Thread(target=self.client_thread, args=(clientsock,))
			ct.start()


	def wait_for_message (self):
		while self.active: # Should just continue to wait for messages
			rd, wd, ed = select.select(self.connections_list, [], [], (self.timeout * int(self.idnum)) + self.timeout)

			if len(rd) == 0: # We haven't received a message in a while...
				printd("Creating a new proposer on replica " + str(self.idnum))
				self.initialize_proposer()

			# Handle received messages
			for s in rd:
				self.recv_message(s)


	def client_thread (self, clientsocket):
		while self.active: # Should just continue to wait for messages
			rd, wd, ed = select.select([clientsocket], [], [])

			# Handle received messages
			for s in rd:
				self.recv_message(s)

	def connect_to_replicas (self, replicas):
		i = 0
		printd(str(self.idnum) + " will connect to replicas: " + str(replicas) + "- len = " + str(len(replicas)))
		for replica in replicas:
			connected = False
			while not connected:
			    try:
			    	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			    	s.connect((replica[0], int(replica[1]))) # Connect to (ip, port)
			    	connected = True
			    except Exception as e:
			    	time.sleep(0.1) # yield thread

			printd(str(self.idnum) + " successfuly connected on (ip, port) " + str(replica))
			self.connections_list.append(s)

		printd(str(len(replicas)) + " connections set up for " + str(self.idnum))


	def setup_server (self, numb_replicas):
		i = 0
		while i < numb_replicas: # We know we should accept connections from all other replicas
			(clientsocket, address) = self.serversocket.accept()
			self.connections_list.append(clientsocket)
			printd(str(self.idnum) + " Accepted connection " + str(clientsocket.getsockname()))
			i += 1

		printd("Server is set up for " + str(self.idnum))

	def recv_message (self, origin_socket):
		if self.active:
			msg = Messenger.recv_message(origin_socket)
			printd("Message is " + str(msg))
			cmd, info = msg.split(":",1)
			args = info.split(",")

			if cmd != MessageType.HEARTBEAT.value:
				printd("Replica: {} received cmd = {}, info={}".format(self.idnum, MessageType(cmd).name, info))
				if MessageType(cmd).name == "REQUEST" and self.proposer and self.proposer.am_leader:
					printd("...and {} thinks its leader".format(self.idnum))

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
				# args: prev_leaderNum, idnum, req_id, seq_number, current value
				#printd("Received You are Leader message")
				if not self.proposer:
					raise RuntimeError("Non-proposer recieved YOU_ARE_LEADER msg")
				if self.proposer:
					self.proposer.newFollower(args[0], args[1], args[2], args[3], args[4])

			elif cmd == MessageType.COMMAND.value:
				# acceptor should decide to accept leader command or not, then broadcast accept message to all learners
				# from: Proposer
				# to:   Acceptor
				# args: leaderNum, req_id, seqNum, value
				self.acceptor.accept_value(args[0], args[1], args[2], args[3])
				printd("Received command message to replica id " + str(self.idnum) + " has leader num " + str(int(self.acceptor.selected_leaderNum)))

			elif cmd == MessageType.ACCEPT.value:
				# Acceptor should now send message
				# from: Acceptor
				# to: Learner
				# info: leaderNum, idnum, req_id, sequence number, value
				#printd(str(self.idnum) + " sending accept message to learner with args " + str(args[0]) + " : " + str(args[1]))
				accepted = self.learner.acceptValue(args[0], args[1], args[2], args[3], args[4]) # True == majority achieved; False == no majority
				if self.proposer and self.proposer.am_leader and self.proposer.seq_number == int(args[3]) and int(args[3])+1 in self.kills:
					self.kill_next_round = True
				this_is_a_kill = (int(args[3]) in self.kills) and self.kill_next_round
				if ( accepted == True and self.proposer and self.proposer.am_leader and ( int(args[3]) in self.kills or (int(args[3])-1 in self.skips)) ):#accepted == True and self.proposer and ( this_is_a_kill or (int(args[3])-1 in self.skips)) ):
					# This is just a test of killing the primary again and again
					if int(args[3]) in self.kills:
						self.kills.remove(int(args[3]))
						self.kill_next_round = False
					if int(args[3])-1 in self.skips:
						self.skips.remove( int(args[3])-1 )
					print("\nMANUALLY KILLING REPLICA " + str(self.idnum)+' at sequence number {}\n'.format(args[3]))
					self.active = False

			elif cmd == MessageType.NACK.value:
				# from: Acceptor
				# to: Proposer
				# info: highest_leader_num, idnum
				self.proposer.set_leader_num(args[0], args[1])
			elif cmd == MessageType.CATCHUP.value:
				# from: Learner
				# to: Other learners
				# info: missing_seq_number
				self.acceptor.send_value_at_seq_number(origin_socket, args[0])
				#self.learner.send_value_at_seq_number(origin_socket, args[0])
			elif cmd == MessageType.MISSING_VALUE.value:
				# from: Other learners
				# to : Learner
				# info: seq_number_found, self.idnum, missing_req_id, missing_seq_number, missing_value
				self.learner.fill_missing_value(args[0], args[1], args[2], args[3], args[4])
				if self.proposer:
					self.proposer.note_missing_value(args[0], args[1], args[3])

			elif cmd == MessageType.HEARTBEAT.value: # Just a HEARTBEAT
				pass
			else:
				printd("The replica " + str(self.idnum) + " did not recognize the message " + str(cmd))
