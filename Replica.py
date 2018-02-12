
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

'''
	Each replica will have an acceptor and a learner service associated with it
	for implementing Paxos
'''

# With synchrony for now...
lock = threading.Lock()

class Replica():

	#idCounter = 0 # Increment id for each new replica

	def __init__ (self, idnum, ip, port, server_pairs, proposer=False):
		self.chat_log = [] # List of strings for each message
		self.idnum = idnum
		self.ip = ip
		self.port = int(port)
		#self.idnum = Replica.idCounter # id for each replica (from 0-2f)
		#Replica.idCounter += 1

		majority = (len(server_pairs) // 2) + 1

		self.other_replicas = [x for x in server_pairs if x != (ip, port)] # List of tuples (ip, port)

		self.acceptor = Acceptor.Acceptor()
		self.learner = Learner.Learner(majority)

		if proposer:
			self.proposer = Proposer.Proposer(majority) # interger division rounds down, so add one
		else:
			self.proposer = None # Only one proposer at a time


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

		# socket connections list should now be set up
		if self.proposer:
			self.proposer.set_socket_list(self.connections_list)

		self.acceptor.set_socket_list(self.connections_list)

		# Here we will start two threads, one to wait for messages from replicas and one to wait
		# for connections from clients
		t2 = threading.Thread(target=self.wait_for_message)
		t3 = threading.Thread(target=self.wait_for_client_connections)

		t2.start()
		t3.start()

		if self.proposer:
			#lock.acquire()	# With synchrony for now... unlock is after learner accepts values
			self.proposer.send_iamleader_message(str(self.idnum))
			#lock.acquire()
			self.proposer.acceptRequest(self.idnum, str(self.idnum), "default_2") 
			#lock.acquire()
			self.proposer.acceptRequest(self.idnum, str(self.idnum), "default_3")
			#lock.acquire()
			self.proposer.acceptRequest(self.idnum, str(self.idnum), "default_4")

		t2.join()


	def wait_for_client_connections (self):
		while 1:
			(clientsock, address) = self.serversocket.accept()
			printd("Accept new client on socket " + str(clientsock.getsockname()))
			self.clientsocket = clientsock # There should only be one client socket
			self.connections_list.append(clientsock)


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


	def add_msg_to_chat_log (self, msg):
		self.chat_log.append(msg)
		# TODO: just for debugging, later remove this
		print self.get_chat_log()

		# I want to open a file a single time but I'm not sure how to ensure we close it at the end
		self.file_log = open("replica_" + str(self.idnum) + ".log", "a")
		self.file_log.write(self.get_chat_log() + "\n")
		self.file_log.close()


	def add_proposer (self, proposer):
		self.proposer = proposer


	def wait_for_message (self):
		while 1: # Should just continue to wait for messages
			rd, wd, ed = select.select(self.connections_list, [], [])

			# Handle received messages
			for s in rd:
				self.recv_message(s)
				#printd(s)


	def recv_message (self, socket):
		msg = Messenger.recv_message(socket)
		#printd("Message is " + str(msg))
		cmd, info = msg.split(":",1)
		args = info.split(",")
		printd("Replica: {} received cmd = {}, info={}".format(self.idnum, MessageType(cmd).name, info))
		if cmd == MessageType.REQUEST.value:
			# from: Client
			# to:   Proposer
			# args: idnum, value
			# TODO: THIS HAS NOT BEEN TESTED
			self.proposer.acceptRequest(self.idnum, args[0])
			# printd("Received request message")
		elif cmd == MessageType.I_AM_LEADER.value:
			# from: Proposer
			# to:   Acceptor
			# args: leader idnum
			self.acceptor.acceptLeader(args[0], socket)
			#printd("Received I am Leader message")
		elif cmd == MessageType.YOU_ARE_LEADER.value:
			# from: Acceptor
			# to:   Proposer
			# args: seq_number, current value
			#printd("Received You are Leader message")
			if self.proposer: # should only get a message if you have a proposer but just in case
				if args[1] != 'None':
					printd("Setting proposer value to prev value " + str(args[1]))
					self.proposer.value = args[1]

				self.proposer.numb_followers += 1 # We have another follower who's joined us
				seq_number = 0 # TODO: actually get sequence number
				self.proposer.send_value(self.idnum, seq_number)
		elif cmd == MessageType.COMMAND.value:
			# acceptor should decide to accept leader command or not, then broadcast accept message to all learners
			# from: Proposer
			# to:   Acceptor
			# args: leaderNum, seqNum, value
			self.acceptor.acceptValue(args[0], args[1], args[2])
			#printd("Received command message")
		elif cmd == MessageType.ACCEPT.value:
			# Acceptor should now send message
			# from: Acceptor
			# to: Learner
			# info: replica_id, sequence number, value
			#printd(str(self.idnum) + " sending accept message to learner with args " + str(args[0]) + " : " + str(args[1]))
			accepted = self.learner.acceptValue(args[0], args[1], args[2])
			if accepted == True:
				self.add_msg_to_chat_log(args[2])
				#if self.proposer:
				#	lock.release()
			else:
				pass
		else:
			printd("The replica " + str(self.idnum) + " did not recognize the message " + str(cmd))


	def get_chat_log (self):
		return ("Chat log for " + str(self.idnum) + ":\n\t" + '\n\t'.join(self.chat_log))
