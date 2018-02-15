from enum import Enum
import Messenger
from Messenger import MessageType
from Util import printd
import Queue
'''
	Proposer class of Paxos
	A proposer will broadcast messages to all acceptors (either IAmLeader or Command)
'''
class Proposer:

	def __init__ (self, idnum, majority_numb):
		#self.value = "default_value"
		self.values_list = []
		self.am_leader = False
		self.majority_numb = majority_numb
		# I think this can be equal to one since we can count ourselves
		self.numb_followers = 1
		self.seq_number = 0
		#self.broadcasted_for_seq_number = self.seq_list_size * [False]
		self.requests_before_leadership = Queue.Queue()
		self.idnum = idnum


	def set_socket_list (self, socket_connections_list):
		self.socket_connections_list = socket_connections_list


	def acceptRequest(self, value, acceptor):
		## if I have majority of followers
		#### broadcast seqNum, command
		self.value = value

		if self.am_leader == True:
			full_msg = str(MessageType.COMMAND.value) + ":" + str(self.idnum) + "," + str(self.seq_number) + "," + str(self.value)
			self.seq_number += 1

			acceptor.set_accept_value(self.idnum, self.seq_number, self.value) # We should also accept a value locally

			Messenger.broadcast_message(self.socket_connections_list, full_msg)
			printd("Request accepted on replica " + str(self.idnum))

		else: # if not yet leader
			self.requests_before_leadership.put(value)
			printd("Request queued because we're not the leader yet on replica " + str(idnum))


	def reply_to_client (self, clientsock, value):
		Messenger.send_message(clientsock, value)


	def send_iamleader_message(self, msg):
		# msg should be process id
		full_msg = str(MessageType.I_AM_LEADER.value) + ":" + str(self.seq_number) + "," + msg
		if self.socket_connections_list:
			Messenger.broadcast_message(self.socket_connections_list, full_msg)
		else:
			printd("ERROR: Socket connections list has not been initialized the proposer")


	def try_to_be_leader (self, acceptor):
		if not self.am_leader:
			if (self.numb_followers >= self.majority_numb):
				self.am_leader = True
				# process all queued requests from clients
				while not self.requests_before_leadership.empty():
					self.acceptRequest(self.requests_before_leadership.get(), acceptor)


		return self.am_leader

	'''def send_value (self, idnum, seq_number):
		int_seq_number = int(seq_number)
		if self.numb_followers >= self.majority_numb:
			self.am_leader = True
			if  self.broadcasted_for_seq_number[int_seq_number] == False:
				full_msg = str(MessageType.COMMAND.value) + ":" + str(idnum) + "," + str(seq_number) + "," + str(self.value)
				if self.socket_connections_list:
					Messenger.broadcast_message(self.socket_connections_list, full_msg)
				else:
					print "Socket connections list has not been initialized for the proposer"
				self.broadcasted_for_seq_number[int_seq_number] = True
		else:
			printd("Waiting for majority: " + str(self.numb_followers < self.majority_numb) + ", already_processed: " + str(self.broadcasted_for_seq_number[int_seq_number]) + ", seq_number is: " + str(seq_number))
			'''
