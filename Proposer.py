
from enum import Enum
import Messenger
from Messenger import MessageType
from Util import printd

'''
	Proposer class of Paxos
	A proposer will broadcast messages to all acceptors (either IAmLeader or Command)
'''
class Proposer:


	seq_list_size = 10000


	def __init__ (self, majority_numb):
		self.value = "default_value"
		self.am_leader = False
		self.majority_numb = majority_numb
		# I think this can be equal to one since we can count ourselves
		self.numb_followers = 1
		self.seq_number = 0
		self.broadcasted_for_seq_number = self.seq_list_size * [False]


	def set_socket_list (self, socket_connections_list):
		self.socket_connections_list = socket_connections_list


	def acceptRequest(self, idnum, value):
		## if I have majority of followers
		#### broadcast seqNum, command
		if self.am_leader == False:
			self.value = value
			self.seq_number += 1
			full_msg = str(MessageType.COMMAND.value) + ":" + str(idnum) + "," + str(self.seq_number) + "," + str(self.value)

			Messenger.broadcast_message(self.socket_connections_list, full_msg)
			printd("Request accepted on replica " + str(idnum))

		else: # We're not yet the leader so we should request to be leader
			self.send_iamleader_message(str(idnum))
			printd("Request denied because we're not the leader yet on replica " + str(idnum))


	def send_iamleader_message(self, msg):
		# msg should be process id
		full_msg = str(MessageType.I_AM_LEADER.value) + ":" + msg
		if self.socket_connections_list:
			Messenger.broadcast_message(self.socket_connections_list, full_msg)
		else:
			print "Socket connections list has not been initialized the proposer"


	def send_value (self, idnum, seq_number):
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
