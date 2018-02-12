
from enum import Enum
import Messenger
from Messenger import MessageType
from Util import printd

'''
	Proposer class of Paxos
	A proposer will broadcast messages to all acceptors (either IAmLeader or Command)
'''
class Proposer:

	def __init__ (self, majority_numb):
		self.value = "default_value"
		self.am_leader = False
		self.majority_numb = majority_numb
		self.numb_followers = 0


	def set_socket_list (self, socket_connections_list):
		self.socket_connections_list = socket_connections_list

	def acceptRequest(value, socket):
		## if I have majority of followers
		#### broadcast seqNum, command

	def send_iamleader_message(self, msg):
		# msg should be process id
		full_msg = str(MessageType.I_AM_LEADER.value) + ":" + msg
		if self.socket_connections_list:
			Messenger.broadcast_message(self.socket_connections_list, full_msg)
		else:
			print "Socket connections list has not been initialized the proposer"

	def send_value (self, idnum, seq_number):
		if self.numb_followers >= self.majority_numb:
			full_msg = str(MessageType.COMMAND.value) + ":" + str(idnum) + "," + str(seq_number) + "," + str(self.value)
			if self.socket_connections_list:
				Messenger.broadcast_message(self.socket_connections_list, full_msg)
			else:
				print "Socket connections list has not been initialized for the proposer"
		else:
			printd("Waiting for majority, only have received " + str(self.numb_followers) + " acceptances of leadership.")

