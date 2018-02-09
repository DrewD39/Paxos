
from enum import Enum 
import Messenger
from Messenger import MessageType

'''
	Proposer class of Paxos
	A proposer will broadcast messages to all acceptors (either IAmLeader or Command)
'''
class Proposer:

	def __init__ (self):
		self.value = None
		self.am_leader = False
	

	def set_socket_list (self, socket_connections_list):
		self.socket_connections_list = socket_connections_list

	
	def send_leader_message(self, msg):
		full_msg = str(MessageType.I_AM_LEADER.value) + ": " + msg
		if self.socket_connections_list:
			Messenger.broadcast_message(self.socket_connections_list, full_msg)
		else:
			print "Socket connections list has not been initialized"
