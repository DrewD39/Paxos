
from enum import Enum 
import Messenger

'''
	Proposer class of Paxos
	A proposer will broadcast messages to all acceptors (either IAmLeader or Command)
'''
class Proposer:

	def __init__ (self, server_list):
		self.value = None
		self.am_leader = False
		self.other_replicas = server_list

	
	def send_leader_message(self, msg):
		full_msg = str(Messenger.IAmLeader) + ": " + msg
		Messenger.broadcast_message(self.socket_connections_list, full_msg)
