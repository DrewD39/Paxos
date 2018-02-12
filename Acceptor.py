
from enum import Enum
import Messenger
from Messenger import MessageType
from Util import printd
'''

'''
class Acceptor:

	def __init__ (self):
		self.selected_leader = None # Integer value for selected leader
		self.accepted_seqNum = None # sequence number for the value
		self.accepted_value = None # Last seen value that was proposed
		self.socket_connections_list = None

	def set_socket_list (self, socket_connections_list):
		self.socket_connections_list = socket_connections_list

	def acceptLeader (self, newLeaderID, socket):
		outMsg = str(self.accepted_seqNum) + "," + str(self.accepted_value)
		full_msg = MessageType.YOU_ARE_LEADER.value + ":" + outMsg
		printd("msg sent by acceptLeader: " + full_msg)
		self.selected_leader = newLeaderID
		printd("Replica accepts leader #{}".format(newLeaderID))
		Messenger.send_message (socket, full_msg)

	def acceptValue(self, leaderID, seqNum, value, socket): # leaderNum, value, socket
		if leaderID == self.selected_leader:
			self.accepted_value = value
			full_msg = MessageType.ACCEPT.value + ":{},{},{}".format(leaderID,seqNum,value)
			Messenger.broadcast_message(self.socket_connections_list, full_msg)
