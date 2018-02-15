
from enum import Enum
import Messenger
from Messenger import MessageType
from Util import printd
'''
	This class is responsible for accepting values and leadership proposals from proposers and
	then passing these values to learners
'''
class Acceptor:

	def __init__ (self):
		self.selected_leader = -1 # Integer value for selected leader
		self.accepted_seqNum = -1 # sequence number for the value is initially none
		self.accepted_value = None # Last seen value that was proposed
		self.socket_connections_list = None


	def set_socket_list (self, socket_connections_list):
		self.socket_connections_list = socket_connections_list


	def acceptLeader (self, seq_number, newLeaderID, socket):
		if seq_number >= self.accepted_seqNum: # Only accept this leader if their sequence number is higher than we've accepted
			self.accepted_seqNum = seq_number
			outMsg = str(self.accepted_seqNum) + "," + str(self.accepted_value)
			full_msg = MessageType.YOU_ARE_LEADER.value + ":" + outMsg
			#printd("msg sent by acceptLeader: " + full_msg)
			self.selected_leader = newLeaderID
			printd("Replica accepts leader #{}".format(newLeaderID))
			Messenger.send_message (socket, full_msg)


	def set_accept_value (self, leaderID, seqNum, value):
		if leaderID == self.selected_leader:
			self.accepted_value = value
			self.accepted_seqNum = seqNum
		else:
			printd("Acceptor could not set accept value because leaderID = " + str(self.selected_leader))


	def accept_value (self, leaderID, seqNum, value): # leaderNum, value
		if leaderID == self.selected_leader:
			self.accepted_value = value
			self.accepted_seqNum = seqNum
			full_msg = MessageType.ACCEPT.value + ":{},{},{}".format(leaderID,seqNum,value)
			Messenger.broadcast_message(self.socket_connections_list, full_msg)
		else:
			printd("Acceptor has not selected leader yet because leaderID = " + str(self.selected_leader))
