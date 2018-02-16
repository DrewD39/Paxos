
from enum import Enum
import Messenger
from Messenger import MessageType
from Util import printd

'''
	This class is responsible for accepting values and leadership proposals from proposers and
	then passing these values to learners
'''

class Acceptor:

	def __init__ (self, idnum):
		self.selected_leaderNum = -1 # Integer value for selected leader
		self.accepted_seqNum = -1 # sequence number for the value
		self.accepted_lastVal = -1 # Last seen value that was proposed
		self.socket_connections_list = None
		self.idnum = idnum


	def set_socket_list (self, socket_connections_list):
		self.socket_connections_list = socket_connections_list


	def acceptLeader (self, newLeaderNum, socket):
		# send YOU_ARE_LEADER to proposer with seqNum, accepted_lastVal, and selected_leaderNum
		# We must only promise to follow leaders with leader numbers higher than our current leader
		if int(newLeaderNum) > self.selected_leaderNum:
			self.selected_leaderNum = int(newLeaderNum)

			printd("Replica " + str(self.idnum) + " accepts leader number {}".format(newLeaderNum))
			msg = "{}:{},{},{}".format(MessageType.YOU_ARE_LEADER.value, self.selected_leaderNum, self.accepted_seqNum, self.accepted_lastVal)
		else:
			msg = "{}:{}".format(MessageType.NACK.value, self.selected_leaderNum)
			printd("Not a high enough leader id {} because our current id {}.".format(newLeaderNum, self.selected_leaderNum))

		Messenger.send_message (socket, msg)


	def accept_value (self, leaderNum, seqNum, value): # leaderNum, value
		if int(leaderNum) == self.selected_leaderNum:
			self.accepted_lastVal = value
			self.accepted_seqNum = seqNum
			self.send_value(leaderNum, seqNum, value)
		else:
			printd("Acceptor " + str(self.idnum) + " has not selected leader yet because leaderNum = " + str(self.selected_leaderNum) + " and we received message from leader " + str(leaderNum))


	def send_value (self, leaderNum, seqNum, value):
		full_msg = MessageType.ACCEPT.value + ":{},{},{}".format(leaderNum,seqNum,value)
		Messenger.broadcast_message(self.socket_connections_list, full_msg)


 # accept_value is being used in place of this function. The broadcast cannot be skipped.
	'''def set_accept_value (self, leaderID, seqNum, value):
		if leaderID == self.selected_leaderNum:
			self.accepted_lastVal = value
			self.accepted_seqNum = seqNum
		else:
			printd("Acceptor could not set accept value because leaderID = " + str(self.selected_leaderNum))'''
