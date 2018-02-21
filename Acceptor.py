
from enum import Enum
import Messenger
from Messenger import MessageType
from Util import printd

'''
	This class is responsible for accepting values and leadership proposals from proposers and
	then passing these values to learners
'''

class Acceptor:

	def __init__ (self, idnum, learner):
		self.selected_leaderNum = -1 # Integer value for selected leader
		self.accepted_seqNum = -1 # sequence number for the value
		self.accepted_lastVal = -1 # Last seen value that was proposed
		self.socket_connections_list = None
		self.seq_dict = dict()
		self.idnum = idnum
		self.learner = learner


	def set_socket_list (self, socket_connections_list):
		self.socket_connections_list = socket_connections_list


	def acceptLeader (self, newLeaderNum, socket):
		# send YOU_ARE_LEADER to proposer with seqNum, accepted_lastVal, and selected_leaderNum
		# We must only promise to follow leaders with leader numbers higher than our current leader
		if int(newLeaderNum) > self.selected_leaderNum:
			self.selected_leaderNum = int(newLeaderNum)

			printd("Replica " + str(self.idnum) + " accepts leader number {}".format(newLeaderNum))
			msg = "{}:{},{},{},{}".format(MessageType.YOU_ARE_LEADER.value, self.selected_leaderNum, self.idnum, self.accepted_seqNum, self.accepted_lastVal)
		else:
			msg = "{}:{},{}".format(MessageType.NACK.value, self.selected_leaderNum, self.idnum)
			printd("Not a high enough leader id {} because our current id {}.".format(newLeaderNum, self.selected_leaderNum))

		Messenger.send_message (socket, msg)


	def accept_value (self, leaderNum, req_id, seqNum, value): # leaderNum, value
		# TODO: could be bug to not have greather than or equal...
		# int(seqNum) not in self.seq_dict:#
		#if int(leaderNum) >= self.selected_leaderNum and int(seqNum) > int(self.accepted_seqNum):
		if int(leaderNum) >= self.selected_leaderNum and int(seqNum) not in self.seq_dict:
			self.accepted_lastVal = value
			self.accepted_seqNum = seqNum
			self.seq_dict[int(seqNum)] = value
			self.learner.acceptValue(leaderNum, self.idnum, req_id, seqNum, value)
			self.send_value(leaderNum, req_id, seqNum, value)
		else:
			printd("Acceptor {} has not selected leader, leaderNum = {} and sequence number = {}, while message has been proposed from leaderNum: {} with seq_number {}".format(self.idnum, self.selected_leaderNum, self.accepted_seqNum, leaderNum, seqNum))

	def send_value (self, leaderNum, req_id, seqNum, value):
		full_msg = MessageType.ACCEPT.value + ":{},{},{},{},{}".format(leaderNum, self.idnum, req_id, seqNum, value)
		Messenger.broadcast_message(self.socket_connections_list, full_msg)


	def get_value_at_seq_number (self, missing_seq_number):
		missing_seq_number = int(missing_seq_number)
		if missing_seq_number in self.seq_dict.keys():
			printd("Replica {}'s acceptor is sending value {} for sequence number {}.".format(self.idnum, self.seq_dict[missing_seq_number], missing_seq_number))
			seq_number_found = "True"
			missing_value = self.seq_dict[missing_seq_number]
		else:
			printd("Replica {}'s accpetor is also behind sequence number {}.".format(self.idnum, missing_seq_number))
			seq_number_found = "False"
			missing_value = ''

		return (seq_number_found, self.selected_leaderNum, missing_value)


	def send_value_at_seq_number(self, socket, missing_seq_number):
		missing_seq_number = int(missing_seq_number)
		if missing_seq_number in self.seq_dict.keys():
			printd("Replica {}'s acceptor is sending value {} for sequence number {}.".format(self.idnum, self.seq_dict[missing_seq_number], missing_seq_number))
			seq_number_found = "True"
			missing_value = self.seq_dict[missing_seq_number]
		else:
			printd("Replica {}'s acceptor is also behind sequence number {}.".format(self.idnum, missing_seq_number))
			seq_number_found = "False"
			missing_value = ''
		msg = "{}:{},{},{},{},{}".format(MessageType.MISSING_VALUE.value, seq_number_found, self.idnum, self.selected_leaderNum, missing_seq_number, missing_value)
		#Messenger.broadcast_message(self.connections_list, msg)
		Messenger.send_message(socket, msg)


 # accept_value is being used in place of this function. The broadcast cannot be skipped.
	'''def set_accept_value (self, leaderID, seqNum, value):
		if leaderID == self.selected_leaderNum:
			self.accepted_lastVal = value
			self.accepted_seqNum = seqNum
		else:
			printd("Acceptor could not set accept value because leaderID = " + str(self.selected_leaderNum))'''
