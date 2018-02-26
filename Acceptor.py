
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
		self.accepted_req_id = ".-."
		self.socket_connections_list = None
		self.seq_dict = dict()
		#self.req_dict = dict() #D2 # dict mapping req_id -> seq_num ## if you've already seen this reqeust, do a NOP
		self.idnum = idnum
		self.learner = learner
		self.locked_seq_nums = set() # need to lock seq_nums after you have sent out a value (or lack-of-value) for the seq_num


	def set_socket_list (self, socket_connections_list):
		self.socket_connections_list = socket_connections_list


	def acceptLeader (self, newLeaderNum, socket):
		# send YOU_ARE_LEADER to proposer with seqNum, accepted_lastVal, and selected_leaderNum
		# We must only promise to follow leaders with leader numbers higher than our current leader
		if int(newLeaderNum) > self.selected_leaderNum:
			self.selected_leaderNum = int(newLeaderNum)

			printd("Replica " + str(self.idnum) + " accepts leader number {}".format(newLeaderNum))
			msg = "{}:{},{},{},{},{}".format(MessageType.YOU_ARE_LEADER.value, self.selected_leaderNum, self.idnum, self.accepted_req_id, self.accepted_seqNum, self.accepted_lastVal)
		else:
			msg = "{}:{},{}".format(MessageType.NACK.value, self.selected_leaderNum, self.idnum)
			printd("Not a high enough leader id {} because our current id {}.".format(newLeaderNum, self.selected_leaderNum))

		Messenger.send_message (socket, msg)


	def accept_value (self, leaderNum, req_id, seqNum, value): # leaderNum, value
		# TODO: could be bug to not have greather than or equal...
		# int(seqNum) not in self.seq_dict:#
		#if int(leaderNum) >= self.selected_leaderNum and int(seqNum) > int(self.accepted_seqNum):
		seqNum = int(seqNum)
		if int(leaderNum) >= self.selected_leaderNum and int(seqNum) not in self.locked_seq_nums:# and int(seqNum) not in self.seq_dict: #DREW DEBUG? DOES TIS NEED TO BE HERE?
			# D2
			#if req_id in self.req_dict and self.req_dict[req_id] != int(seqNum): # if you already gave this req_id a different seq_num, send a NOP for this seq_num
			#	self.learner.acceptValue(leaderNum, self.idnum, "NOP", seqNum, "NOP")
			#	self.send_value(leaderNum, "NOP", seqNum, "NOP")
			#else: # else accept as usual
			self.accepted_lastVal = value
			self.accepted_req_id = req_id
			self.accepted_seqNum = seqNum
			self.seq_dict[int(seqNum)] = (req_id, value)
			self.locked_seq_nums.add(int(seqNum))
			self.learner.acceptValue(leaderNum, self.idnum, req_id, seqNum, value)
			self.send_value(leaderNum, req_id, seqNum, value)
			#self.req_dict[req_id] = int(seqNum) #D2
		else:
			printd("Acceptor {} did not accept value with leaderNum = {} and sequence number = {}, while message has been proposed from leaderNum: {} with seq_number {}".format(self.idnum, self.selected_leaderNum, self.accepted_seqNum, leaderNum, seqNum))



	def send_value (self, leaderNum, req_id, seqNum, value):
		full_msg = MessageType.ACCEPT.value + ":{},{},{},{},{}".format(leaderNum, self.idnum, req_id, seqNum, value)
		Messenger.broadcast_message(self.socket_connections_list, full_msg)


	def get_value_at_seq_number (self, missing_seq_number):
		missing_seq_number = int(missing_seq_number)
		if missing_seq_number in self.seq_dict.keys():
			printd("Replica {}'s acceptor is sending value {} for sequence number {}.".format(self.idnum, self.seq_dict[missing_seq_number][1], missing_seq_number))
			seq_number_found = "True"
			missing_req_id = self.seq_dict[missing_seq_number][0]
			missing_value = self.seq_dict[missing_seq_number][1]
		else:
			printd("Replica {}'s accpetor is also behind sequence number {}.".format(self.idnum, missing_seq_number))
			seq_number_found = "False"
			missing_req_id = "NONE"
			missing_value = ''

		self.locked_seq_nums.add(int(missing_seq_number))
		return ( seq_number_found, missing_req_id, missing_value)


	def send_value_at_seq_number(self, socket, missing_seq_number):
		missing_seq_number = int(missing_seq_number)
		if missing_seq_number in self.seq_dict.keys():
			printd("Replica {}'s acceptor is sending value {} for sequence number {}.".format(self.idnum, self.seq_dict[missing_seq_number], missing_seq_number))
			seq_number_found = "True"
			missing_req_id = self.seq_dict[missing_seq_number][0]
			missing_value = self.seq_dict[missing_seq_number][1]
		else:
			printd("Replica {}'s acceptor is also behind sequence number {}.".format(self.idnum, missing_seq_number))
			seq_number_found = "False"
			missing_req_id = "NONE"
			missing_value = ''
		msg = "{}:{},{},{},{},{}".format(MessageType.MISSING_VALUE.value, seq_number_found, self.idnum, missing_req_id, missing_seq_number, missing_value)
		#Messenger.broadcast_message(self.connections_list, msg)
		Messenger.send_message(socket, msg)
		self.locked_seq_nums.add(int(missing_seq_number))


 # accept_value is being used in place of this function. The broadcast cannot be skipped.
	'''def set_accept_value (self, leaderID, seqNum, value):
		if leaderID == self.selected_leaderNum:
			self.accepted_lastVal = value
			self.accepted_seqNum = seqNum
		else:
			printd("Acceptor could not set accept value because leaderID = " + str(self.selected_leaderNum))'''
