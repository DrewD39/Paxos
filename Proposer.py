from enum import Enum
import Messenger
from Messenger import MessageType
from Util import printd
import Queue
from collections import defaultdict

'''
	Proposer class of Paxos
	A proposer will broadcast messages to all acceptors (either IAmLeader or Command)
'''
class Proposer:

	def set_socket_list (self, socket_connections_list):
		self.socket_connections_list = socket_connections_list


	## Assume that any replica that wants to take leadership does it be initializing a new proposer
	def __init__ (self, idnum, majority_numb, acceptor, kills, skips, socket_connections_list = None):
		self.value = "default_value"
		self.am_leader = False
		self.majority_numb = majority_numb
		self.followers = set()
		self.seq_number = -1
		self.idnum = idnum
		self.leaderNum = 0
		self.follower_collection = []
		self.acceptor = acceptor # potential bug depending on how python passes parameters (MATT SAYS: definitely by reference right?)
		self.acceptor.selected_leaderNum = self.leaderNum # At least our acceptor will follow us...
		if socket_connections_list != None:
			self.set_socket_list(socket_connections_list)
		self.skips = skips
		self.kills = kills
		self.missing_vals_of_learners = defaultdict(set) # dict of key: seq_num -> val: number of learners missing the value at this seq_num



	def acceptRequest(self, origin_socket, client_name, client_seq_number, value): # value, seq_number, seq_number_override=-1): self.client_name, self.client_seq_number, value
		self.value = value
		req_id = str(client_name) + '-' + str(client_seq_number)

		if self.am_leader == True:
			#if ( (client_name,client_seq_number) not in self.request_history ):  # if you've never seen this client_name, client_seq_num pair, it is a new request
			self.seq_number += 1
			if self.seq_number in self.skips: 		# skip logic: set up leader 0 to skip seq_num 3, then be killed on seq_num 5
				print("\nMANUALLY SKIPPING SEQ_NUM {}".format(self.seq_number))
				self.seq_number += 1

			msg =  str(self.leaderNum)
			msg += "," + req_id
			msg += "," + str(self.seq_number)
			msg += "," + str(self.value)
			full_msg = str(MessageType.COMMAND.value) + ":" + msg

			printd("Leader " + str(self.leaderNum) + "'s sequence number is " + str(self.seq_number))
			self.acceptor.accept_value(self.leaderNum, req_id, self.seq_number, self.value) # We should also accept a value locally

			Messenger.broadcast_message(self.socket_connections_list, full_msg)
			printd("Request accepted on replica {} (leader number: {})".format(str(self.idnum),str(self.leaderNum)))


	def send_iamleader_message(self):
		self.follower_collection = []
		# need to add its own acceptor to collection
		self.followers = set()
		self.followers.add(self.idnum) # Let's include ourselves
		self.am_leader = False
		if self.acceptor.selected_leaderNum > self.leaderNum:
			self.leaderNum = self.acceptor.leaderNum
			self.value = self.acceptor.accepted_lastVal
		else:
			self.acceptor.selected_leaderNum = self.leaderNum

		printd("Sending message with leader value " + str(self.leaderNum))
		full_msg = str(MessageType.I_AM_LEADER.value) + ":" + str(self.leaderNum)
		if self.socket_connections_list:
			Messenger.broadcast_message(self.socket_connections_list, full_msg)
		else:
			raise RuntimeError("Socket connections list has not been initialized for the proposer")


	def newFollower (self, prev_leaderNum, acceptor_id, req_id, seq_number, last_value):
		self.follower_collection.append( (int(prev_leaderNum), str(req_id), int(seq_number), str(last_value)) ) # add follower info to collection
		if not self.am_leader and acceptor_id not in self.followers: # if not leader

			self.followers.add(acceptor_id) # We have another follower who's joined us

			if len(self.followers) == self.majority_numb: # time to become leader since we have enough followers
				self.am_leader = True
				self.seq_number = int(seq_number)
				# do not repeat manual kills
				if int(seq_number) in self.kills:
					self.kills.remove(int(seq_number))
				if int(seq_number)+1 in self.kills:
					self.kills.remove(int(seq_number)+1)

				# need to decide most relavant last value.
				# find follower with highest prevLeaderNum. Break ties with seq_num, then val.
				max_last_follower = [-1, ".-.", -1, '']
				max_prevLeader = -1; max_prevSeqNum = -1; max_prevVal = '';
				for follower in self.follower_collection:
					if follower[0] > max_last_follower[0]: # if prev_leaderNum > max_prevLeader
						max_last_follower = follower
						continue
					if follower[0] == max_last_follower[0] and follower[2] > max_last_follower[2]:
						max_last_follower = follower
						continue
					if follower[0] == max_last_follower[0] and follower[2] == max_last_follower[2] and follower[3] > max_last_follower[3]:
						max_last_follower = follower
						continue
				# As first order of business as new leader, broadcast the most recent/relavant message you got back from accepts
				if (int(max_last_follower[0]) != -1 and int(max_last_follower[2]) != -1):

					# do not repeat manual kills
					self.seq_number = max_last_follower[2]-1
					if int(self.seq_number) in self.kills:
						self.kills.remove(int(self.seq_number))
					if int(self.seq_number)+1 in self.kills:
						self.kills.remove(int(self.seq_number+1))
					n_client_name = max_last_follower[1].split('-')[0]
					n_client_seq_num = max_last_follower[1].split('-')[1]
					n_val = max_last_follower[3]
					self.acceptRequest(None, n_client_name, n_client_seq_num, n_val)

					printd("Replica {} just became the leader with value = {}, sequence number = {}, req_id = {}".format(self.idnum, n_val, self.seq_number,max_last_follower[1]))
				else:
					printd("Replica {} just became the leader with default values".format(self.idnum))


		else:
			printd(str(self.idnum) + " was already the leader!")


	def set_leader_num (self, highest_leader_num, idnum):
		if self.leaderNum <= int(highest_leader_num): # and not self.am_leader:
			self.leaderNum = int(highest_leader_num) + 1 # Set leader number to one higher so we can be leader
			self.send_iamleader_message() # Try again to be leader...


	def note_missing_value (self, seq_number_found, learner_id, missing_seq_number):

		printd("NOTE MISSING VALUE {}".format(missing_seq_number))
		missing_seq_number = int(missing_seq_number)
		learner_id = int(learner_id)

		self.missing_vals_of_learners[missing_seq_number].add(learner_id)

		if seq_number_found == "True":
			self.missing_vals_of_learners[missing_seq_number] = set()
			return
		elif seq_number_found == "False":
			printd(self.missing_vals_of_learners[missing_seq_number])
			if len(self.missing_vals_of_learners[missing_seq_number]) >= self.majority_numb: # Count ourselves too
				self.acceptor.accept_value(self.leaderNum, "NOP-SHOULD NOT HAPPEN", missing_seq_number, "NOP-SHOULD NOT HAPPEN") # We should also accept a value locally
				if int(missing_seq_number) == self.seq_number + 1:
					printd("INCREMENTING SEQ NUMBER IN PROPOSER")
					self.seq_number += 1

				full_msg = str(MessageType.COMMAND.value) + ":{},NOP,{},NOP1".format(self.leaderNum,missing_seq_number)
				Messenger.broadcast_message(self.socket_connections_list, full_msg)
				self.missing_vals_of_learners[missing_seq_number] = set()
				printd("Leader num {} is proposing NOP at seq_num {}".format(self.leaderNum,missing_seq_number))
		else:
			raise RuntimeError("Error: invalid seq_number_found arg for MISSING_VALUE command")
