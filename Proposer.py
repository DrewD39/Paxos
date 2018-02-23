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
	def __init__ (self, idnum, majority_numb, acceptor, skips, socket_connections_list = None):
		self.value = "default_value"
		self.am_leader = False
		self.majority_numb = majority_numb
		# I think this can be equal to one since we can count ourselves
		self.followers = set()
		self.seq_number = -1
		#self.requests_before_leadership = Queue.Queue()
		self.idnum = idnum
		self.leaderNum = 0
		self.follower_collection = []
		self.acceptor = acceptor # potential bug depending on how python passes parameters (MATT SAYS: definitely by reference right?)
		self.acceptor.selected_leaderNum = self.leaderNum # At least our acceptor will follow us...
		self.request_history = dict() # key = client_name,client_seq_num tuple; value = socket of origin,internal_seq_num tuple
		if socket_connections_list != None:
			self.set_socket_list(socket_connections_list)
		self.skips = skips
		self.missing_vals_of_learners = defaultdict(set) # dict of key: seq_num -> val: number of learners missing the value at this seq_num



	def acceptRequest(self, origin_socket, client_name, client_seq_number, value): # value, seq_number, seq_number_override=-1): self.client_name, self.client_seq_number, value
		## if I have majority of followers
		#### broadcast seqNum, command
		self.value = value
		req_id = str(client_name) + '-' + str(client_seq_number)
		#print("SEQUENCE NUMBER HERE IS " + str(self.seq_number))
		'''if seq_number_override == -1: # if no override
			self.seq_number += 1
		else: # may need to override seq_num for a leader's first command
			print "OVERRIDING SEQUENCE NUMBER"
			self.seq_number = seq_number_override'''


		#if ( (client_name,client_seq_number) not in self.request_history ):  # if you've never seen this client_name, client_seq_num pair, it is a new request
		self.seq_number += 1
		if self.seq_number in self.skips: 		# skip logic: set up leader 0 to skip seq_num 3, then be killed on seq_num 5
			printd("MANUALLY SKIPPING SEQ_NUM {}".format(self.seq_number))
			self.seq_number += 1

		self.request_history[(client_name,client_seq_number)] = (origin_socket, self.seq_number)


		if self.am_leader == True:
			msg =  str(self.leaderNum)
			msg += "," + req_id
			msg += "," + str(self.seq_number)
			msg += "," + str(self.value)
			full_msg = str(MessageType.COMMAND.value) + ":" + msg

			printd("Leader " + str(self.leaderNum) + "'s sequence number is " + str(self.seq_number))
			self.acceptor.accept_value(self.leaderNum, req_id, self.seq_number, self.value) # We should also accept a value locally

			Messenger.broadcast_message(self.socket_connections_list, full_msg)
			printd("Request accepted on replica {} (leader number: {})".format(str(self.idnum),str(self.leaderNum)))

		#else: # if not yet leader
		#	self.requests_before_leadership.put((value, self.seq_number))
			#printd("Request queued because we're not the agreed upon leader yet")


	def send_iamleader_message(self):
		# msg should be: leadernum
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

		#self.leaderNum += 1
		#self.requests_before_leadership = Queue.Queue() # may not need this
		printd("Sending message with leader value " + str(self.leaderNum))
		full_msg = str(MessageType.I_AM_LEADER.value) + ":" + str(self.leaderNum)
		if self.socket_connections_list:
			Messenger.broadcast_message(self.socket_connections_list, full_msg)
		else:
			raise RuntimeError("Socket connections list has not been initialized for the proposer")


	# potential bug: do we need to pass in leaderNum with ACCEPT message?
	def newFollower (self, prev_leaderNum, acceptor_id, seq_number, last_value):
		#returnList = [False]
		self.follower_collection.append( (int(prev_leaderNum), int(seq_number), last_value) ) # add follower info to collection
		if not self.am_leader and acceptor_id not in self.followers: # if not leader

			self.followers.add(acceptor_id) # We have another follower who's joined us

			if len(self.followers) == self.majority_numb: # time to become leader since we have enough followers
				self.am_leader = True
				self.seq_number = int(seq_number) # TODO BUG: This seems like a bug....
				# need to decide most relavant last value.
				# find follower with highest prevLeaderNum. Break ties with seq_num, then val.
				max_prevLeader = -1; max_prevSeqNum = -1; max_prevVal = '';
				for i in self.follower_collection:
					if i[0] > max_prevLeader: # if prev_leaderNum > max_prevLeader
						max_prevLeader = i[0]
						max_prevSeqNum = i[1]
						max_prevVal =    i[2]
						continue
					if i[0] == max_prevLeader and i[1] > max_prevSeqNum:
						max_prevSeqNum = i[1]
						max_prevVal =    i[2]
						continue
					if i[0] == max_prevLeader and i[1] == max_prevSeqNum and i[2] > max_prevVal:
						max_prevVal =    i[2]
						continue
				# As first order of business as new leader, broadcast the most recent/relavant message you got back from accepts
				# acceptRequest requires acceptor object, so it needs to be called from replica
				# so we return a list of args that replica can use to call accept request
				# alternative: pass in acceptor on initialize. Possible bug if mem references aren't shared
				# this function will set seq_num and value accordingly
				if (max_prevVal != '' or max_prevSeqNum != -1 or max_prevLeader != ''):
					# TODO: We need to send out this value, eventually
					#self.acceptRequest(max_prevVal, self.acceptor, seq_number_override=max_prevSeqNum)
					self.seq_number = int(max_prevSeqNum)
					self.value = max_prevVal

				printd("Replica {} just became the leader with value = {} and sequence number = {}".format(self.idnum, self.value, seq_number))

				#returnList = [True, max_prevVal, max_prevSeqNum]

				# Logic to handle skipped seq_number from previous leaders


				# TODO: do we need this, or should clients just fail if they send request before leader elected?
				# process all queued requests from clients
				#while not self.requests_before_leadership.empty():
				#	(value, seq_numb) = self.requests_before_leadership.get()
				#	self.acceptRequest(value, seq_numb)


		else:
			printd(str(self.idnum) + " was already the leader!")
		#seq_number = 0 # TODO: actually get sequence number
		#self.proposer.send_value(self.idnum, seq_number)


	def set_leader_num (self, highest_leader_num, idnum):
		if self.leaderNum <= int(highest_leader_num): # and not self.am_leader:
			self.leaderNum = int(highest_leader_num) + 1 # Set leader number to one higher so we can be leader
			self.send_iamleader_message() # Try again to be leader...


	def note_missing_value (self, seq_number_found, learner_id, missing_seq_number):

		printd("NOTE MISSING VALUE {}".format(missing_seq_number))
		missing_seq_number = int(missing_seq_number)
		learner_id = int(learner_id)
		# if this is the first time seeing a missing val at this seq_num

		self.missing_vals_of_learners[missing_seq_number].add(learner_id)

		# if the value at seq_num is found in a learner, let the learners resolve it
		if seq_number_found == "True":
			self.missing_vals_of_learners[missing_seq_number] = set()
			return
		elif seq_number_found == "False":
			# if there is a majority of learners missing this value, send a NOP
			print self.missing_vals_of_learners[missing_seq_number]
			if len(self.missing_vals_of_learners[missing_seq_number]) >= self.majority_numb: # Count ourselves too
				self.acceptor.accept_value(self.leaderNum, "NOP-SHOULD NOT HAPPEN", missing_seq_number, "NOP-SHOULD NOT HAPPEN") # We should also accept a value locally
				if int(missing_seq_number) == self.seq_number + 1:
					printd("INCREMENTING SEQ NUMBER IN PROPOSER")
					self.seq_number += 1

				full_msg = str(MessageType.COMMAND.value) + ":{},NOP,{},NOP".format(self.leaderNum,missing_seq_number)
				Messenger.broadcast_message(self.socket_connections_list, full_msg)
				self.missing_vals_of_learners[missing_seq_number] = set()
				printd("Leader num {} is proposing NOP at seq_num {}".format(self.leaderNum,missing_seq_number))
		else:
			raise RuntimeError("Error: invalid seq_number_found arg for MISSING_VALUE command")
		


	'''def send_value (self, [idnum OR leaderNum?], seq_number):
		int_seq_number = int(seq_number)
		if self.numb_followers >= self.majority_numb:
			self.am_leader = True
			if  self.broadcasted_for_seq_number[int_seq_number] == False:
				full_msg = str(MessageType.COMMAND.value) + ":" + str([idnum OR leaderNum?]) + "," + str(seq_number) + "," + str(self.value)
				if self.socket_connections_list:
					Messenger.broadcast_message(self.socket_connections_list, full_msg)
				else:
					print "Socket connections list has not been initialized for the proposer"
				self.broadcasted_for_seq_number[int_seq_number] = True
		else:
			printd("Waiting for majority: " + str(self.numb_followers < self.majority_numb) + ", already_processed: " + str(self.broadcasted_for_seq_number[int_seq_number]) + ", seq_number is: " + str(seq_number))
			'''
