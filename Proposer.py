from enum import Enum
import Messenger
from Messenger import MessageType
from Util import printd
import Queue
'''
	Proposer class of Paxos
	A proposer will broadcast messages to all acceptors (either IAmLeader or Command)
'''
class Proposer:

	def set_socket_list (self, socket_connections_list):
		self.socket_connections_list = socket_connections_list


	## Assume that any replica that wants to take leadership does it be initializing a new proposer
	def __init__ (self, idnum, majority_numb, prev_leader_numb, acceptor, learner, socket_connections_list = None):
		self.value = "default_value"
		self.am_leader = False
		self.majority_numb = majority_numb
		# I think this can be equal to one since we can count ourselves
		self.numb_followers = 1
		self.seq_number = 0
		self.requests_before_leadership = Queue.Queue()
		self.idnum = idnum
		self.leaderNum = prev_leader_numb + 1
		print (str(idnum) + " replica has leaderNum " + str(self.leaderNum))
		self.follower_collection = []
		self.acceptor = acceptor # potential bug depending on how python passes parameters (MATT SAYS: definitely by reference right?)
		self.acceptor.selected_leaderNum = self.leaderNum # At least our acceptor will follow us...
		print "LEADER NUMB IS " + str(self.leaderNum)
		self.learner = learner # same as above but I think we're good
		if socket_connections_list != None:
			self.set_socket_list(socket_connections_list)


	def acceptRequest(self, value, seq_number, seq_number_override=-1):
		## if I have majority of followers
		#### broadcast seqNum, command
		self.value = value
		#print("SEQUENCE NUMBER HERE IS " + str(self.seq_number))
		'''if seq_number_override == -1: # if no override
			self.seq_number += 1
		else: # may need to override seq_num for a leader's first command
			print "OVERRIDING SEQUENCE NUMBER"
			self.seq_number = seq_number_override'''
		self.seq_number = seq_number

		if self.am_leader == True:
			msg =  str(self.leaderNum)
			msg += "," + str(self.seq_number)
			msg += "," + str(self.value)
			full_msg = str(MessageType.COMMAND.value) + ":" + msg

			printd("Leader " + str(self.leaderNum) + "'s sequence number is " + str(self.seq_number))
			#acceptor.accept_value(self.leaderNum, self.seq_number, self.value) # We should also accept a value locally
			self.acceptor.accept_value(self.leaderNum, self.seq_number, self.value)

			Messenger.broadcast_message(self.socket_connections_list, full_msg)
			printd("Request accepted on replica {} (leader number: {})".format(str(self.idnum),str(self.leaderNum)))

		else: # if not yet leader
			self.requests_before_leadership.put((value, seq_number))
			printd("Request queued because we're not the agreed upon leader yet")


	def reply_to_client (self, clientsock, value):
		Messenger.send_message(clientsock, value)


	def send_iamleader_message(self):
		# msg should be: leadernum
		self.follower_collection = []
		# need to add its own acceptor to collection
		self.numb_followers = 1
		self.am_leader = False
		#self.leaderNum += 1
		self.requests_before_leadership = Queue.Queue() # may not need this
		full_msg = str(MessageType.I_AM_LEADER.value) + ":" + str(self.leaderNum)
		if self.socket_connections_list:
			Messenger.broadcast_message(self.socket_connections_list, full_msg)
		else:
			raise RuntimeError("Socket connections list has not been initialized for the proposer")


	# potential bug: do we need to pass in leaderNum with ACCEPT message?
	def newFollower (self, prev_leaderNum, seq_number, last_value):
		self.numb_followers += 1 # We have another follower who's joined us
		#returnList = [False]
		#self.follower_collection.append( (int(prev_leaderNum), int(seq_number), int(last_value)) ) # add follower info to collection
		if not self.am_leader: # if not leader
			if (self.numb_followers >= self.majority_numb):
				self.am_leader = True
				# need to decide most relavant last value.
				# find follower with highest prevLeaderNum. Break ties with seq_num, then val.
				max_prevLeader = -1; max_prevSeqNum = -1; max_prevVal = '';
				for i in self.follower_collection:
					if i[0] > max_prevLeader:
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
				if (max_prevVal != '' or max_prevSeqNum != -1 or max_prevLeader != -1):
					print 'HERE'
					self.acceptRequest(max_prevVal, self.acceptor, seq_number_override=max_prevSeqNum)
				#returnList = [True, max_prevVal, max_prevSeqNum]

				# TODO: do we need this, or should clients just fail if they send request before leader elected?
				# process all queued requests from clients
				while not self.requests_before_leadership.empty():
					(value, seq_numb) = self.requests_before_leadership.get()
					self.acceptRequest(value, seq_numb)


		else:
			printd(str(self.idnum) + " is the leader!")
		#seq_number = 0 # TODO: actually get sequence number
		#self.proposer.send_value(self.idnum, seq_number)

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
