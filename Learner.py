
from Util import printd
from Util import pop_req_id_from_pq
from Queue import PriorityQueue
import Messenger
from Messenger import MessageType
from operator import itemgetter
from collections import defaultdict
import hashlib

'''
	This class will be the final decider of values
'''
class Learner:


	def __init__ (self, num_replicas, majority_numb, idnum):
		# DREW: had to change from list to dict to accomadate NOP
		self.chat_log = [] # nvm # key: seq_num -> val: value
		self.num_replicas = num_replicas
		self.majority_numb = majority_numb
		# I think this should be equal to 1 since we can count ourselves in the majority
		self.seq_dict = defaultdict(set) # This is a mapping of sequence number -> dictionary with key = value -> count
		self.commands_to_execute = PriorityQueue()
		self.last_executed_seq_number = - 1 # We haven't executed any commands yet
		self.idnum = idnum
		self.client_mapping = dict()
		self.connections_list = None
		self.prev_leader_nums = defaultdict(dict) #D2 # dict of dict: seq_num -> req_id -> set(acceptor_id)
		self.acceptor = None
		self.proposer = None
		self.accepted_seq_numbs = dict()
		#self.missing_vals_of_learners = dict() # dict of key: seq_num -> val: number of learners missing the value at this seq_num
		self.exec_req_set = set() #D2
		self.catchup_requests_count = dict() # serves as timeout
		self.hasher = hashlib.md5()
		self.hash_count = 0
		self.exec_req_history = set() # set of req_id that have been exectured (exclude NOP)

	def acceptValue (self, leaderNum, idnum, req_id, seq_number, value):
		seq = int(seq_number)

		if seq > self.last_executed_seq_number:# or req_id == "NOP": # Else we should ignore

			#if seq not in self.seq_dict.keys():
			#	self.seq_dict[seq] = dict()

			self.seq_dict[seq].add(int(idnum)) # We've now seen one of these values
			#else:
				#self.seq_dict[seq][req_id] += 1 # Increment the number of messages we've seen for this sequence number and value

			if len(self.seq_dict[seq]) == self.majority_numb:
				# Execute commnand
				#self.add_msg_to_chat_log(value)
				# We shouldn't execute again for this seq_number, and since we've already received
				# a majority, we're guaranteed to not receive a majority again
				#self.seq_dict[seq_number] = 0
				printd(str(self.idnum) + " has majority for value at {} of {} (last exec seq num = {})".format(seq_number,str(value),self.last_executed_seq_number))
				self.reply_to_client(req_id, value)				   # so we can go ahead and reply to the client
				self.add_and_execute_seq_command(seq, value, req_id)
				#self.commands_to_execute.put((seq, value, req_id)) # once it is in here is is guaranteed to execute. Eventually.
				#self.try_to_execute_commands()
				del self.seq_dict[seq]
				return True
			else:
				printd("{} cannot execute for {},{} because we've only seen messages from{}.".format(self.idnum, seq_number, value, self.seq_dict[seq]))
				#printd("Don't have majority for learner yet..., seq_number " + seq_number + " and values_list = "  + str(self.seq_dict[seq_number]))
				return False


	def set_acceptor(self, acceptor):
		self.acceptor = acceptor


	def set_proposer(self, proposer):
		self.proposer = proposer


	def try_to_execute_commands (self):

		#for i in range(self.last_executed_seq_number + 1, int(self.commands_to_execute.queue[0][0])):
		if not self.commands_to_execute.empty() and self.last_executed_seq_number + 1 < int(self.commands_to_execute.queue[0][0]):
			printd("Replica {} sending catchup because it's missing {}.".format(self.idnum, self.last_executed_seq_number + 1).upper())
			#self.missing_vals_of_learners[i] = 1 # keep track of how many learners are missing this value
			(seq_number_found, missing_req_id, missing_value) = self.acceptor.get_value_at_seq_number(self.last_executed_seq_number + 1)
			self.fill_missing_value(seq_number_found, self.idnum, missing_req_id, self.last_executed_seq_number + 1, missing_value)
			if self.proposer:
				self.proposer.note_missing_value(seq_number_found, self.idnum, self.last_executed_seq_number + 1
				)
			else:
				printd("NO PROPOSER FOR " + str(self.idnum))

			msg = "{}:{}".format(MessageType.CATCHUP.value, self.last_executed_seq_number + 1)
			Messenger.broadcast_message (self.connections_list, msg)

			# keep track of how many times you request catchup. After so many, timeout
			if self.last_executed_seq_number + 1 in self.catchup_requests_count:
				self.catchup_requests_count[self.last_executed_seq_number + 1] += 1
			else:
				self.catchup_requests_count[self.last_executed_seq_number + 1] = 1
			printd("CATCHUP ATTEMPT COUNT: {}".format(self.catchup_requests_count[self.last_executed_seq_number + 1]))
			return
		#else:
			#print "Replica {} has queue {}.".format(self.idnum, self.commands_to_execute.queue)

		# Convoluted way to peek at PriorityQueue
		while not self.commands_to_execute.empty() and int(self.commands_to_execute.queue[0][0]) == self.last_executed_seq_number + 1:
			command = self.commands_to_execute.get()
			self.execute_command(command)


	def execute_command (self, command):
		seq_number = command[0]
		value = command[1]
		req_id = command[2]

		self.add_msg_to_chat_log(seq_number, value, req_id)
		self.last_executed_seq_number = max(self.last_executed_seq_number,int(seq_number))

		printd(str(self.idnum) + " EXECUTES COMMAND " + str(command))


	# command succesfully executed
	def reply_to_client (self, req_id, value):
		if req_id == "NOP" or req_id == "NONE":
			return # no client to reply to
		client_name, client_seq_number = req_id.split('-')

		if client_name in self.client_mapping: # This client name must be in the client mapping
			clientsock = self.client_mapping[client_name]
			printd("Responding to client {} with client_seq_number {}.".format(client_name, client_seq_number))
			Messenger.send_message(clientsock, req_id)
		else:
			raise RuntimeError("This client name: {}, is not in our mapping for replica {}.".format(client_name, self.idnum))


	def add_client (self, clientname, clientsock):
		self.client_mapping[clientname] = clientsock


	def add_and_execute_seq_command (self, seq_number, value, req_id):
		if seq_number not in self.accepted_seq_numbs and seq_number != -1:
			if req_id not in self.exec_req_set: #D2 # if you have not already executed for this req_id
				self.commands_to_execute.put((seq_number, value, req_id)) #D2
				self.exec_req_set.add(req_id) #D2
			else: # D2 # if you have executed for this req_id, but have majority again, execute NOP
				alt_req = pop_req_id_from_pq(self.commands_to_execute, req_id) # remove and return the item with matching req_id in the pq
				if alt_req == None: # req-id has already been executed
					if req_id in self.exec_req_history and req_id != "NOP":
						self.commands_to_execute.put((seq_number, value, req_id))
					else:
						self.commands_to_execute.put((seq_number, "NOP2", "NOP"))

				elif alt_req[0] > seq_number: # put the request with lowest seq_num onto the pq. The other will be a NOP
					self.commands_to_execute.put((seq_number, value, req_id))
					self.commands_to_execute.put((alt_req[0], "NOP3", "NOP"))
				else:
					self.commands_to_execute.put(alt_req)
					self.commands_to_execute.put((seq_number, "NOP4", "NOP"))
			self.accepted_seq_numbs[seq_number] = True
			self.try_to_execute_commands() # Now try to process commands again


	def fill_missing_value (self, seq_number_found, acceptor_id, missing_req_id, missing_seq_number, missing_value):
		#if missing_seq_number in self.missing_vals_of_learners: # if we have not already resolved this issue
		missing_seq_number = int(missing_seq_number)
		if missing_req_id not in self.prev_leader_nums[missing_seq_number]:
			self.prev_leader_nums[missing_seq_number][missing_req_id] = set()

		self.prev_leader_nums[missing_seq_number][missing_req_id].add(acceptor_id)
		printd("{} IN MISSING VALUE, NUM UNIQUE REQ_IDS = {}".format(self.idnum,len(self.prev_leader_nums[missing_seq_number])))

		if missing_seq_number in self.prev_leader_nums:

					# do not need to iterate through req_ids. In this call, only the current req_id could have attained majority
			#for i in range(len(self.prev_leader_nums[missing_seq_number])): # iterate through each unique req_id
			#	if i not in self.prev_leader_nums[missing_seq_number]:
			#		continue
			sum_of_votes = 0
			for i in self.prev_leader_nums[missing_seq_number].values():
				sum_of_votes += len(i)
			printd("{} has {} votes for req_id: {} at seq_num: {} (total num votes = {})".format(self.idnum,len(self.prev_leader_nums[missing_seq_number][missing_req_id]),missing_req_id,missing_seq_number,sum_of_votes))
			if len(self.prev_leader_nums[missing_seq_number][missing_req_id]) == self.majority_numb: # if this seq_num, req_id combo has majority
				value = missing_value #max(self.prev_leader_nums[missing_seq_number], key=itemgetter(0))[1]
				del self.prev_leader_nums[missing_seq_number]

				if missing_seq_number > self.last_executed_seq_number and self.commands_to_execute.queue and missing_seq_number < int(self.commands_to_execute.queue[0][0]): # ignore previous messages
					#self.chat_log[missing_seq_number] = missing_value
					# DREW: why is this the case? The last executed command shouldn't change, right? #
					#self.last_executed_seq_number = missing_seq_number # + 1

					#del self.missing_vals_of_learners[missing_seq_number]
					printd("A different learner had the missing value. Fixing internal to the learners")
					if seq_number_found == "True":
						self.add_and_execute_seq_command(missing_seq_number, value, missing_req_id)
					else:
						self.add_and_execute_seq_command(missing_seq_number, "NOP5", "NOP")
						#print("Defaulted to NOP when seq_num = {}, val = {}, and req_id ={}".format(missing_seq_number, value, missing_req_id))
					#self.commands_to_execute.put((missing_seq_number, value, "NONE"))
					#self.try_to_execute_commands() # Now try to process commands again

			else: # need to check if majority impossible. If so, take NOP
				sum_of_votes = 0
				for i in self.prev_leader_nums[missing_seq_number].values():
					sum_of_votes += len(i)
					# if this is true, it is unable to achieve majority and all learners should execute NOP (catchup_requests_count acts as timeout)
					if sum_of_votes >= self.majority_numb and self.catchup_requests_count[missing_seq_number] > 20:
						#print("Catchup attempts: {}".format(self.catchup_requests_count[missing_seq_number]))
						self.add_and_execute_seq_command(missing_seq_number, "NOP6", "NOP")

			'''
			else: # check if this req-id is already in the to-execute queue. If so, this is a NOP
				for i in self.commands_to_execute.queue:
					if i[2] == missing_req_id:
						self.add_and_execute_seq_command(missing_seq_number, "NOP", "NOP")
						printd("REQ_ID COMING LATER. DO NOP NOW")
			'''



		#else:
			#print("{}, {}.".format(missing_seq_number, self.commands_to_execute.queue))
			# DREW: decided to move this logic to proposer. Will delete...
			#self.missing_vals_of_learners[missing_seq_number] += 1
			# if a majority of learners are also missing this value, let the proposer know
			#if self.missing_vals_of_learners[missing_seq_number] >= self.majority_numb:

		#else:
		#	raise RuntimeError("Error: invalid seq_number_found arg for MISSING_VALUE command")
		#else:
		#	return # already resolved. no action required.


	def set_socket_list (self, connections_list):
		self.connections_list = connections_list


	def add_msg_to_chat_log (self, seq_number, msg, req_id):
		self.chat_log.append(req_id.split('-')[0] + ": " + msg)

		# point of EXECUTION
		# if NOP, add to chat log but do not print ('execute'). Also check seq_num for hashing
		if str(req_id) != "NOP": # do nothing for NO OP
			self.file_log = open("replica_" + str(self.idnum) + ".log", "a")
			self.file_log.write(self.chat_log[seq_number] + '\n')#self.get_chat_log() + "\n")
			self.file_log.close()
			self.exec_req_history.add(req_id)
		#else:
		#	print("{} executing NOP with value {} at seq_num {}".format(self.idnum,self.chat_log[seq_number],seq_number))
		if seq_number % 49 == 0 and seq_number != 0: # print hash every 50 commands
			self.hash_count += 1
			with open("replica_" + str(self.idnum) + ".log", 'rb') as afile:
				buf = afile.read()
				self.hasher.update(buf)
			print("Replica #{}, hash #{} | hash: {}".format(self.idnum,self.hash_count,self.hasher.hexdigest()))


	def get_chat_log (self):
		#chat_log_list = []
		#for i in range(0, self.last_executed_seq_number+1):
		#	if i in self.chat_log:
		#		chat_log_list.append(self.chat_log[i])
		#return ("Chat log for " + str(self.idnum) + ":\n\t" + '\n\t'.join(self.chat_log))
		return "\n\t" + '\n\t'.join(self.chat_log)
