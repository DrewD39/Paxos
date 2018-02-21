
from Util import printd
from Queue import PriorityQueue
import Messenger
from Messenger import MessageType
from operator import itemgetter
from collections import defaultdict

'''
	This class will be the final decider of values
'''
class Learner:


	def __init__ (self, majority_numb, idnum):
		# DREW: had to change from list to dict to accomadate NOP
		self.chat_log = [] # nvm # key: seq_num -> val: value
		self.majority_numb = majority_numb
		# I think this should be equal to 1 since we can count ourselves in the majority
		self.seq_dict = defaultdict(set) # This is a mapping of sequence number -> dictionary with key = value -> count
		self.commands_to_execute = PriorityQueue()
		self.last_executed_seq_number = - 1 # We haven't executed any commands yet
		self.idnum = idnum
		self.client_mapping = dict()
		self.connections_list = None
		self.prev_leader_nums = defaultdict(list)
		self.acceptor = None
		self.proposer = None
		self.accepted_seq_numbs = dict()
		#self.missing_vals_of_learners = dict() # dict of key: seq_num -> val: number of learners missing the value at this seq_num


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
			(seq_number_found, leader_num, missing_value) = self.acceptor.get_value_at_seq_number(self.last_executed_seq_number + 1)
			self.fill_missing_value(seq_number_found, leader_num, self.last_executed_seq_number + 1, missing_value)
			if self.proposer:
				self.proposer.note_missing_value(seq_number_found, self.idnum, self.last_executed_seq_number + 1
				)
			else:
				printd("NO PROPOSER FOR " + str(self.idnum))

			msg = "{}:{}".format(MessageType.CATCHUP.value, self.last_executed_seq_number + 1)
			Messenger.broadcast_message (self.connections_list, msg)
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
		if seq_number not in self.accepted_seq_numbs:
			self.accepted_seq_numbs[seq_number] = True
			self.commands_to_execute.put((seq_number, value, "NONE"))
			self.try_to_execute_commands() # Now try to process commands again


	def fill_missing_value (self, seq_number_found, leader_num, missing_seq_number, missing_value):
		#if missing_seq_number in self.missing_vals_of_learners: # if we have not already resolved this issue
		if seq_number_found == "True":
			self.prev_leader_nums[missing_seq_number].append((leader_num, missing_value))
			printd("IN MISSING VALUE, LEN = {}".format(len(self.prev_leader_nums[missing_seq_number])))

		if missing_seq_number in self.prev_leader_nums and len(self.prev_leader_nums[missing_seq_number]) == self.majority_numb:
			value = max(self.prev_leader_nums[missing_seq_number], key=itemgetter(0))[1]
			del self.prev_leader_nums[missing_seq_number]

			missing_seq_number = int(missing_seq_number)
			if seq_number_found == "True" and missing_seq_number > self.last_executed_seq_number and missing_seq_number < int(self.commands_to_execute.queue[0][0]): # ignore previous messages
				#self.chat_log[missing_seq_number] = missing_value
				# DREW: why is this the case? The last executed command shouldn't change, right? #
				#self.last_executed_seq_number = missing_seq_number # + 1

				#del self.missing_vals_of_learners[missing_seq_number]
				printd("A different learner had the missing value. Fixing internal to the learners")
				self.add_and_execute_seq_command(missing_seq_number, value, "NONE")
				#self.commands_to_execute.put((missing_seq_number, value, "NONE"))
				#self.try_to_execute_commands() # Now try to process commands again

			elif seq_number_found == "False":
				return

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
		#if req_id == "NOP":
		#	return # do not perform any execution on a NOP
		self.chat_log.append(str(seq_number) + "|" + msg)
		# TODO: just for debugging, later remove this
		#print self.get_chat_log()

		# I want to open a file a single time but I'm not sure how to ensure we close it at the end
		self.file_log = open("replica_" + str(self.idnum) + ".log", "a")
		self.file_log.write(self.get_chat_log() + "\n")
		self.file_log.close()


	def get_chat_log (self):
		#chat_log_list = []
		#for i in range(0, self.last_executed_seq_number+1):
		#	if i in self.chat_log:
		#		chat_log_list.append(self.chat_log[i])
		#return ("Chat log for " + str(self.idnum) + ":\n\t" + '\n\t'.join(self.chat_log))
		return "\n\t" + '\n\t'.join(self.chat_log)
