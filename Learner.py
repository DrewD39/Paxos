
from Util import printd
from Queue import PriorityQueue
import Messenger
from Messenger import MessageType

'''
	This class will be the final decider of values
'''
class Learner:


	def __init__ (self, majority_numb, idnum):
		self.chat_log = [] # List of strings for each message
		self.majority_numb = majority_numb
		# I think this should be equal to 1 since we can count ourselves in the majority
		self.seq_dict = dict() # This is a mapping of sequence number -> dictionary with key = value -> count
		self.commands_to_execute = PriorityQueue()
		self.last_executed_seq_number = - 1 # We haven't executed any commands yet
		self.idnum = idnum
		self.client_mapping = dict()
		self.connections_list = None


	def acceptValue (self, leaderNum, req_id, seq_number, value):
		seq = int(seq_number)

		if seq > self.last_executed_seq_number: # Else we should ignore
			if seq not in self.seq_dict.keys():
				self.seq_dict[seq] = dict()

			if value not in self.seq_dict[seq]:
				self.seq_dict[seq][value] = 1 # We've now seen one of these values
			else:
				self.seq_dict[seq][value] += 1 # Increment the number of messages we've seen for this sequence number and value

			if self.seq_dict[seq][value] == self.majority_numb:
				# Execute commnand
				#self.add_msg_to_chat_log(value)
				# We shouldn't execute again for this seq_number, and since we've already received
				# a majority, we're guaranteed to not receive a majority again
				#self.seq_dict[seq_number] = 0
				self.commands_to_execute.put((seq, value, req_id))
				self.try_to_execute_commands()
				printd(str(self.idnum) + " has majority for value " + str(value))
				del self.seq_dict[seq]
				return True
			else:
				printd("{} cannot execute for {},{} because we've only seen {} messages.".format(self.idnum, seq_number, value, self.seq_dict[seq][value]))
				#printd("Don't have majority for learner yet..., seq_number " + seq_number + " and values_list = "  + str(self.seq_dict[seq_number]))
				return False


	def try_to_execute_commands (self):

		for i in range(self.last_executed_seq_number + 1, int(self.commands_to_execute.queue[0][0])):
			printd("Replica {} could execute command {} but it's missing {}.".format(self.idnum, self.commands_to_execute.queue[0][0], self.last_executed_seq_number + 1))
			msg = "{}:{}".format(MessageType.CATCHUP.value, i)
			Messenger.broadcast_message (self.connections_list, msg)

		# Convoluted way to peek at PriorityQueue
		while not self.commands_to_execute.empty() and int(self.commands_to_execute.queue[0][0]) == self.last_executed_seq_number + 1:
			command = self.commands_to_execute.get()
			self.execute_command(command)


	def execute_command (self, command):
		seq_number = command[0]
		value = command[1]
		req_id = command[2]

		self.add_msg_to_chat_log(value)
		self.last_executed_seq_number = int(seq_number)

		printd(str(self.idnum) + " executes commands " + str(command))

		self.reply_to_client(req_id, value)


	# command succesfully executed
	def reply_to_client (self, req_id, value):
		client_name, client_seq_number = req_id.split('-')

		if client_name in self.client_mapping: # This client name must be in the client mapping
			clientsock = self.client_mapping[client_name]
			printd("Responding to client {} with client_seq_number {}.".format(client_name, client_seq_number))
			Messenger.send_message(clientsock, value)
		else:
			raise RuntimeError("This client name: {}, is not in our mapping for replica {}.".format(client_name, self.idnum))


	def add_client (self, clientname, clientsock):
		self.client_mapping[clientname] = clientsock


	def send_value_at_seq_number (self, missing_seq_number):
		if len(self.chat_log) > missing_seq_number:
			printd("Replica {} is sending value for sequence number {}.".format(self.idnum, missing_seq_number))
			msg = "{}:{},{}".format(MessageType.MISSING_VALUE, missing_seq_number, self.chat_log[missing_seq_number])
			Messenger.broadcast_message(self.connections_list, msg)
		else:
			printd("Replica {} is also behind sequence number {}.".format(self.idnum, missing_seq_number))


	def fill_missing_value (self, missing_seq_number, missing_value):
		self.chat_log[missing_seq_number] = missing_value
		self.last_executed_seq_number = missing_seq_number + 1

		self.try_to_execute_commands() # Now try to process commands again


	def set_socket_list (self, connections_list):
		self.connections_list = connections_list


	def add_msg_to_chat_log (self, msg):
		self.chat_log.append(msg)
		# TODO: just for debugging, later remove this
		print self.get_chat_log()

		# I want to open a file a single time but I'm not sure how to ensure we close it at the end
		self.file_log = open("replica_" + str(self.idnum) + ".log", "a")
		self.file_log.write(self.get_chat_log() + "\n")
		self.file_log.close()


	def get_chat_log (self):
		return ("Chat log for " + str(self.idnum) + ":\n\t" + '\n\t'.join(self.chat_log))
