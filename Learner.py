
from Util import printd
from queue import PriorityQueue

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


	def acceptValue (self, leaderNum, seq_number, value):

		if seq_number not in self.seq_dict:
			self.seq_dict[seq_number] = dict()

		if value not in self.seq_dict[seq_number]:
			self.seq_dict[seq_number][value] = 1 # We've now seen one of these values
		else:
			self.seq_dict[seq_number][value] += 1 # Increment the number of messages we've seen for this sequence number and value

		if self.seq_dict[seq_number][value] == self.majority_numb:
			# Execute commnand
			printd(str(self.idnum) + " executes commands " + str(value) + " for seq number " + str(seq_number))
			#self.add_msg_to_chat_log(value)
			# We shouldn't execute again for this seq_number, and since we've already received
			# a majority, we're guaranteed to not receive a majority again
			#self.seq_dict[seq_number] = 0
			self.commands_to_execute.put((seq_number, value))
			self.try_to_execute_command()
			del self.seq_dict[seq_number]
			return True
		else:
			printd("{} cannot execute for {},{} because we've only seen {} messages.".format(self.idnum, seq_number, value, self.seq_dict[seq_number][value]))
			#printd("Don't have majority for learner yet..., seq_number " + seq_number + " and values_list = "  + str(self.seq_dict[seq_number]))
			return False


	def try_to_execute_command (self):
		# Convoluted way to peek at PriorityQueue
		earliest_seq_number = int(self.commands_to_execute.queue[0][0])

		if earliest_seq_number == self.last_executed_seq_number + 1:
			command = self.commands_to_execute.get()
			self.execute_command(command)
		else:
			printd("Couldn't execute command for sequence number {} because the max sequence number is {}".format(earliest_seq_number, self.last_executed_seq_number))


	def execute_command (self, command):
		seq_number = command[0]
		value = command[1]

		self.add_msg_to_chat_log(value)
		self.last_executed_seq_number = int(seq_number)


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
