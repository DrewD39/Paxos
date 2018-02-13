
from Util import printd

'''
	This class will be the final decider of values
'''
class Learner:


	seq_list_size = 10


	def __init__ (self, majority_numb):
		self.majority_numb = majority_numb
		# I think this should be equal to 1 since we can count ourselves in the majority
		self.values_list = self.seq_list_size * [1]


	def acceptValue (self, replicaID, seq_number, value):
		int_seq_number = int(seq_number)
		self.values_list[int_seq_number] += 1 # Increment the number of messages we've seen for this sequence number
		if self.values_list[int_seq_number] >= self.majority_numb:
			# Execute commnand
			printd(str(replicaID) + " executes commands for seq number " + str(seq_number))
			# We shouldn't execute again for this seq_number, and since we've already received
			# a majority, we're guaranteed to not receive a majority again
			self.values_list[int_seq_number] = 0
			return True
		else:
			return False
			printd("Don't have majority for learner yet...")
