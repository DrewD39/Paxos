
from Util import printd

'''
	This class will be the final decider of values
'''
class Learner:

	list_size = 10

	def __init__ (self, majority_numb):
		self.majority_numb = majority_numb
		self.values_list = self.list_size * [0]

	def acceptValue (self, replicaID, seq_number, value):
		int_seq_number = int(seq_number)
		self.values_list[int_seq_number] += 1 # Increment the number of messages we've seen for this sequence number
		if self.values_list[int_seq_number] >= self.majority_numb:
			# Execute commnand
			printd("Execute command!")
		else:
			printd("Don't have majority for learner yet...")
