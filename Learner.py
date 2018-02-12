
'''
	This class will be the final decider of values
'''
class Learner:

	def __init__ (self, majority_numb):
		self.majority_numb = majority_numb
		self.values_list = []

	def acceptValues (self, replicaID, seq_number, value):
		values_list[seq_number] += 1 # Increment the number of messages we've seen for this sequence number
		if values_list[seq_number] >= majority_numb:
			# Execute commnand
			printd("Execute command!")
		else:
			printd("Don't have majority for learner yet...")
