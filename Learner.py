
from Util import printd

'''
	This class will be the final decider of values
'''
class Learner:

	list_size = 10


	def __init__ (self, majority_numb):
		self.majority_numb = majority_numb
		# I think this should be equal to 1 since we can count ourselves
		self.values_list = self.list_size * [1] 


	def acceptValue (self, replicaID, seq_number, value):
		int_seq_number = int(seq_number)
		self.values_list[int_seq_number] += 1 # Increment the number of messages we've seen for this sequence number
		printd("values list " + str(self.values_list[int_seq_number]) + " for seq number " + seq_number + " and majority is " + str(self.majority_numb))
		if self.values_list[int_seq_number] >= self.majority_numb:
			# Execute commnand
			printd("Execute command! values list " + str(self.values_list[int_seq_number]) + " for seq number " + seq_number + " and majority is " + str(self.majority_numb) )
		else:
			printd("Don't have majority for learner yet...")
