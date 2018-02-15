
from Util import printd

'''
	This class will be the final decider of values
'''
class Learner:


	seq_list_size = 100


	def __init__ (self, majority_numb, idnum):
		self.majority_numb = majority_numb
		# I think this should be equal to 1 since we can count ourselves in the majority
		self.seq_dict = dict()
		self.idnum = idnum


	def acceptValue (self, leaderNum, seq_number, value):
		if seq_number not in self.seq_dict:
			self.seq_dict[seq_number] = 1

		self.seq_dict[seq_number] += 1 # Increment the number of messages we've seen for this sequence number

		if self.seq_dict[seq_number] >= self.majority_numb:
			# Execute commnand
			printd(str(self.idnum) + " executes commands for seq number " + str(seq_number))
			# We shouldn't execute again for this seq_number, and since we've already received
			# a majority, we're guaranteed to not receive a majority again
			self.seq_dict[seq_number] = 0
			return True
		else:
			printd("Don't have majority for learner yet..., seq_number = " + seq_number + " and valeus_list = "  + str(self.seq_dict[seq_number]))
			return False
