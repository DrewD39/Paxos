

'''
	Each replica will have an acceptor and a learner service associated with it
	for implementing Paxos
'''
class Replica():

	idCounter = 0 # Increment id for each new replica

	def __init__ (self, ip, port, server_pairs, proposer=False):
		self.chat_log = [] # List of strings for each message
		self.ip = ip # IP address for this replica
		self.port = port # port for this replica
		self.other_servers = [x for x in server_pairs if x != (self.ip, self.port)] # List of tuples
		self.id = Replica.idCounter # id for each replica (from 0-2f)
		Replica.idCounter += 1
		# TODO: self.acceptor = Acceptor()
		# TODO: self.learner = Learner()
		if proposer:
			# TODO: self.proposer = Proposer()
			pass
		else:
			proposer = None # Only one proposer at a time

	def add_msg_to_chat_log (self, msg):
		self.chat_log.append(msg)
		# TODO: just for debugging, later remove this
		print_chat_log()

	def print_chat_log (self):
		print '\n'.join(self.chat_log)

