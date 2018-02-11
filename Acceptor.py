
from enum import Enum
import Messenger
from Messenger import MessageType
from Util import printd
'''

'''
class Acceptor:

	def __init__ (self):
		self.selected_leader = None # Integer value for selected leader
		self.accepted_value = None # Last seen value that was proposed

	def acceptLeader (self, newLeaderID, socket):
		outMsg = str(self.selected_leader) + "," + str(self.accepted_value)
		full_msg = MessageType.YOU_ARE_LEADER.value + ":" + outMsg
		printd("msg sent by acceptLeader: " + full_msg)
		self.selected_leader = newLeaderID
		printd("Replica accepts leader #{}".format(newLeaderID))
		Messenger.send_message (socket, full_msg)
