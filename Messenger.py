
from enum import Enum
import sys
from Util import printd

class MessageType(Enum):
	REQUEST = "1"
	I_AM_LEADER = "2"
	YOU_ARE_LEADER = "3"
	COMMAND = "4"
	ACCEPT = "5"

def send_header (socket, msg_size):
	#print "Sending header " + str(msg_size)
	msg = '%8s'%msg_size
	sent = socket.send(msg)
	if sent == 0:
		raise RuntimeError("Send header failed")


def recv_header (socket):
	msg_size = socket.recv(8)
	#print "Received " + str(msg_size)
	return int(msg_size)


# recv message on socket
# return message
def recv_message (socket):
	msg_size = recv_header(socket)
	chunk = socket.recv(msg_size)
	if chunk == '':
		raise RuntimeError("Receiving message failed")
	return chunk


# Send a single message
def send_message (socket, msg):
	send_header(socket, len(msg))
	sent = socket.send(msg)
	printd("socket {} sent msg: {} to: {}".format(socket.getsockname(),msg, socket.getpeername()))
	if sent == 0:
			raise RuntimeError("Send message failed")


def broadcast_message (sockets, msg):
	for socket in sockets:
		send_message(socket, msg)
