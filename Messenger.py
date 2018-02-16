
from enum import Enum
import sys
from Util import printd

class MessageType(Enum):
	REQUEST = "1"
	I_AM_LEADER = "2"
	YOU_ARE_LEADER = "3"
	COMMAND = "4"
	ACCEPT = "5"
	NACK = "6"


def send_promise (socket, promise):
	send_message (socket, str(promise))


def send_header (aSocket, msg_size):
	#print "Sending header " + str(msg_size)
	msg = '%8s'%msg_size
	sent = aSocket.send(msg)
	if sent == 0:
		raise RuntimeError("Send header failed")


def recv_header (aSocket):
	msg_size = aSocket.recv(8)
	#print "Received " + str(msg_size)
	return int(msg_size)


# recv message on socket
# return message
def recv_message (aSocket):
	msg_size = recv_header(aSocket)
	chunk = aSocket.recv(msg_size)
	if chunk == '':
		raise RuntimeError("Receiving message failed")
	return chunk


# Send a single message
def send_message (aSocket, msg):
	header = '%8s' % len(msg)
	#send_header(socket, len(msg))
	sent = aSocket.send(header + msg)
	#printd("socket {} sent msg: {} to: {}".format(socket.getsockname(),msg, socket.getpeername()))
	if sent == 0:
			raise RuntimeError("Send message failed")


def broadcast_message (sockets, msg):
	for aSocket in sockets:
		send_message(aSocket, msg)
