
from enum import Enum
import sys
from Util import printd
import random
from socket import error as SocketError


class MessageType(Enum):
	REQUEST = "1"
	I_AM_LEADER = "2"
	YOU_ARE_LEADER = "3"
	COMMAND = "4"
	ACCEPT = "5"
	NACK = "6"
	CATCHUP = "7"
	MISSING_VALUE = "8"
	HEARTBEAT = "9"

def should_drop_message ():
	threshold = .10
	p = random.uniform(0, 1)
	return p < threshold


def send_promise (socket, promise):
	send_message (socket, str(promise))


def recv_header (aSocket):
	msg_size = aSocket.recv(8)
	#print "Received " + str(msg_size)
	return int(msg_size)


# recv message on socket
# return message
def recv_message (aSocket):
	try:
		msg_size = recv_header(aSocket)
		chunk = aSocket.recv(msg_size)

		if chunk == '':
			raise RuntimeError("Receiving message failed")
		return chunk
	except (SocketError, ValueError) as e:
		printd("Socket error in receive but should be no problem if you're killing a process")
		return ''


# Send a single message
def send_message (aSocket, msg):
	try:
		if not should_drop_message():
			header = '%8s' % len(msg)
			#send_header(socket, len(msg))
			sent = aSocket.send(header + msg)
			#printd("socket {} sent msg: {} to: {}".format(socket.getsockname(),msg, socket.getpeername()))
			if sent == 0:
				raise RuntimeError("Send message failed")
		else:
			printd("Dropped message ".upper() + str(msg))
	except SocketError as e:
		printd("Socket error in send but should be no problem if you're killing a process")
		return ''


def broadcast_message (sockets, msg):
	for aSocket in sockets:
		send_message(aSocket, msg)
