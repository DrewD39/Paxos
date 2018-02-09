
from enum import Enum 

class MessageType(Enum):
	REQUEST = "1"
	I_AM_LEADER = "2"
	YOU_ARE_LEADER = "3"
	COMMAND = "4"
	ACCEPT = "5"

MSGLEN = len("1: hello mate")

# recv message on socket
# return message
def recv_message (socket):
	chunks = []
	bytes_recd = 0
	while bytes_recd < MSGLEN:
		chunk = socket.recv(min(MSGLEN - bytes_recd, 2048))
		if chunk == '':
			raise RuntimeError("socket connection broken")
		chunks.append(chunk)
		bytes_recd = bytes_recd + len(chunk)
	return ''.join(chunks)


# Send a single message
def send_message (socket, msg):
	totalsent = 0
	while totalsent < len(msg):
		sent = socket.send(msg[totalsent:])
		if sent == 0:
			raise RuntimeError("socket connection broken")
		totalsent = totalsent + sent


def broadcast_message (sockets, msg):
	for socket in sockets:
		send_message(socket, msg)
