
import socket
import Messenger
from Util import printd
from Messenger import MessageType

'''
    This class is responsible for sending chat messages to the replicas
'''

class Client:


    def __init__ (self, replica_list):
        # Each request should be identifiable by a client sequence number
        self.seq_number = 0
        self.replica_list = [x for x in replica_list]


    def connect_to_proposer (self, idnum):
        connected = False
        while not connected:
            try:
                # RIP an hour...
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # This is a bug if we move this out... see man connect for python
                s.connect((self.replica_list[idnum][0], int(self.replica_list[idnum][1]))) # Connect to (ip, port)
                connected = True
            except Exception as e:
                time.sleep(0) # yield thread

        printd("Client connected to replica " + str(idnum))
        self.connection_socket = s


    def recv_message (self):
        return Messenger.recv_message(self.connection_socket)


    def send_message (self, seq_number, value):
        full_msg = str(MessageType.REQUEST.value) + ":" + seq_number + "," + value
        Messenger.send_message(self.connection_socket, full_msg)
        printd("Client sent message with value " + str(value))
