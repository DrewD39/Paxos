import time
import socket
import select
import Messenger
from Util import printd
from Messenger import MessageType

'''
    This class is responsible for sending chat messages to the replicas
'''

class Client:

    client_timeout = 5 # timeout for client response

    def __init__ (self, replica_list):
        # Each request should be identifiable by a client sequence number
        self.seq_number = 0
        self.replica_list = [x for x in replica_list]
        self.connection_sockets = []
        self.msg = None

    def connect_to_all_replicas (self):
        for replica in self.replica_list:
            self.connect_to_proposer(replica)


    def connect_to_proposer (self, replica):
        connected = False
        while not connected:
            try:
                # RIP an hour...
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # This is a bug if we move this out... see man connect for python
                s.connect((replica[0], int(replica[1]))) # Connect to (ip, port)
                connected = True
            except Exception as e:
                time.sleep(0) # yield thread

        #printd("Client connected to replica " + str(idnum))
        self.connection_sockets.append(s)


    def recv_message (self):

        while 1:
            rd, wd, ed = select.select(self.connection_sockets, [], [], self.client_timeout)

            if len(rd) == 0: # Haven't received a message in a while...
                print "Client timed-out and is resending " + str(self.msg)
                Messenger.broadcast_message(self.connection_sockets, self.msg)

            # Handle received messages
            for s in rd:
                return Messenger.recv_message(s)


    def send_message (self, value):
        self.msg = str(MessageType.REQUEST.value) + ":" + str(self.seq_number) + "," + value
        Messenger.broadcast_message(self.connection_sockets, self.msg)
        #Messenger.send_message(self.connection_socket, full_msg)
        printd("Client sent message to all replicas with value " + str(value))
        self.seq_number += 1
