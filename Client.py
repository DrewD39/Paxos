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

    client_timeout = 3 # timeout for client response

    def __init__ (self, replica_list, client_name):
        # Each request should be identifiable by a client sequence number
        self.client_seq_number = 0
        self.replica_list = [x for x in replica_list]
        self.connection_sockets = []
        self.msg = None
        self.client_name = client_name
        self.prev_value = None


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
                header = '%8s' % len(self.client_name)
                s.send(header + self.client_name)
                connected = True
            except Exception as e:
                time.sleep(0) # yield thread

        #printd("Client connected to replica " + str(idnum))
        self.connection_sockets.append(s)


    def recv_message (self):

        while 1:
            rd, wd, ed = select.select(self.connection_sockets, [], [], self.client_timeout)

            if len(rd) == 0: # Haven't received a message in a while...
                printd("Client {} timed-out and is resending {}." .format(self.client_name, self.msg))
                Messenger.broadcast_message(self.connection_sockets, self.msg)

            # Handle received messages
            for s in rd:

                message = Messenger.recv_message(s)

                if message == self.prev_value: # Only should accept one message back
                    pass
                elif message is not '':
                    self.client_seq_number += 1 # move on to next client sequence number and next command
                    self.prev_value = message
                    self.msg = None
                    return message
                else: # We got a socket disconnection from one of our replicas which means its kaputs for good...
                    self.connection_sockets.remove(s)


    def send_message (self, value):
        self.msg = "{}:{},{},{}".format(MessageType.REQUEST.value, self.client_name, self.client_seq_number, value)
        Messenger.broadcast_message(self.connection_sockets, self.msg)
        #Messenger.send_message(self.connection_socket, full_msg)
        printd("Client {} sent message to all replicas with value {}".format(self.client_name,str(value)))


    def operate (self, num_messages=1, manual_messages=False, repeated_message=None, messages_file=None):
        printd("Client {} is operating".format(self.client_name))
        if manual_messages:
            msg = raw_input("What is Client {}'s msg? ".format(self.client_name))
        elif repeated_message != None:
            msg = repeated_message
        elif messages_file:
            pass

        #if self.client_name == 'A':
        #    time.sleep(10)

        for i in range(num_messages):
            self.send_message(str(i) + ":" + str(self.client_seq_number) + ":" + msg)
            recvd_msg = str(self.recv_message())
            printd("Client received message {}.".format(recvd_msg))
            #self.client_seq_number += 1 # move on to next client sequence number and next command
            #time.sleep(1)
