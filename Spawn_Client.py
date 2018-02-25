
import time
import argparse
from Util import Config
from Client import Client
from multiprocessing import Process, Semaphore
from Util import printd
#import os


if __name__ == "__main__":

    #time.sleep(2)

    parser = argparse.ArgumentParser(description="Initialize a client for paxos based chat service")
    parser.add_argument('-i', '--client_name', help='Unique name of this client')
    parser.add_argument('-m', '--mode', help='Select mode between "manual" and "auto"')
    parser.add_argument('-n', '--number_messages', help='Number of messages for this client to send')
    parser.add_argument('-r', '--replicas', help='The pairs of all replicas already in the system')

    args = parser.parse_args()

    if args.mode == 'manual':
        man_mode = True
    else: # default or automatic mode
        man_mode = False

    if args.number_messages: # use config file
        num_messages = int(args.number_messages)
    else: # default number of messages is 50
        num_messages = 50

    if args.replicas:
    	config = Config(None, 1, args.replicas, None)
    else:
        raise RuntimeError("No replicas provided to the client")


    printd("Client server pairs:"); printd(config.server_pairs)
    printd("client name: " + args.client_name)
    client1 = Client (config.server_pairs, args.client_name)
    client1.connect_to_all_replicas() # Connect to all replicas even if they don't have a proposer yet

    time.sleep(2)

    client1.operate(num_messages, man_mode)

    # once a client has sent all of their messages, they should shut themselves down
    #os.system("kill -9 %d"%(os.getppid()))
