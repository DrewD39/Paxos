
import argparse
from Util import Config
from Replica import Replica
from multiprocessing import Process, Semaphore
from Client import Client
# from chatterbot import ChatBot for later
import time

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="Run a Paxos based chat service")
	parser.add_argument('-i', '--input_file', help='Config file for input')
	parser.add_argument('-f', '--tolerated_faults', help='Total number of tolerated faults')
	parser.add_argument('-p', '--parameters', help='Testing parameters (eg skip slot, message loss, etc.')
	parser.add_argument('-s', '--server_pairs', help="Server pairs in format (ip,port)")

	args = parser.parse_args()

	if args.input_file: # use config file
		config = Config(args.input_file)
	elif args.tolerated_faults and args.server_pairs and args.parameters: # from CL
		config = Config(None, args.tolerated_faults, args.server_pairs, args.parameters)
	else: # just use default values
		config = Config(None, 1, "(127.0.0.1,4003),(127.0.0.1,4004),(127.0.0.1,4005)", None)

	print config

	# Set up fun chat bot TODO: for later
	'''chatbot = ChatBot(
    	'Ron Obvious',
    	trainer='chatterbot.trainers.ChatterBotCorpusTrainer'
	)

	# Train based on the english corpus
	chatbot.train("chatterbot.corpus.english")'''

	total_processes = 2 * int(config.tolerated_faults) + 1 # 2f + 1

	semaphore = Semaphore(0)
	rep_0 = None
	processes = []
	for idnum, pair in enumerate(config.server_pairs):
		if idnum == 0: # Create a single proposer with replica 0 as the primary
			has_proposer = True
		else:
			has_proposer = False

		replica = Replica(idnum, pair[0], pair[1], config.server_pairs, semaphore, proposer=has_proposer)

		processes.append(Process(target=replica.start_replica))
		processes[idnum].start()

	# After starting all processes, we should wait for them all to connect to each other
	# before sending any messages
	for i in range(0, len(config.server_pairs)):
		semaphore.acquire()

	# And now we should run the client
	client = Client (config.server_pairs)
	client.connect_to_all_replicas() # Connect to all replicas even if they don't have a proposer yet

	msg = "Hello how are you today?"
	for i in range(0, 10):
		#msg = raw_input("What is your msg? ")
		client.send_message(str(i) + ":" + msg)
		#if i == 5:
		#	time.sleep(6)
		recvd_msg = str(client.recv_message())
		#msg = str(chatbot.get_response(recvd_msg)) For later....
