
import argparse
from Util import Config
from Replica import Replica
from multiprocessing import Process

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
	total_processes = 2 * int(config.tolerated_faults) + 1 # 2f + 1

	for i, pair in enumerate(config.server_pairs):
		p = Process(target=Replica, args=(i, pair[0], pair[1], config.server_pairs, True,))
		p.start()

	#p.join()

	for idnum, pair in enumerate(config.server_pairs):
		if idnum == 0: # Create a single proposer with replica 0 as the primary
			replica = Replica(idnum, pair[0], pair[1], config.server_pairs, proposer=True)
		else:
			replica = Replica(idnum, pair[0], pair[1], config.server_pairs, proposer=False)

		p = Process(target=replica.start_replica)
		p.start()
