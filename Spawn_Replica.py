
import argparse
from Util import Config
from Replica import Replica
from multiprocessing import Process, Semaphore
from Client import Client
import time
from Util import printd

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Initialize a replica for paxos based chat service")
    parser.add_argument('-f', '--tolerated_faults', help='Total number of tolerated faults')
    parser.add_argument('-i', '--rep_id', help='Unique ID of this replica')
    parser.add_argument('-r', '--replicas', help='The pairs of all replicas already in the system')
    parser.add_argument('-t', '--tests', help="Message loss and other test cases to perform")

    args = parser.parse_args()

    if args.tolerated_faults: # use config file
        printd("tolerated_faults = " + args.tolerated_faults)
        tolerated_faults = int(args.tolerated_faults)

    if args.rep_id:
        printd("rep_id = " + args.rep_id)
        rep_id = int(args.rep_id)



    if args.replicas:
    	config = Config(None, tolerated_faults, args.replicas, args.tests)
        rep_ip   =     config.server_pairs[rep_id][0]
        rep_port = int(config.server_pairs[rep_id][1])
    else: # if no replicas or given
        config = Config(None, tolerated_faults, None, args.tests)
        rep_ip = "127.0.0.1"
        rep_port = 4003

    printd("replicas = " + args.replicas)
    	#config = Config(None, tolerated_faults, "(127.0.0.1,4003),(127.0.0.1,4004),(127.0.0.1,4005),(127.0.0.1,4006),(127.0.0.1,4007), (127.0.0.1,4008),(127.0.0.1,4009),(127.0.0.1,4010),(127.0.0.1,4011),(127.0.0.1,4012)", args.test_cases)

    if args.tests: # tests format:    0.1;skip,4;kill,9;skip,22;.....
        printd("tests = " + args.tests)
        test_list_str = args.tests.split(';')
        try:
            p = float(test_list_str[0])
        except:
            raise RuntimeError("Error: Must specify a p value")
        test_list = []
        try:
            for i in test_list_str[1:]: # for each test case (excluding p value)
                test_str = i.split(',')
                test_list.append(  ( str(test_str[0]),int(test_str[1]) )  )
        except: # do not throw errors if no test cases specified. It's legal
            pass

    printd(config)

    total_replicas = 2 * int(config.tolerated_faults) + 1 # 2f + 1

    semaphore = Semaphore(0)
    rep_0 = None
    processes = []
    if rep_id == 0: # Create a single proposer with replica 0 as the primary
    	has_proposer = True
    else:
    	has_proposer = False

    replica = Replica(rep_id, rep_ip, rep_port, config.server_pairs, semaphore, test_list, proposer=has_proposer)


    replica.start_replica()
    ## replacing this with bash-level process spawning
    #rep_process = Process(target=replica.start_replica)
    #rep_process.start()


    # After starting all processes, we should wait for them all to connect to each other
    # before sending any messages
    #for i in range(0, len(config.server_pairs)):
    semaphore.acquire()
