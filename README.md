# Paxos Messaging System
Authors: Drew Davis, Matt Furlong

**What is Paxos?**  
https://en.wikipedia.org/wiki/Paxos


**Basic Usage**  
This is a program that simulates a messaging app where consensus and fault tolerance is achieved through Paxos. The system is always safe and live when synchronous. The hash of the replica execution logs will automatically be output periodically (every 49 internal steps) to show safeness across replicas.

Clients automatically generate 'sentences' to message to the system using a corpus of the 1000 most common English words (words.txt).

Interaction with the Paxos program will primarily be handled through shell scripts. There are two primary modes of operation: Script Mode and Manual Mode.



* Script Mode (Script_mode.sh)
  * In script mode, a single script spawns 2f+1 replicas, given an input configuration file. The configuration file should be passed on the command line as an argument (ex: ./Script_mode.sh config.txt) A sample input file is provided (config.txt). This configuration file allows for both replicas and clients to be spawned. Example lines of spawning each are shown below. Both take different input arguments. It is the user's responsibility to ensure this file is set up properly and provides legal arguments. The system will not operate until all expected replicas are spawned. All arguments are required.
    * replica arguments = number of tolerated faults|replica id number|(IP, port) pairs of all replicas|test cases
      number of tolerated faults: Faults (f) this Paxos setup can tolerate (system requires 2f+1 replicas)
      replica id number: The id of this specific replica (from 0 to 2f+1)
      (IP, port) pairs of all replicas: The addressing information for every other replica that has been or will be spawned in this system
      test cases: Set of tests to manually trigger on the system. The prepending float = p (the % of messages that are dropped). Each pair following p represents (test type, sequence number to test)
      ex) replica|2|0|(127.0.0.1,4003),(127.0.0.1,4004),(127.0.0.1,4005),(127.0.0.1,4006),(127.0.0.1,4007)|0.1;skip,4;kill,80
    * client arguments  = client name|(IP, port) pairs of all replicas|number of messages to send
      client name: String name of the client
      (IP, port) pairs of all replicas: The addressing information for every other replica that has been or will be spawned in this system
      number of messages to send: Client will automatically generate messages. This determines how many messages before it quits
      ex) client|drew|(127.0.0.1,4003),(127.0.0.1,4004),(127.0.0.1,4005),(127.0.0.1,4006),(127.0.0.1,4007)|200

* Manual Mode (Manual_mode.sh)
  * Manual mode allows for replicas to be spawned individually and without a full system in place. The Manual_mode.sh script requires arguments to be passed in on the command line. An example command line call is shown below.
  -f, --tolerated_faults: number of faults tolerated
  -i,--red_id: ID of the replica being spawned
  -r, --replicas: The addressing information for every other replica that could be spawned for a potential system with this replica
  -t, --tests: Set of tests to manually trigger on the system. The prepending float = p (the % of messages that are dropped). Each pair following p represents (test type, sequence number to test)
  ex) ./Manual_mode.sh -f "2" -i "0" -t "0.1;kill,4" -r "(127.0.0.1,4003),(127.0.0.1,4004),(127.0.0.1,4005),(127.0.0.1,4006),(127.0.0.1,4007)"





**How to run provided required scenarios:**

1. Use script mode. When providing the test cases, only provide a p value (followed by semicolon), no other test cases.
  example replica line in config file: replica|2|0|(127.0.0.1,4003),(127.0.0.1,4004),(127.0.0.1,4005),(127.0.0.1,4006),(127.0.0.1,4007)|0.1;

2. Use script mode. After the p value in your test case parameter inside the configuration file, include a kill test case, which is the word kill followed by a comma and a sequence number to force the kill at. example replica line in config file: replica|2|0|(127.0.0.1,4003),(127.0.0.1,4004),(127.0.0.1,4005),(127.0.0.1,4006),(127.0.0.1,4007)|0.1;kill,80

3. Use script mode. Same as 2, but with an additional kill test case (up to f kills). example replica line in config file: replica|2|0|(127.0.0.1,4003),(127.0.0.1,4004),(127.0.0.1,4005),(127.0.0.1,4006),(127.0.0.1,4007)|0.1;kill,80;kill,130

4. Use script mode. Skips can be forced in the same way as kills in 2. Except now use the skip keyword in place of kill inside the config file. example replica line in config file: replica|2|0|(127.0.0.1,4003),(127.0.0.1,4004),(127.0.0.1,4005),(127.0.0.1,4006),(127.0.0.1,4007)|0.1;skip,80

5. use script mode. As discussed earlier, p is set as the first float value passed in with the test cases. In the example below, p = 10%. If you want p to be 0, simply set the float as 0. example line from config file:  replica|2|0|(127.0.0.1,4003),(127.0.0.1,4004),(127.0.0.1,4005),(127.0.0.1,4006),(127.0.0.1,4007)|0.1;
