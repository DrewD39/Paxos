#!/bin/bash

# inputs: f, ID, (IP,port) pairs of all replicas, test cases
# f, i, r are mandatory
# t requires atleast a value for p, followed by semi-colon (i.e. "0.05;")

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
  -f|--tolerated_faults)
  F="$2"
  shift # past argument
  shift # past value
  ;;
  -i|--rep_id)
  ID="$2"
  shift # past argument
  shift # past value
  ;;
  -r|--replicas)
  REPLICA_PAIRS="$2"
  shift # past argument
  shift # past value
  ;;
  -t|--tests)
  TESTS="$2"
  shift # past argument
  shift # past value
  ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

python2 Spawn_Replica.py -f ${F} -i ${ID} -r "${REPLICA_PAIRS}" -t "${TESTS}" &
