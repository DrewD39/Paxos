#!/bin/bash
rm *.log -f
while IFS='' read -r line || [[ -n "$line" ]]; do
    #echo "Text read from file: $line"
    IFS='|' read -ra ADDR <<< "$line"
    #for i in "${ADDR[@]}"; do
    #echo "array at ind 3 = ${ADDR[3]}"
    if [ "${ADDR[0]}" == "replica" ]; then
      python2 Spawn_Replica.py -f ${ADDR[1]} -i ${ADDR[2]} -r "${ADDR[3]}" -t "${ADDR[4]}" &
      echo "------ replica ${ADDR[2]} started"
    fi
    if [ "${ADDR[0]}" == "client" ]; then
      sleep 0.5
      python2 Spawn_Client.py -i ${ADDR[1]} -r ${ADDR[2]} -n ${ADDR[3]} &
      echo "------ client ${ADDR[1]} started"
    fi
    #done
done < "$1"
