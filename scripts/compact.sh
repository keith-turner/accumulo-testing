#!/bin/bash

while true
do
  sleep 10800
  echo "Starting compaction $(date)"
	accumulo shell -u root -p secret -e 'compact -t ci -w'
  echo "Finished compaction $(date)"
done
