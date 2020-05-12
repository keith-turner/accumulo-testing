#!/bin/bash

while true
do
	accumulo shell -u root -p secret -e 'scan -t accumulo.metadata -b 2; -e 2< -c file -np' 2>/dev/null | grep file: | gzip > dumps/dump.$(date +%Y%m%d%H%M%S).gz
	sleep 30
done
