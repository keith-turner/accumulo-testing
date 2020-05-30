#!/bin/bash

for f in dumps/dump.[0-9]*; do
	echo -e -n "$f\t"
	zcat $f |  datamash -g 1 -W count 2 | datamash -W mean 2 sstdev 2 min 2 max 2
done
