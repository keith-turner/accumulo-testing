#!/bin/bash

for f in dumps/dump.[0-9]*; do
	clear
	echo $f
	echo
	zcat $f | grep '3<'
	sleep .1
done
