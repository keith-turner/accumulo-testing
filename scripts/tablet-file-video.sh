#!/bin/bash

counter=0

for f in dumps/dump.[0-9]*; do
	(echo $f; echo; zcat $f | grep '3<') | convert -size 850x330 xc:black -font /usr/share/fonts/truetype/noto/NotoMono-Regular.ttf -pointsize 14 -fill white -annotate +25+25 "@-"  frames/$(printf '%05d.png' $counter) &
	let counter=counter+1
	if [ $(($counter%20)) == 0 ]; then
		wait
	fi
done

wait

for i in {0..60}; do
	echo "That's all folks." |  convert -size 850x330 xc:black -font /usr/share/fonts/truetype/noto/NotoMono-Regular.ttf -pointsize 24 -fill white -annotate +25+25 "@-"  frames/$(printf '%05d.png' $counter)
	let counter=counter+1
done

cd frames
ffmpeg -framerate 30 -i '%05d.png' tablet-files.gif
