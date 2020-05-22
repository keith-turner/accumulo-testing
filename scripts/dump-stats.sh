
#!/bin/bash

for f in dumps/dump.[0-9]*; do
  echo -n "$f "
  zcat $f | grep file: | datamash -g 1 -W count 2 | datamash -W mean 2
done

