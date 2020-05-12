./bin/build
cp $ACCUMULO_HOME/conf/accumulo-client.properties ./conf
docker build --build-arg HADOOP_HOME=$HADOOP_HOME --build-arg HADOOP_USER_NAME=`whoami` --network host -t accumulo-testing .

#TODO set compaction threads
accumulo shell -u root -p secret -e "config -s tserver.compaction.major.concurrent.max=6"

while read host; do
  docker save accumulo-testing | ssh -C $host docker load &
done < $ACCUMULO_HOME/conf/tservers
