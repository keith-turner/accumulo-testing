./bin/build
cp $ACCUMULO_HOME/conf/accumulo-client.properties ./conf
docker build --build-arg HADOOP_HOME=$HADOOP_HOME --build-arg HADOOP_USER_NAME=`whoami` --network host -t accumulo-testing .

#TODO set compaction threads
accumulo shell -u root -p secret -e "config -s tserver.compaction.major.concurrent.max=6"


config -s 'tserver.compaction.service.user.planner=org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner'
config -s 'tserver.compaction.service.user.planner.opts.executors=[{"name":"all","numThreads":2}]'
config -s 'tserver.compaction.service.user.planner.opts.maxOpen=30'
config -s 'tserver.compaction.service.default.planner=org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner'
config -s 'tserver.compaction.service.default.planner.opts.executors=[{"name":"small","maxSize":"32M","numThreads":2},{"name":"large","numThreads":2}]'
config -s 'tserver.compaction.service.default.planner.opts.maxOpen=10'

while read host; do
  docker save accumulo-testing | ssh -C $host docker load &
done < $ACCUMULO_HOME/conf/tservers


docker service create --network="host" --replicas 10 --name ci --restart-condition none  accumulo-testing cingest ingest -o test.ci.ingest.client.entries=6000000000
