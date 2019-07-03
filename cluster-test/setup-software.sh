ACCUMULO_GIT_REPO=https://github.com/apache/accumulo.git
ACCUMULO_GIT_BRANCH=2.0

TEST_GIT_REPO=https://github.com/apache/accumulo-testing.git
TEST_GIT_BRANCH=master

cd ~

mkdir -p git
cd ~/git

git clone $ACCUMULO_GIT_REPO
cd accumulo
git checkout $ACCUMULO_GIT_BRANCH
mvn install -PskipQA

cd ~/git
git clone $TEST_GIT_REPO
cd accumulo-testing
git checkout $TEST_GIT_BRANCH

# TODO need to 
#  * cp conf/accumulo-testing.properties.example conf/accumulo-testing.properties
#  * edit conf/accumulo-testing.properties and set test.common.hdfs.root and test.common.yarn.resource.manager

./bin/build

cp $ACCUMULO_HOME/conf/accumulo-client.properties ./conf

docker build --build-arg HADOOP_HOME=$HADOOP_HOME --build-arg HADOOP_USER_NAME=`whoami` -t accumulo-testing .

# copy docker image to other nodes
for i in {0..9} ; do
  docker save accumulo-testing | ssh -C "worker$i" docker load &
done
