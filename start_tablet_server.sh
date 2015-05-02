#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
export HADOOP_INSTALL=/usr/local/hadoop
export PATH=$PATH:$HADOOP_INSTALL/bin
export PATH=$PATH:$HADOOP_INSTALL/sbin
export HADOOP_MAPRED_HOME=$HADOOP_INSTALL
export HADOOP_COMMON_HOME=$HADOOP_INSTALL
export HADOOP_HDFS_HOME=$HADOOP_INSTALL
export YARN_HOME=$HADOOP_INSTALL
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_INSTALL/lib/native
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_INSTALL/lib/native"
export HBASE_HOME=/usr/local/hbase
#HADOOP VARIABLES END
export PATH=$PATH:$HBASE_HOME/bin
. classpath.sh
export CLASSPATH=${CLASSPATH}:`ls /usr/local/hbase/lib/*.jar | tr '\n' ':'`
export CLASSPATH=${CLASSPATH}/var/lib/mongodb/jar/mongo-java-driver-2.13.1.jar


env | grep CLASSPATH

cd ~/dspeyer-scratch/spatialtable

nohup ./tabletserver 5555 > log/tabletServers.${HOSTNAME}.out 2>log/tabletServers.${HOSTNAME}.err &
