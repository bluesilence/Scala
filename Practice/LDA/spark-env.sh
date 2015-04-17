#!/usr/bin/env bash

# This file contains environment variables required to run Spark. Copy it as
# spark-env.sh and edit that to configure Spark for your site.
#
# The following variables can be set in this file:
# - SPARK_LOCAL_IP, to set the IP address Spark binds to on this node
# - MESOS_NATIVE_LIBRARY, to point to your libmesos.so if you use Mesos
# - SPARK_JAVA_OPTS, to set node-specific JVM options for Spark. Note that
#export SPARK_JAVA_OPTS+=" -Dtachyon.user.file.writetype.default=MUST_CACHE"
#   we recommend setting app-wide options in the application's driver program.
#     Examples of node-specific options : -Dspark.local.dir, GC options
#     Examples of app-wide options : -Dspark.serializer
#
# If using the standalone deploy mode, you can also set variables for it here:
# - SPARK_MASTER_IP, to bind the master to a different IP address or hostname
# - SPARK_MASTER_PORT / SPARK_MASTER_WEBUI_PORT, to use non-default ports
# - SPARK_WORKER_CORES, to set the number of cores to use on this machine
# - SPARK_WORKER_MEMORY, to set how much memory to use (e.g. 1000m, 2g)
# - SPARK_WORKER_PORT / SPARK_WORKER_WEBUI_PORT
# - SPARK_WORKER_INSTANCES, to set the number of worker processes per node
# - SPARK_WORKER_DIR, to set the working directory of worker processes
export JAVA_HOME=/home/dm/software/jdk1.7.0_60
export SCALA_HOME=/home/dm/software/scala-2.10.4
export HADOOP_HOME=/home/dm/software/hadoop-2.4.0

export SPARK_HOME=/home/dm/software/spark-1.2
export SPARK_LOG_DIR=/home/dm/logs/spark
export SPARK_PID_DIR=/home/dm/tmp

#export SPARK_JAR=/home/dm/software/spark-coalesce/lib/spark-assembly-1.0.1-hadoop2.4.0.jar

#export MASTER=spark://192.168.1.120:7077
#export SPARK_MASTER_IP=srgssd-06
export SPARK_MASTER_IP=192.168.1.120
#export SPARK_MASTER_PORT=7087
#export SPARK_MASTER_WEBUI_PORT=8090
#export SPARK_WORKER_WEBUI_PORT=8091
#export SPARK_MASTER_IP=192.168.1.106

export SPARK_WORKER_MEMORY=90G
#export SPARK_MEM=80G
#export SPARK_JAVA_OPTS+="-Xmx70000m"
export SPARK_DAEMON_MEMORY=3G
export SPARK_WORKER_CORES=16
export SPARK_WORKER_DIR=/mnt/ssd/dm/data/spark/work

export HADOOP_CONF_DIR=/home/dm/software/hadoop-2.4.0/etc/hadoop
export YARN_CONF_DIR=/home/dm/software/hadoop-2.4.0/etc/hadoop

#export MASTER=yarn-cluster
export LOCAL_DIRS=/mnt/ssd/dm/tmp
export TEMP=/mnt/ssd/dm/tmp
export SPARK_LOCAL_DIRS=/home/dm/software/spark-1.2/rdd
export SPARK_CLASSPATH=/home/dm/software/tachyon-0.6-tangyun/client/target/tachyon-client-0.6.0-SNAPSHOT-jar-with-dependencies.jar:$SPARK_CLASSPATH

