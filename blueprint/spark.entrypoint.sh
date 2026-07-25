#!/bin/bash
set -e

envsubst < /opt/spark/conf/spark-defaults.conf.template > /opt/spark/conf/spark-defaults.conf

# Start master and worker in the background
./sbin/start-master.sh -p 7077
./sbin/start-worker.sh spark://spark-iceberg:7077
./sbin/start-thriftserver.sh --master spark://spark-iceberg:7077 --executor-cores 1 --executor-memory 1

if [[ $# -gt 0 ]] ; then
    eval "$@"
fi

# Keep the container alive
tail -f /dev/null