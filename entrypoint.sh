#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
    start-master.sh \
    --ip spark-master \
    --port 7077 \
    --webui-port 8088
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
    start-worker.sh --webui-port 8088 spark://spark-master:7077
elif [ "$SPARK_WORKLOAD" == "history" ]
then
    start-history-server.sh
fi