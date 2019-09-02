#!/bin/sh
/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --ip spark-master \
    --port 7077 \
    --webui-port 8080
