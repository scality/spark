#docker run --rm -it --name spark-master --hostname spark-master \
#    -p 7077:7077 -p 8080:8080 spark /spark/bin/spark-class org.apache.spark.deploy.master.Master --ip `hostname` --port 7077 --webui-port 8080

docker run -d --rm -it  --net=host --name spark-worker --hostname spark-worker  --add-host spark-master:178.33.63.238 --add-host spark-worker:178.33.63.238  --add-host node1:178.33.63.238 spark-worker 
