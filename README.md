# SparkSampleProject

Start Spark Master
docker run \
    --name spark-master \
    -h spark-master \
    -e ENABLE_INIT_DAEMON=false \
    -d bde2020/spark-master:2.3.0-hadoop2.7

Start Spark Workers
docker run --name spark-worker-1 \
    --link spark-master:spark-master \
    -e ENABLE_INIT_DAEMON=false \
    -d bde2020/spark-worker:2.3.0-hadoop2.7
    
docker run --name spark-worker-2 \
    --link spark-master:spark-master \
    -e ENABLE_INIT_DAEMON=false \
    -d bde2020/spark-worker:2.3.0-hadoop2.7

docker run --name spark-worker-3 \
    --link spark-master:spark-master \
    -e ENABLE_INIT_DAEMON=false \
    -d bde2020/spark-worker:2.3.0-hadoop2.7