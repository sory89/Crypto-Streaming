#!/bin/bash
set -e

echo "==> Waiting for Kafka and Postgres to be ready..."
sleep 10

echo "==> Submitting Spark Streaming job..."

/opt/spark/bin/spark-submit \
  --master local[*] \
  --packages \
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.postgresql:postgresql:42.7.1 \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.streaming.stopGracefullyOnShutdown=true" \
  --conf "spark.executor.memory=1g" \
  --conf "spark.driver.memory=1g" \
  /opt/spark-app/spark_streaming.py
