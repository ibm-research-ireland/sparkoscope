#!/usr/bin/env bash

./bin/spark-submit --class org.apache.spark.examples.SparkPi \
    --master yarn \
    --deploy-mode cluster \
    --executor-cores 1 \
    --num-executors 1 \
    --executor-memory 500m \
    --driver-memory 500m \
    dist/examples/jars/spark-examples*.jar \
    10