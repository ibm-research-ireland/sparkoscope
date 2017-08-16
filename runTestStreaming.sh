#!/usr/bin/env bash

./bin/spark-submit \
--master spark://localhost:7077 \
--class org.apache.spark.examples.streaming.NetworkWordCount \
dist/examples/jars/spark-examples*.jar \
localhost 9999
