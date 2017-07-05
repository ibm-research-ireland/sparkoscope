#!/usr/bin/env bash

bin/spark-submit --class org.apache.spark.examples.SparkPi --master spark://localhost:7077 dist/examples/jars/spark-examples_2.11-2.1.0.jar 10000
