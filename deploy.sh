#!/usr/bin/env bash

cd /home/johngouf/TPC-H/spark-instrumentation/
mvn clean
rsync -azP /home/johngouf/TPC-H/spark-instrumentation/ johngouf@9.162.50.21:/home/johngouf/spark-viz
ssh johngouf@9.162.50.21 '/home/johngouf/spark-viz/deployShamrock.sh'