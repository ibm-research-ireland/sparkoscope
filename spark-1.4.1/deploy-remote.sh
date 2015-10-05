#!/usr/bin/env bash

rsync -azP /home/johngouf/TPC-H/spark-instrumentation/spark-1.4.1/ johngouf@9.162.50.21:/home/johngouf/spark-viz
ssh johngouf@9.162.50.21 '/home/johngouf/spark-viz/build-yiannis.sh'