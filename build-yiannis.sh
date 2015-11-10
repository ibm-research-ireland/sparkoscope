#!/bin/bash
cd /home/johngouf/spark-viz/
./build/mvn clean -Phadoop-2.6 -Dhadoop.version=2.6.0 -DskipTests package
