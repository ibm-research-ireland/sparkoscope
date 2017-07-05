#!/usr/bin/env bash

sbin/stop-all.sh
./build/mvn  --projects core -Phadoop-2.7 -DskipTests install #not package
./build/mvn --projects assembly/ -Phadoop-2.7 -DskipTests install
sbin/start-all.sh
