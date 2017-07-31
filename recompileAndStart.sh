#!/usr/bin/env bash

sbin/stop-all.sh
sbin/stop-history-server.sh
./build/mvn  --projects core -Phadoop-2.7 -Pyarn -DskipTests install #not package
./build/mvn --projects assembly/ -Phadoop-2.7 -Pyarn -DskipTests install
sbin/start-all.sh
sbin/start-history-server.sh