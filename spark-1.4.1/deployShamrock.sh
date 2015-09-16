#!/usr/bin/env bash

ssh shamrock064 '/disk/spark-viz/sbin/stop-all.sh'
/home/johngouf/spark-viz/build-yiannis.sh
find /home/johngouf/spark-viz/conf -type f -name '*.bak' | while read f; do mv "$f" "${f%.bak}"; done
rsync -azP /home/johngouf/spark-viz/ shamrock064:/disk/spark-viz
rsync -azP /home/johngouf/spark-viz/ shamrock066:/disk/spark-viz
rsync -azP /home/johngouf/spark-viz/ shamrock067:/disk/spark-viz
rsync -azP /home/johngouf/spark-viz/ shamrock068:/disk/spark-viz
ssh shamrock064 '/disk/spark-viz/sbin/start-all.sh'