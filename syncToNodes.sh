#!/usr/bin/env bash

rsync -azP /disk/spark-viz/ shamrock066:/disk/spark-viz
rsync -azP /disk/spark-viz/ shamrock067:/disk/spark-viz
rsync -azP /disk/spark-viz/ shamrock068:/disk/spark-viz
/disk/spark-viz/sbin/start-all.sh