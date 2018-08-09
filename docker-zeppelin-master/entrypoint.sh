#!/usr/bin/env bash
bin/zeppelin-daemon.sh start
echo "Started Zeppelin Daemon"
echo "Starting S3 Daemon"
mkdir -p /mlruns
python /tmp/s3_sync.py upload -b s3://amrit-splice -m /mlruns -l 5
#python -c 'exec("import time\nwhile True:\n\ttime.sleep(1)")'
