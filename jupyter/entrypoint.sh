#!/usr/bin/env bash
bin/zeppelin-daemon.sh start
echo "Started Zeppelin Daemon"
mkdir -p /mlruns
python -c 'exec("import time\nwhile True:\n\ttime.sleep(1)")' # keep jupyter alive
