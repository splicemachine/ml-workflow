#!/usr/bin/env bash
# test required inputs

if [[ "$JDBC_URL" == "" ]]; then
   echo "Error: environment variable JDBC_URL is required"
   exit 1
fi

if [[ "$USER" == "" ]]; then
   echo "Error: environment variable USER is required"
   exit 1
fi

if [[ "$PASSWORD" == "" ]]; then
   echo "Error: environment variable PASSWORD is required"
   exit 1
fi

if [[ "$S3_BUCKET_NAME" == "" ]]; then
   echo "Error: environment variable S3_BUCKET_NAME is required"
   exit 1
fi

if [[ "$DASH_PORT" == "" ]]; then
   echo "Error: environment variable DASH_PORT is required"
   exit 1
fi
if [[ "$FRAMEWORK_NAME" == "" ]]; then
   echo "Error: environment variable FRAMEWORK_NAME is required"
   exit 1
fi


mkdir -p /mlruns
echo "Starting up Docker Daemon"
nohup ./bob/utilities/run_dind.sh &

echo "Copying config from HRegionServer"
curl -kLs  "http://hmaster-0-node.${FRAMEWORK_NAME}.mesos:16010/logs/conf.tar.gz" | tar -xz -C \
    /var/tmp/tmp_conf_files
cp  /var/tmp/tmp_conf_files/conf/core-site.xml $SPARK_HOME/conf/
cp  /var/tmp/tmp_conf_files/conf/fairscheduler.xml $SPARK_HOME/conf/
cp  /var/tmp/tmp_conf_files/conf/hbase-site.xml $SPARK_HOME/conf/
cp  /var/tmp/tmp_conf_files/conf/hdfs-site.xml $SPARK_HOME/conf/


rm -r /var/tmp/tmp_conf_files


echo "Starting Dashboard UI"
cd /bob/job_status && \
    nohup gunicorn --bind 0.0.0.0:$DASH_PORT --workers 4 dash:app > /tmp/dash.log &

echo "Starting Worker"
cd /bob/service_jobs && python worker.py
