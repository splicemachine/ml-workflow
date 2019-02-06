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

if [[ "$FRAMEWORK_NAME" == "" ]]; then
   echo "Error: environment variable FRAMEWORK_NAME is required"
   exit 1
fi

if [[ "$SAGEMAKER_ROLE" == "" ]]; then
   echo "Error: environment variable SAGEMAKER_ROLE is required"
   exit 1
fi

echo "Creating Directories..."
mkdir -p /mlruns
mkdir -p /var/tmp/tmp_conf_files

echo "Starting up Docker Daemon"
nohup /bob/utilities/run_dind.sh &

#echo "Copying config from HMaster on DBaaS cluster"

#curl -kLs  "http://hmaster-0-node.${FRAMEWORK_NAME}.mesos:16010/logs/conf.tar.gz" | tar -xz -C \
#   /var/tmp/tmp_conf_files
#cp  /var/tmp/tmp_conf_files/conf/core-site.xml $SPARK_HOME/conf/
#cp  /var/tmp/tmp_conf_files/conf/fairscheduler.xml $SPARK_HOME/conf/
#cp  /var/tmp/tmp_conf_files/conf/hbase-site.xml $SPARK_HOME/conf/
#cp  /var/tmp/tmp_conf_files/conf/hdfs-site.xml $SPARK_HOME/conf/

echo "Cleaning Up conf files"
rm -r /var/tmp/tmp_conf_files

echo "Starting Worker"
cd /bob/service_jobs && nohup python worker.py &

python /bob/utilities/keep_alive.py
