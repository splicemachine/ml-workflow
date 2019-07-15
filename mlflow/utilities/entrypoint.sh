#!/usr/bin/env bash

# check environment vars
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

if [[ "$MLFLOW_PORT" == "" ]]; then
   echo "Error: environment variable MLFLOW_PORT is required"
   exit 1
fi

if [[ "$API_PORT" == "" ]]; then
   echo "Error: environment variable API_PORT is required"
   exit 1
fi

if [[ "$DASH_PORT" == "" ]]; then
   echo "Error: environment variable DASH_PORT is required"
   exit 1
fi


# do actual stuff
echo "Starting S3 Daemon"
mkdir -p /mlruns
mkdir -p /tmp/mlruns

cd /api/job_handler && nohup gunicorn --bind 0.0.0.0:$API_PORT --workers 4 app:app  &
nohup python /api/tracking/s3_sync.py upload -b $S3_BUCKET_NAME/persist -m /mlruns -i /tmp/mlruns -l 5 &

echo "Starting Job Tracker"
echo "Starting Dashboard UI"
cd /api/job_status && \
    nohup gunicorn --bind 0.0.0.0:$DASH_PORT --workers 4 dash:app > /tmp/dash.log &

echo "Starting Mlflow Server on 0.0.0.0"
cd /mlruns && nohup mlflow server --host 0.0.0.0 --default-artifact-root $S3_BUCKET_NAME/artifacts -p $MLFLOW_PORT & > /tmp/mlflow.log

python /api/utilities/keep_alive.py
