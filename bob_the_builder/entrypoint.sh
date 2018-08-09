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

# do actual stuff
echo "Starting up Docker Daemon"
mkdir -p /mlruns
nohup dockerd-entrypoint.sh > /tmp/docker.log &
echo "Starting Dashboard UI"
cd /bob/job_status && \
    nohup gunicorn --bind 0.0.0.0:$DASH_PORT --workers 4 dash:app > /tmp/dash.log &

echo "Starting Worker"
cd /bob && python worker.py
