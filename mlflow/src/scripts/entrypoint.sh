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

#!/usr/bin/env bash

# Test Required Variables (data validation)

if [[ "$DB_HOST" == "" ]]
then
   echo "Error: environment variable DB_HOST is required"
   exit 1
fi

if [[ "$DB_USER" == "" ]]
then
   echo "Error: environment variable DB_USER is required"
   exit 1
fi

if [[ "$DB_PASSWORD" == "" ]]
then
   echo "Error: environment variable DB_PASSWORD is required"
   exit 1
fi

if [[ "$S3_BUCKET_NAME" == "" ]]
then
   echo "Error: environment variable S#_BUCKET_NAME is required"
   exit 1
fi

if [[ "$SAGEMAKER_ROLE" == "" ]]
then
   echo "Error: environment variable SAGEMAKER_ROLE is required"
   exit 1
fi


if [[ "$FRAMEWORK_NAME" == "" ]]
then
    echo "Error: environment variable FRAMEWORK_NAME is required"
    exit 1
fi

# Test Optional Environment Variables
if [[ "$DB_PORT" == "" ]]
then
    export DB_PORT=1527
fi

if [[ "$TASK_NAME" == "" ]]
then
    export TASK_NAME="bobby-0"
fi

if [[ "$COMPONENT" == "" ]]
then
    export COMPONENT="mlmanager"
fi

if [[ "$WORKER_THREADS" == "" ]]
then
    export WORKER_THREADS=5
fi

if [[ "$MLFLOW_PERSIST_PATH" == "" ]]
then
    export MLFLOW_PERSIST_PATH="/artifacts"
fi

# Start Main Processes
echo "Starting up Docker Daemon"
nohup ${BOBBY_SRC_HOME}/scripts/run_dind.sh

echo "Starting Worker"
python3.6 ${BOBBY_SRC_HOME}/main.py

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
