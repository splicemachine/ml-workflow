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
   echo "Error: environment variable S3_BUCKET_NAME is required"
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

if [[ "$MODE" == ""  ]] || [[ "$MODE" == "production" ]]
then
    export MODE="production"
elif [[ "$MODE" == "development" ]]
then
    export MODE="development"
fi

# Start Main Processes
echo "Starting up Docker Daemon"
nohup ${BOBBY_SRC_HOME}/scripts/run_dind.sh

echo "Starting Worker"
python3.6 ${BOBBY_SRC_HOME}/main.py
