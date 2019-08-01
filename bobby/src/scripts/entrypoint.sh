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

if [[ "$MLFLOW_URL" == "" ]]
then
    echo "Error: environment variable MLFLOW_URL is required"
fi

if [[ "$AWS_ACCESS_KEY_ID" == "" ]]
then
    echo "Error: environment variable AWS_ACCESS_KEY_ID is required"
    exit 1
fi

if [[ "$AWS_SECRET_ACCESS_KEY" == "" ]]
then
    echo "Error: environment variable AWS_SECRET_ACCESS_KEY is required"
    exit 1
fi

# Test Optional Environment Variables
if [[ "$DB_PORT" == "" ]]
then
    export DB_PORT=1527
fi

if [[ "$MLFLOW_PORT" == "" ]]
then
    export MLFLOW_PORT=5001
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

# Setting mode to development/production changes the logging levels
if [[ "$MODE" == ""  ]] || [[ "$MODE" == "production" ]]
then
    export MODE="production"
elif [[ "$MODE" == "development" ]]
then
    export MODE="development"
fi

# Start Main Processes
echo "Starting up Docker Daemon"
nohup ${SRC_HOME}/scripts/run_dind.sh &

echo "Starting Worker"
python3.6 ${SRC_HOME}/main.py
