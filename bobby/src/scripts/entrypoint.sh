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

if [[ "$FRAMEWORK_NAME" == "" ]]
then
    echo "Error: environment variable FRAMEWORK_NAME is required"
    exit 1
fi

if [[ "$MLFLOW_URL" == "" ]]
then
    echo "Error: environment variable MLFLOW_URL is required"
fi

if [[ "$ENVIRONMENT" == "aws" ]] || [[ "$ENVIRONMENT" == "AWS" ]]
then
    export ENVIRONMENT="aws"
    if [[ "$SAGEMAKER_ROLE" == "" ]]
    then
       echo "Error: environment variable SAGEMAKER_ROLE is required"
       exit 1
    fi

elif [[ "$ENVIRONMENT" == "azure" ]] || [[ "$ENVIRONMENT" == "AZ" ]] || [[ "$ENVIRONMENT" == "az" ]] || [[ "$ENVIRONMENT" == "AZURE" ]]
then
    export ENVIRONMENT="azure"
    export AZURE_SUBSCRIPTION_ID=$(python3.7 ${SRC_HOME}/scripts/login_azure.py)
elif [[ "$ENVIRONMENT" == "openstack" ]] || [[ "$ENVIRONMENT" == "Openstack" ]] || [[ "$ENVIRONMENT" == "OPENSTACK" ]] || [[ "$ENVIRONMENT" == "OpenStack" ]]
then
    export ENVIRONMENT="openstack"
elif [[ "$ENVIRONMENT" == "gcp" ]] || [[ "$ENVIRONMENT" == "GCP" ]]
then
    export ENVIRONMENT="gcp"

else
    export ENVIRONMENT="default"
fi

# Test Optional Environment Variables
if [[ "$DB_PORT" == "" ]]
then
    export DB_PORT=1527
fi

if [[ "$API_PORT" == "" ]]
then
    export API_PORT=2375
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
nohup dockerd &
echo "Starting Worker. Listening on port ${API_PORT}"
nohup uvicorn --host 0.0.0.0 --port ${API_PORT} --app-dir ${SRC_HOME} main:APP
