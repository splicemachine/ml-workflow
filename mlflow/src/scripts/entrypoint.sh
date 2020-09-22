#!/usr/bin/env bash

MLFLOW_LOG_FILE='/var/log/mlflow-server.log'

foundNoArgumentExit () {
    echo "Error: environment variable $1 is required"
    exit 1;
}

# check environments vars
if [[ "$DB_HOST" == "" ]]
then
   foundNoArgumentExit "DB_HOST"
fi

if [[ "$DB_USER" == "" ]]
then
   foundNoArgumentExit "DB_USER"
fi

if [[ "$DB_PASSWORD" == "" ]]
then
   foundNoArgumentExit "DB_PASSWORD"
fi

if [[ "$FRAMEWORK_NAME" == "" ]]
then
    echo "Error: environment variable FRAMEWORK_NAME is required"
    exit 1
fi

if [[ "$FRAMEWORK_NAME" == "" ]]
then
    echo "Error: environment variable FRAMEWORK_NAME is required"
    exit 1
fi

# Test Optional Environment Variables
if [[ "$MLFLOW_PORT" == "" ]]
then
    export MLFLOW_PORT=5001
fi

if [[ "$ENVIRONMENT" == "aws" ]] || [[ "$ENVIRONMENT" == "AWS" ]]
then
    export ENVIRONMENT="aws"

elif [[ "$ENVIRONMENT" == "azure" ]] || [[ "$ENVIRONMENT" == "AZ" ]] || [[ "$ENVIRONMENT" == "az" ]] || [[ "$ENVIRONMENT" == "AZURE" ]]
then
    export ENVIRONMENT="azure"
elif [[ "$ENVIRONMENT" == "openstack" ]] || [[ "$ENVIRONMENT" == "Openstack" ]] || [[ "$ENVIRONMENT" == "OPENSTACK" ]] || [[ "$ENVIRONMENT" == "OpenStack" ]]
then
    export ENVIRONMENT="openstack"
elif [[ "$ENVIRONMENT" == "gcp" ]] || [[ "$ENVIRONMENT" == "GCP" ]]
then
    export ENVIRONMENT="gcp"
else
    export ENVIRONMENT="default"
fi

if [[ "$GUI_PORT" == "" ]]; then
    export GUI_PORT=5003;
fi

if [[ "$DB_PORT" == "" ]]
then
    export DB_PORT=1527
fi

if [[ "$TASK_NAME" == "" ]]
then
    export TASK_NAME="mlflow-0"
fi

if [[ "$COMPONENT" == "" ]]
then
    export COMPONENT="mlmanager"
fi

if [[ "$GUNICORN_THREADS" == "" ]]
then
    export GUNICORN_THREADS=3
fi


if [[ "$MODE" == ""  ]] || [[ "$MODE" == "production" ]]
then
    export MODE="production"
elif [[ "$MODE" == "development" ]]
then
    export MODE="development"
fi


echo "Starting Job Tracking UI on port :${GUI_PORT}"
nohup gunicorn --bind 0.0.0.0:${GUI_PORT} --chdir ${SRC_HOME}/app --workers ${GUNICORN_THREADS} main:APP &

echo "Starting Java Gateway Server for py4j"
nohup java gateway &

# Start MLFlow Tracking Server logging to mlflow log file
echo "Starting MLFlow Server on port :${MLFLOW_PORT}"
mlflow server --host 0.0.0.0 --backend-store-uri "splicetracking://" \
    --default-artifact-root "spliceartifacts:////" -p ${MLFLOW_PORT} 2>&1 | tee ${MLFLOW_LOG_FILE}
