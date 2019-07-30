#!/usr/bin/env bash

MLFLOW_LOG_FILE='/var/log/mlflow_server.log'

# check environment vars
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

if [[ "$MLFLOW_PORT" == "" ]]; then
   echo "Error: environment variable MLFLOW_PORT is required"
   exit 1
fi


if [[ "$GUI_PORT" == "" ]]; then
   echo "Error: environment variable GUI_PORT is required"
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

export SQLALCHEMY_ODBC_URL="splicemachinesa://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/splicedb"

# Start Job Tracker GUI
echo "Starting Job Tracking UI on port :${GUI_PORT}"
nohup gunicorn --bind 0.0.0.0:${GUI_PORT} --chdir ${SRC_HOME}/app --workers ${GUNICORN_THREADS} main:APP &

# Start MLFlow Tracking Server
echo "Starting MLFlow Server on port :${MLFLOW_PORT}"

mlflow server --host 0.0.0.0 --backend-store-uri "${SQLALCHEMY_ODBC_URL}" \
    --default-artifact-root "${S3_BUCKET_NAME}/${MLFLOW_PERSIST_PATH}" -p ${MLFLOW_PORT} 2>&1 | tee ${MLFLOW_LOG_FILE}


