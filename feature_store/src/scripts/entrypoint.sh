#!/usr/bin/env bash

FEATURE_STORE_LOG_FILE='/var/log/featurestore-server.log'

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

# Test Optional Environment Variables
if [[ "$FEATURE_STORE_PORT" == "" ]]
then
    export FEATURE_STORE_PORT=8000
fi


if [[ "$DB_PORT" == "" ]]
then
    export DB_PORT=1527
fi

if [[ "$TASK_NAME" == "" ]]
then
    export TASK_NAME="featurestore-0"
fi

if [[ "$COMPONENT" == "" ]]
then
    export COMPONENT="mlmanager" #CHECK THIS
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

echo "Starting Java Gateway Server for py4j"
nohup java gateway &

cd ${SRC_HOME}/rest_api

# Create the necessary tables
echo "Creating Feature Store Tables"
python3 preload.py
# Start Feature Store Server logging to feature_store log file
echo "Starting Feature Store Server on port :${FEATURE_STORE_PORT}"
nohup gunicorn --timeout 300 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:${FEATURE_STORE_PORT} --workers ${GUNICORN_THREADS} main:APP 2>&1 | tee ${FEATURE_STORE_LOG_FILE}
