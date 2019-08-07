#!/usr/bin/env bash

MLFLOW_LOG_FILE='/var/log/mlflow-server.log'

foundNoArgumentExit () {
    echo "Error: environment variable $1 is required"
    exit 1;
}

# check environment vars
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

<<<<<<< HEAD
if [[ "$MLFLOW_PERSIST_PATH" == "" ]]
then
    export MLFLOW_PERSIST_PATH="/artifacts"
fi

if [[ "$ENVIRONMENT" == "aws" ]]
then
    echo "Running on AWS..."
    if [[ "$AWS_ACCESS_KEY_ID" == "" ]]
    then
       foundNoArgumentExit "AWS_ACCESS_KEY_ID"
    fi

    if [[ "$AWS_SECRET_ACCESS_KEY" == "" ]]
    then
        foundNoArgumentExit "AWS_SECRET_ACCESS_KEY"
    fi

    if [[ "$S3_BUCKET_NAME" == "" ]]
    then
        foundNoArgumentExit "S3_BUCKET_NAME"
    fi

    export MLFLOW_DEFAULT_ARTIFACT_STORE="${S3_BUCKET_NAME}${MLFLOW_PERSIST_PATH}"

elif [[ "$ENVIRONMENT" == "azure" ]]
then
    echo "Running on Azure..."
    if [[ "$AZURE_STORAGE_ACCESS_KEY" == "" ]]
    then
        foundNoArgumentExit "AZURE_STORAGE_KEY"
    fi

    if [[ "$AZURE_STORAGE_CONTAINER" == "" ]]
    then
        foundNoArgumentExit "AZURE_STORAGE_CONTAINER"
    fi

    if [[ "$AZURE_STORAGE_ACCOUNT" == "" ]]
    then
        foundNoArgumentExit "AZURE_STORAGE_ACCOUNT"
    fi
    export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=${AZURE_STORAGE_ACCOUNT};AccountKey=${AZURE_STORAGE_ACCESS_KEY}EndpointSuffix=core.windows.net"
    export MLFLOW_DEFAULT_ARTIFACT_STORE="wasbs://${AZURE_STORAGE_CONTAINER}@${AZURE_STORAGE_ACCOUNT}.blob.core.windows.net${MLFLOW_PERSIST_PATH}"
else
    foundNoArgumentExit "ENVIRONMENT"
=======
if [[ "$FRAMEWORK_NAME" == "" ]]
then
    echo "Error: environment variable FRAMEWORK_NAME is required"
    exit 1
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
>>>>>>> e70754b160dd8e4f48aabdc9721447c9b9f9d634
fi

# Test Optional Environment Variables
if [[ "$MLFLOW_PORT" == "" ]]
then
    export MLFLOW_PORT=5001
fi
<<<<<<< HEAD
if [[ "$GUI_PORT" == "" ]]
then
=======
if [[ "$GUI_PORT" == "" ]]; then
>>>>>>> e70754b160dd8e4f48aabdc9721447c9b9f9d634
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

export SQLALCHEMY_ODBC_URL="splicemachinesa://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/splicedb"

# Start Job Tracker GUI
echo "Starting Job Tracking UI on port :${GUI_PORT}"
nohup gunicorn --bind 0.0.0.0:${GUI_PORT} --chdir ${SRC_HOME}/app --workers ${GUNICORN_THREADS} main:APP &

# Start MLFlow Tracking Server
echo "Starting MLFlow Server on port :${MLFLOW_PORT}"
mlflow server --host 0.0.0.0 --backend-store-uri "${SQLALCHEMY_ODBC_URL}" \
<<<<<<< HEAD
    --default-artifact-root "${MLFLOW_DEFAULT_ARTIFACT_STORE}" -p ${MLFLOW_PORT} 2>&1 | tee ${MLFLOW_LOG_FILE}
=======
    --default-artifact-root "${S3_BUCKET_NAME}${MLFLOW_PERSIST_PATH}" -p ${MLFLOW_PORT} 2>&1 | tee ${MLFLOW_LOG_FILE}


>>>>>>> e70754b160dd8e4f48aabdc9721447c9b9f9d634
