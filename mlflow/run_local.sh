#!/usr/bin/env bash

# <n> means replace this with an appropriate value

echo "Building MLFlow Container"
docker build -t mlflow .

echo "Running Container"
docker run --name mlflow --network mynetwork -p <CONTAINER MLFLOW PORT>:<HOST PORT> \
    -p <CONTAINER JOB HANDLER PORT>:<HOST PORT> \
    -e S3_BUCKET_NAME='<YOUR S3 BUCKET NAME>' \
    -e JDBC_URL='<YOUR JDBC URL>' \
    -e USER='<DB USER>' -e PASSWORD='<DB PASSWORD>' -e MLFLOW_PORT=<CONTAINER PORT> \
    -e API_PORT='4041'  -e AWS_ACCESS_KEY_ID='AKIAILC2A7RT2GK5RFBQ' \
    -e  WS_SECRET_ACCESS_KEY='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' mlflow