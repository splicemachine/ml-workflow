#!/usr/bin/env bash

# <n> means replace this with an appropriate value

echo "Building Docker Container"
docker build -t bobby .

echo "Running Bobby Container"
docker run --privileged -e S3_BUCKET_NAME='<YOUR S3 BUCKET' -e \
JDBC_URL='<YOUR JDBC URL>' -e USER='<DB USER>' -e PASSWORD='<DB PASS>' -e  \
AWS_ACCESS_KEY_ID='<AWS ACCESS KEY ID (WITH ECR, S3 and SageMaker permission>' -e \
AWS_SECRET_ACCESS_KEY='<AWS SECRET ACCESS KEY>' -e \
DASH_PORT='<CONTAINER PORT TO RUN ON>' -p <DASH_CONTAINER_PORT>:<HOST PORT> --name bobby bobby