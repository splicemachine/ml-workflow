#!/usr/bin/env bash

# <n> means replace this with an appropriate value

docker rm mlflow

echo "Building MLFlow Container"
docker build -t mlflow .

echo "Running Container"
#docker run --name mlflow \
#	#--network mynetwork \
#    -p 4040:4040 \
#	-p 4041:4041 \
#	-p 4042:4042 \
#	-e MLFLOW_PORT='4040' \
#    -e API_PORT='4041' \
#	-e DASH_PORT='4042' \
#    -e S3_BUCKET_NAME='s3://amrit-splice' \
#    -e JDBC_URL='jdbc:splice://amritsaccount-mlflow.splicemachine-qa.io:1527/splicedb;ssl=basic' \
#    -e USER='splice' \
#	-e PASSWORD='lYOaWxfQ' \
#	-e AWS_ACCESS_KEY_ID='AKIAILC2A7RT2GK5RFBQ' \
#    -e AWS_SECRET_ACCESS_KEY='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' \
#	mlflow

docker run \
	--name mlflow \
	-p 4040:4040 \
	-p 4041:4041 \
	-p 4042:4042 \
	-e DASH_PORT='4042' \
	-e S3_BUCKET_NAME='s3://amrit-splice' \
	-e JDBC_URL='jdbc:splice://amritsaccount-mlflow.splicemachine-qa.io:1527/splicedb;ssl=basic' \
	-e USER='splice' \
	-e PASSWORD='lYOaWxfQ' \
	-e MLFLOW_PORT='4040' \
	-e API_PORT='4041' \
	-e AWS_ACCESS_KEY_ID='AKIAILC2A7RT2GK5RFBQ' \
	-e AWS_SECRET_ACCESS_KEY='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' mlflow
