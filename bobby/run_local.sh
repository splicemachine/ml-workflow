#!/usr/bin/env bash

#
# The purpose of this script is to run the bobby container on your local system.
#

DB_HOST=
DB_USER=splice
DB_PASSWORD=admin
S3_BUCKET_NAME="s3://splice-demo/machine-learning/sync"
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
CONTAINER_PORT=2375
HOST_PORT=2375

#
# Shows the usage of the script.
#

function show_usage() {
    echo
    echo "Usage: ${0} --db_host splice url [--db_user user {splice}] [--db_password password {admin}] [-s3_bucket_name number of nodes {s3://splice-demo/machine-learning/sync}] --aws_access_key_id Aws Access Key Id --aws_secret_access_key AWS Secret [--container_port container port {2375} [--host_port host port {2375}]"
    echo
    echo
    echo -e "\t--db_host\t\t\tThis is the splice machine odbc host  It should not include the user/password.  A valid example is nikhilsaccount-ml.splicemachine-dev.io"
    echo
	echo -e "\t--db_user\t\t\tThe splice machine database user.  Defaults to splice."
    echo
    echo -e "\t--db_password\t\t\tThe splice machine database password.  Defaults to admin"
    echo
	echo -e "\t--s3_bucket_name\t\tThe artifact bucket name to use for testing purposes. Defaults to s3://splice-demo/machine-learning/sync "
    echo
	echo -e "\t--aws_access_key_id\t\tThe AWS access key id.  It must have ECR, S3 and SageMaker permissions. "
    echo
	echo -e "\t--aws_secret_access_key\t\tThe AWS secret access key that corresponds to the AWS access id. "
    echo
	echo -e "\t--container_port\t\tThe container port to run on.  Defaults to 2375 "
    echo
	echo -e "\t--host_port\t\t\tThe host port to run on.  Defaults to 2375 "
    echo
    exit 1
}

#
# Prints the parameters that are going to be used
#
function print_parameters() {
    echo "=========== Parameters ================="
    echo "d=$JDBC_URL"
    echo "DB_USER=$DB_USER"
    echo "DB_PASSWORD=$DB_PASSWORD"
    echo "S3_BUCKET_NAME=$S3_BUCKET_NAME"
    echo "AWS_ACCESS_KEY_ID=REDACTED"
    echo "AWS_SECRET_ACCESS_KEY=REDACTED"
    echo "CONTAINER_PORT=$CONTAINER_PORT"
    echo "HOST_PORT=$HOST_PORT"
}

#
# Build the bobby docker image
#
function build_docker_image() {
    echo "Building Docker Container"
    make
}

#
# Run the bobby container
#
function run_container() {
    echo "Running Bobby Container"
    docker run --privileged \
    -e S3_BUCKET_NAME="$S3_BUCKET_NAME" \
    -e DB_HOST="$JDBC_URL" \
    -e DB_USER="$DB_USER" \
    -e DB_PASSWORD="$DB_PASSWORD" \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    -e DASH_PORT=$CONTAINER_PORT \
    -p $CONTAINER_PORT:$HOST_PORT \
    --name bobby splicemachine/bobby:latest /bin/bash
}


PARAMS=""
while (( "$#" )); do
  case "$1" in
    -d|--db_host)
      DB_HOST=$2
      shift 2
      ;;
    -u|--db_user)
      DB_USER=$2
      shift 2
      ;;
    -p|--db_password)
      DB_PASSWORD=$2
      shift 2
      ;;
    -b|--s3_bucket_name)
      S3_BUCKET_NAME=$2
      shift 2
      ;;
    -k|--aws_access_key_id)
      AWS_ACCESS_KEY_ID=$2
      shift 2
      ;;
    -s|--aws_secret_access_key)
      AWS_SECRET_ACCESS_KEY=$2
      shift 2
      ;;
    -c|--container_port)
      CONTAINER_PORT=$2
      shift 2
      ;;
    -h|--host_port)
      HOST_PORT=$2
      shift 2
      ;;
    --) # end argument parsing
      shift
      break
      ;;
    -*|--*=) # unsupported flags
      echo "Error: Unsupported flag $1" >&2
	  show_usage
      exit 1
      ;;
    *) # preserve positional arguments
      PARAMS="$PARAMS $1"
      shift
      ;;
  esac
done
# set positional arguments in their proper place
eval set -- "$PARAMS"

if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "Missing aws access key id. "
    show_usage
	exit 2
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "Missing aws secret access key. "
    show_usage
	exit 2
fi

if [ -z "$DB_HOST" ]; then
    echo "Missing db url. "
    show_usage
	exit 2
fi


print_parameters
build_docker_image
run_container