#!/usr/bin/env bash
logMessage (){
  echo "INFO $(date +%Y-%m-%d-%H-%M-%S.%s) - $1"
}

MODEL=${MOUNT_PATH:-/opt/ml/model}

logMessage "Step 1/1: Starting serving server"
python -c "from mlflow.models import container as C; C._serve()" || logMessage "SERVING_CONTAINER_FAILED"

