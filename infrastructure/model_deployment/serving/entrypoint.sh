#!/usr/bin/env bash

MODEL=${MOUNT_PATH:-/opt/ml/model}

echo "Starting serving server..."
python -c "from mlflow.models import container as C; C._serve()"
