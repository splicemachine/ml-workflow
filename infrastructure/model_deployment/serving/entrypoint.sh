#!/usr/bin/env bash

MODEL=${MOUNT_PATH:-/opt/ml/model}

echo "Installing Image Serving Dependencies for model at $MODEL..."

python -c "from mlflow.models.container import _install_pyfunc_deps; \\
           _install_pyfunc_deps('$MODEL', install_mlflow=True)"

echo "Done installing dependencies..."
echo "Starting serving server..."
python -c "from mlflow.models import container as C; C._serve()"
