#!/usr/bin/env bash

logMessage (){
  echo "INFO $(date +%Y-%m-%d-%H-%M-%S.%s) - $1"
}

logMessage "Step 1/2: Creating Conda Environment for Retraining"

#(conda env create -n retrain -f $MOUNT_PATH/$CONDA_ENV_NAME
#conda activate retrain) || logMessage "RETRAINING_CONTAINER_FAILED"

(conda env update -n base -f $MOUNT_PATH/$CONDA_ENV_NAME) || logMessage "RETRAINING_CONTAINER_FAILED"
pip install --upgrade git+http://www.github.com/splicemachine/pysplice@DBAAS-4791

logMessage "Step 2/2: Loading Retrainer, Creating Contexts, and Retraining Model"
python3.7 /opt/retrain.py
logMessage "Job Completed!"
