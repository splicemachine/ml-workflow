#!/usr/bin/env bash

conda env create -n retrain -f $MOUNT_PATH/$CONDA_ENV_NAME
conda activate retrain
python3.7 /opt/retrain.py

