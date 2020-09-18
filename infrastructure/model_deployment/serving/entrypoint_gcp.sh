#!/usr/bin/env bash

# Install gcloud cli
echo "Extracting model from cloud storage"

MODEL_DIR=/opt/ml
MODEL_ZIP=${MODEL_DIR}/model.zip
MODEL_FILE=${MODEL_DIR}/${MODEL_NAME}

source /root/google-cloud-sdk/completion.bash.inc
source /root/google-cloud-sdk/path.bash.inc
gsutil cp gs://$BUCKET_NAME/$MODEL_NAME.zip $MODEL_ZIP
unzip $MODEL_ZIP -d $MODEL_DIR
rm $MODEL_ZIP
if [ $MODEL_NAME != 'model' ]
then
mv $MODEL_FILE $MODEL_DIR/model
fi
echo "Starting serving server..."
python -c "from mlflow.models import container as C; C._serve()"

