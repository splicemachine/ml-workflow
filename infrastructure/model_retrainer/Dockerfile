FROM splicemachine/sm_k8_jupyter_allspark-3.0.0:$splice_jupyter_allspark_version

ARG SHARED_DIR=/opt/shared
ARG LOGGING_DIR=./shared/shared/logger
ARG INFRA_DIR=./infrastructure/model_retrainer

RUN pip3.7 install loguru

COPY $INFRA_DIR/entrypoint.sh /opt/entrypoint.sh
COPY $INFRA_DIR/retrain.py /opt/retrain.py
RUN mkdir -p $SHARED_DIR/logger && touch $SHARED_DIR/__init__.py
COPY $LOGGING_DIR/* $SHARED_DIR/logger


WORKDIR /opt
ENTRYPOINT /opt/entrypoint.sh
