ARG splice_jupyter_allspark_version
FROM splicemachine/sm_k8_jupyter_allspark-3.0.0:$splice_jupyter_allspark_version

COPY . /opt
RUN fix-permissions /opt/entrypoint.sh && fix-permissions /opt/retrain.py
WORKDIR /opt
ENTRYPOINT /opt/entrypoint.sh
