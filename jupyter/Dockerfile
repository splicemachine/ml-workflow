FROM jupyter/pyspark-notebook:d4cbf2f80a2a

EXPOSE 8888
RUN pip install mlflow==1.1.0 && \
    git clone https://github.com/splicemachine/pysplice && \
    cd pysplice && \
    git checkout DBAAS-2571 && \
    pip install .

