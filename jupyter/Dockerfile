FROM jupyter/pyspark-notebook:d4cbf2f80a2a

USER root

ENV BUILD_HOME=/tmp

COPY build $BUILD_HOME

# Install Apt Modules
RUN apt update && \
    apt install -y $(cat $BUILD_HOME/apt-packages.txt)

# Install Pip modules
RUN pip install -r $BUILD_HOME/requirements.txt

# Delete Build Dependencies
RUN rm -rf $BUILD_HOME

EXPOSE 8888