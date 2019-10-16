FROM beakerx/beakerx
LABEL maintainer="nnygaard@splicemachine.com,bepstein@splicemachine.com,abaveja@splicemachine.com"

# ODBC Driver Configuration
ARG odbc_driver_version='2.7.63.0'
ENV ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

ENV BUILD_HOME=/build
ENV SRC_HOME=/opt/notebook

ARG pyspark_version='2.4.4'

COPY build $BUILD_HOME

USER root

# Install Apt Packages (sudo required)
RUN apt-get update && \
    apt-get install -y $(cat $BUILD_HOME/apt_packages.txt)

# Install ODBC Driver
RUN cd $BUILD_HOME && \
    wget -O splice_odbc.tar.gz $ODBC_DRIVER_ARTIFACT && \
    tar xzf splice_odbc.tar.gz && \
    cd splice_odbc_linux64-${odbc_driver_version} && \
    bash ./install.sh -q

# Spark needs the user to be non-root or it throws an error
RUN /opt/conda/envs/beakerx/bin/pip  install -r $BUILD_HOME/requirements.txt && \
    /opt/conda/envs/beakerx/bin/pip install 'pyspark=='$pyspark_version

# Delete Build Dependencies
RUN rm -rf $BUILD_HOME

COPY notebooks $SRC_HOME

RUN chmod -R 777 $SRC_HOME

WORKDIR $SRC_HOME


