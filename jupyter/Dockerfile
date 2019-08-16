FROM jupyter/pyspark-notebook:d4cbf2f80a2a

# ODBC Driver Configuration
ARG odbc_driver_version='2.7.63.0'
ENV ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

ENV BUILD_HOME=/build

COPY build $BUILD_HOME

USER root

# Install Apt Modules | as root
RUN apt update && \
    apt install -y $(cat $BUILD_HOME/apt_packages.txt)

# Install ODBC Driver
RUN cd $BUILD_HOME && \
    wget -O splice_odbc.tar.gz $ODBC_DRIVER_ARTIFACT && \
    tar xzf splice_odbc.tar.gz && \
    cd splice_odbc_linux64-${odbc_driver_version} && \
    bash ./install.sh -q

# Spark needs the user to be non-root or it throws an error

# Install Pip modules
RUN pip install -r $BUILD_HOME/requirements.txt

# Delete Build Dependencies
RUN rm -rf $BUILD_HOME

USER jovyan
EXPOSE 8888
