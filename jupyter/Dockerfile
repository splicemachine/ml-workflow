FROM beakerx/beakerx
LABEL maintainer="nnygaard@splicemachine.com,bepstein@splicemachine.com,abaveja@splicemachine.com"

# ODBC Driver Configuration
ARG odbc_driver_version='2.7.63.0'
ENV ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'
ENV SPLICE_JAR_DIR='/usr/lib/splicemachine'
ENV BUILD_HOME=/build
ENV SRC_HOME=/opt/notebook
ARG sm_version=3.0.0.1949
ARG SPLICE_BUILD=3.0.0.1949
ARG SPLICE_BUILD_SUFFIX=p0.111
ARG SPLICE_CDH_VERSION=cdh6.3.0
ARG SPLICEMACHINE_URI=https://splice-releases.s3.amazonaws.com/${SPLICE_BUILD}/cluster/parcel/${SPLICE_CDH_VERSION}/SPLICEMACHINE-${SPLICE_BUILD}.${SPLICE_CDH_VERSION}.${SPLICE_BUILD_SUFFIX}-el7.parcel
ARG SPLICE_FOLDER_NAME=SPLICEMACHINE-${SPLICE_BUILD}.${SPLICE_CDH_VERSION}.${SPLICE_BUILD_SUFFIX}
ARG STATIC_BASE_ARTIFACT_URI=https://s3.amazonaws.com/splicemachine/artifacts


ARG pyspark_version='2.4.0'

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

# Get Splicemachine jars
RUN mkdir -p $SPLICE_JAR_DIR && \
    curl -kLs $SPLICEMACHINE_URI | tar -xz && \
    mv $SPLICE_FOLDER_NAME $SPLICE_JAR_DIR && \
    ln -s $SPLICE_JAR_DIR/bin/sqlshell.sh /usr/bin/sqlshell.sh

# Spark needs the user to be non-root or it throws an error
RUN /opt/conda/envs/beakerx/bin/pip  install -r $BUILD_HOME/requirements.txt && \
    /opt/conda/envs/beakerx/bin/pip install 'pyspark=='$pyspark_version

# Delete Build Dependencies
RUN rm -rf $BUILD_HOME

COPY notebooks $SRC_HOME

RUN chmod -R 777 $SRC_HOME
RUN apt-get -y install openjdk-8-jdk
ENV CLASSPATH=$SRC_HOME/lib/*:/usr/local/share/py4j/py4j0.10.8.1.jar:$SPLICE_JAR_DIR/$SPLICE_FOLDER_NAME/*

RUN pip install sparkmonitor-s==0.0.11

WORKDIR $SRC_HOME

#RUN jupyter nbextension install --py sparkmonitor --user --symlink && \
#    jupyter nbextension enable sparkmonitor --user --py && \
#    jupyter serverextension enable --py --user sparkmonitor && \
#    echo "c.InteractiveShellApp.extensions.append('sparkmonitor.kernelextension')" >>  \
#    $(ipython profile locate default)/ipython_kernel_config.py
