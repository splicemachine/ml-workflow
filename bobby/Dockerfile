ARG splice_base_image_version
FROM splicemachine/sm_k8_base:$splice_base_image_version

LABEL maintainer="nnygaard@splicemachine.com,bepstein@splicemachine.com,abaveja@splicemachine.com"

ENV PYSPARK_PYTHON="/usr/bin/python3"

# ODBC Driver Configuration
ARG odbc_driver_version='2.7.63.0'
ARG ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

# Spark Configuration
ARG spark_version='2.4.0'

# File Retriever Configuration
ARG file_retriever_version='0.0.1'

# Defining Directories
ENV SRC_HOME=/opt/bobby
ENV BUILD_HOME=/tmp
ENV WORKER_HOME=/opt/worker

# Bug w/ click
ENV LC_ALL en_US.utf-8
ENV LANG en_US.utf-8

# Add the list of necessary environments packages for development.
COPY bobby/build $BUILD_HOME

# Add the repository for docker and IUS Repo fo Python3.
# Azure forces this ugly code upon us. Any workarounds would be appreciated.
RUN yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo && \
    rpm --import https://packages.microsoft.com/keys/microsoft.asc && \
    sh -c 'echo -e "[azure-cli]\nname=Azure CLI\nbaseurl=https://packages.microsoft.com/yumrepos/azure-cli\nenabled=1\ngpgcheck=1\ngpgkey=https://packages.microsoft.com/keys/microsoft.asc" > /etc/yum.repos.d/azure-cli.repo'

# Install Yum packages.
RUN yum clean all && \
    rm -rf /var/cache/yum && \
    yum -y update && \
    yum -y install $(cat /$BUILD_HOME/yum_packages.txt)

# Install ODBC Driver
RUN cd $BUILD_HOME && \
    wget -O splice_odbc.tar.gz $ODBC_DRIVER_ARTIFACT && \
    tar xzf splice_odbc.tar.gz && \
    cd splice_odbc_linux64-${odbc_driver_version} && \
    sh ./install.sh -q

# Install Python dependencies
RUN pip3 install --upgrade pip wheel && \
    pip3 install -r $BUILD_HOME/requirements.txt && \
    pip3 install 'pyspark=='$spark_version

# Clean up Build Dependencies and setup src directories
RUN rm -r $BUILD_HOME/* && \
    mkdir -p $SRC_HOME && \
    mkdir -p $WORKER_HOME

# Install Helm
RUN curl https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash

# Install Jars
RUN mkdir -p $SRC_HOME/lib && \
    curl https://splicemachine.s3.amazonaws.com/artifacts/fileretriever-0.0.2-shaded.jar -o $SRC_HOME/lib/fileretriever.jar

# Install Shared Lib
COPY shared /tmp/shared
RUN pip3 install /tmp/shared && \
    rm -rf /tmp/shared

COPY bobby/src $SRC_HOME

# make the entrypoint executable by all users
RUN chmod a+x $SRC_HOME/scripts/entrypoint.sh

VOLUME /var/lib/docker
EXPOSE 2375
CMD $SRC_HOME"/scripts/entrypoint.sh"
