ARG splice_base_image_version
FROM splicemachine/sm_k8_base:$splice_base_image_version

LABEL maintainer="nnygaard@splicemachine.com,bepstein@splicemachine.com,abaveja@splicemachine.com"

# ml-workflow-lib Configuration
ARG github_PAT

# so the pyspark drivers use python3.7 instead of python2
ENV PYSPARK_PYTHON = /usr/local/bin/python3.7

# ODBC Driver Configuration
ARG odbc_driver_version='2.7.63.0'
ARG ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

# Spark Configuration
ARG spark_version='2.4.0'

# Defining Directories
ENV SRC_HOME=/opt/bobby
ENV BUILD_HOME=/tmp
ENV WORKER_HOME=/opt/worker

# Bug w/ click
ENV LC_ALL en_US.utf-8
ENV LANG en_US.utf-8

# Add the list of necessary environment packages for development.
COPY bobby/build $BUILD_HOME

# Install lzma python dependency for python 3.7.3
RUN yum install -y xz-devel

# Install python 3.7.3
RUN yum update -y && \
    yum install yum-utils -y && \
    yum groupinstall development -y && \
    yum -y --enablerepo=extras install epel-release && \
    yum install -y gcc openssl-devel bzip2-devel libffi-devel && \
    yum install -y make
RUN cd /tmp && \
  wget https://www.python.org/ftp/python/3.7.3/Python-3.7.3.tgz && \
  tar xzf Python-3.7.3.tgz && \
  cd Python-3.7.3  && \
  ./configure --enable-optimizations && \
  make altinstall && \
  ln -sfn /usr/local/bin/python3.7 /usr/bin/python3 && \
  ln -sfn /usr/local/bin/pip3.7 /usr/bin/pip3 && \
  rm -rf /tmp/Python-3.7.3 && \
  rm /tmp/rm -rf Python-3.7.3.tgz && \
  yum remove -y  make gcc openssl-devel bzip2-devel && \
  rm -rf /var/cache/yum && yum clean all

# Add the repository for docker and IUS Repo fo Python3.
# Azure forces this ugly code upon us. Any workarounds would be appreciated.
RUN yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo && \
    rpm --import https://packages.microsoft.com/keys/microsoft.asc && \
    sh -c 'echo -e "[azure-cli]\nname=Azure CLI\nbaseurl=https://packages.microsoft.com/yumrepos/azure-cli\nenabled=1\ngpgcheck=1\ngpgkey=https://packages.microsoft.com/keys/microsoft.asc" > /etc/yum.repos.d/azure-cli.repo'

# Install development packages.
RUN yum clean all && \
    rm -rf /var/cache/yum && \
    yum -y install $(cat /$BUILD_HOME/yum_packages.txt)

# Install ODBC Driver
RUN cd $BUILD_HOME && \
    wget -O splice_odbc.tar.gz $ODBC_DRIVER_ARTIFACT && \
    tar xzf splice_odbc.tar.gz && \
    cd splice_odbc_linux64-${odbc_driver_version} && \
    sh ./install.sh -q

# Install Python dependencies
#RUN pip3.7 install $ML_WORKFLOW_LIB_PACKAGE && \
RUN pip3.7 install -r $BUILD_HOME/requirements.txt && \
    pip3.7 install 'pyspark=='$spark_version

# Clean up Build Dependencies and setup src directories
RUN rm -r $BUILD_HOME/* && \
    rm -rf /spark.tgz && \
    mkdir -p $SRC_HOME && \
    mkdir -p $WORKER_HOME

COPY bobby/src $SRC_HOME

COPY mlmanager-lib /tmp/mlmanager-lib
RUN pip3.7 install /tmp/mlmanager-lib/.

# make the entrypoint executable by all users
RUN chmod a+x $SRC_HOME/scripts/entrypoint.sh

VOLUME /var/lib/docker
EXPOSE 2375
CMD $SRC_HOME"/scripts/entrypoint.sh"
