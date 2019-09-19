ARG splice_base_image_version
FROM splicemachine/sm_k8_base:$splice_base_image_version

# Shiro config
ARG sm_version=2.8.0.1920
ARG shiro_core_version=shiro-core-1.2.3
ARG shiro_web_version=shiro-web-1.2.3
ARG splice_shiro_version=splice-shiro-$sm_version
ENV SHIRO_CORE_URI=https://splicemachine.s3.amazonaws.com/artifacts/$shiro_core_version.jar
ENV SHIRO_WEB_URI=https://splicemachine.s3.amazonaws.com/artifacts/$shiro_web_version.jar
ENV SPLICE_SHIRO_URI=https://splicemachine.s3.amazonaws.com/artifacts/splice-shiro-2.7.0.1915.jar
ENV SPLICE_BUILD=2.8.0.1920-SNAPSHOT

# ml-workflow-lib Configuration
ARG github_PAT
# If you want to install from a branch instead of a version
# ARG ml_workflow_lib_branch='SOME_BRANCH'
# ENV ML_WORKFLOW_LIB_PACKAGE="git+https://$github_PAT:@github.com/splicemachine/ml-workflow-lib.git@$ml_workflow_lib_branch"

ARG ml_workflow_lib_version='0.0.4-dev'
ENV ML_WORKFLOW_LIB_PACKAGE="git+https://$github_PAT:@github.com/splicemachine/ml-workflow-lib.git@$ml_workflow_lib_version"

# ODBC Driver Configuration
ARG odbc_driver_version='2.7.63.0'
ENV ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

# Defining Root Directory
ENV SRC_HOME=/opt/mlflow
ENV BUILD_HOME=/tmp

# Bug w/ click
ENV LC_ALL en_US.utf-8
ENV LANG en_US.utf-8

COPY build $BUILD_HOME

# Install lzma python dependency python 3.7.3
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
COPY splice_mlflow_plugin $BUILD_HOME/splice_mlflow_plugin
RUN pip3.7 install -r $BUILD_HOME/requirements.txt && \
    pip3.7 install $ML_WORKFLOW_LIB_PACKAGE && \
    pip3.7 install $BUILD_HOME/splice_mlflow_plugin

# Remove Build Directories and Create source directory
RUN rm -rf $BUILD_HOME/* && \
    mkdir -p $SRC_HOME/lib

COPY src $SRC_HOME
COPY src/lib $SRC_HOME/lib

# Copy Shiro (and dependencies) for authentication
RUN \
  curl -kLs $SHIRO_CORE_URI      -o $SRC_HOME/lib/$shiro_core_version.jar && \
  curl -kLs $SHIRO_WEB_URI       -o $SRC_HOME/lib/$shiro_web_version.jar && \
  curl -kLs $SPLICE_SHIRO_URI    -o $SRC_HOME/lib/$splice_shiro_version.jar && \
  curl -kLs https://www.slf4j.org/dist/slf4j-1.7.28.zip -o $SRC_HOME/lib/slf4j.zip && unzip $SRC_HOME/lib/slf4j.zip -d $SRC_HOME/lib/ && \
  rm -f $SRC_HOME/lib/slf4j.zip && \
  curl -kLs http://repository.splicemachine.com/nexus/content/groups/public/com/splicemachine/db-client/$SPLICE_BUILD/db-client-2.8.0.1920-20190620.183307-9.jar -o $SRC_HOME/lib/db-client.jar && \
  curl -kLs http://repository.splicemachine.com/nexus/content/groups/public/com/splicemachine/db-client/$SPLICE_BUILD/db-client-2.8.0.1920-20190620.183307-9-sources.jar -o $SRC_HOME/lib/db-client-sources.jar

ENV CLASSPATH=$SRC_HOME/lib/*:$SRC_HOME/lib/slf4j-1.7.28/*:/usr/local/share/py4j/py4j0.10.8.1.jar
CMD $SRC_HOME"/scripts/entrypoint.sh"
