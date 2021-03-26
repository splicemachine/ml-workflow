ARG splice_base_image_version
FROM splicemachine/sm_k8_base:$splice_base_image_version

LABEL maintainer="nnygaard@splicemachine.com,bepstein@splicemachine.com,abaveja@splicemachine.com"

ENV PYSPARK_PYTHON="/usr/bin/python3"

# ODBC Driver Configuration
ARG odbc_driver_version='2.7.63.0'
ARG ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

# Spark Configuration
ARG spark_version='3.1.1'

# File Retriever Configuration
ARG file_retriever_version='0.0.3'

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
    sh -c 'echo -e "[azure-cli]\nname=Azure CLI\nbaseurl=https://packages.microsoft.com/yumrepos/azure-cli\nenabled=1\ngpgcheck=1\ngpgkey=https://packages.microsoft.com/keys/microsoft.asc" > /etc/yum.repos.d/azure-cli.repo' && \
    yum -y update && \
    yum -y install $(cat /$BUILD_HOME/yum_packages.txt) && \
    yum -y groupinstall development && \
    yum -y --enablerepo=extras install epel-release && \
    yum clean all && \
    rm -rf /var/cache/yum

# Install ODBC Driver
RUN cd $BUILD_HOME && \
    wget -O splice_odbc.tar.gz $ODBC_DRIVER_ARTIFACT && \
    tar xzf splice_odbc.tar.gz && \
    cd splice_odbc_linux64-${odbc_driver_version} && \
    sh ./install.sh -q

# Install Python dependencies
RUN cd /tmp && \
  wget https://www.python.org/ftp/python/3.7.3/Python-3.7.3.tgz && \
  tar xzf Python-3.7.3.tgz && \
  cd Python-3.7.3  && \
  ./configure --enable-shared --enable-optimizations && \
  make altinstall && \
  ln -sfn /usr/local/bin/python3.7 /usr/bin/python3 && \
  ln -sfn /usr/local/bin/pip3.7 /usr/bin/pip3 && \
  rm -rf /tmp/Python-3.7.3 && \
  rm /tmp/rm -rf Python-3.7.3.tgz && \
  ldconfig /usr/local/lib

RUN pip3.7 install --upgrade pip wheel && \
    pip3.7 install -r $BUILD_HOME/requirements.txt && \
    pip3.7 install 'pyspark=='$spark_version

# Clean up Build Dependencies and setup src directories
RUN rm -r $BUILD_HOME/* && \
    mkdir -p $SRC_HOME && \
    mkdir -p $WORKER_HOME

# Install Helm
RUN curl https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash

# Install Jars
RUN mkdir -p $SRC_HOME/lib && \
    curl https://splicemachine.s3.amazonaws.com/artifacts/fileretriever-$file_retriever_version.jar -o $SRC_HOME/lib/fileretriever.jar


ARG SCALA_VERSION=2.12
ARG MLEAP_VERSION=0.16.0

ARG MLEAP_CORE_URI=https://repo1.maven.org/maven2/ml/combust/mleap/mleap-core_$SCALA_VERSION/$MLEAP_VERSION/mleap-core_$SCALA_VERSION-$MLEAP_VERSION.jar
ARG MLEAP_RUNTIME_URI=https://repo1.maven.org/maven2/ml/combust/mleap/mleap-runtime_$SCALA_VERSION/$MLEAP_VERSION/mleap-runtime_$SCALA_VERSION-$MLEAP_VERSION.jar
ARG MLEAP_BUNDLE_HDFS_URI=https://repo1.maven.org/maven2/ml/combust/bundle/bundle-hdfs_$SCALA_VERSION/$MLEAP_VERSION/bundle-hdfs_$SCALA_VERSION-$MLEAP_VERSION.jar
ARG MLEAP_BUNDLE_ML_URI=https://repo1.maven.org/maven2/ml/combust/bundle/bundle-ml_$SCALA_VERSION/$MLEAP_VERSION/bundle-ml_$SCALA_VERSION-$MLEAP_VERSION.jar
ARG MLEAP_BASE_URI=https://repo1.maven.org/maven2/ml/combust/mleap/mleap-base_$SCALA_VERSION/$MLEAP_VERSION/mleap-base_$SCALA_VERSION-$MLEAP_VERSION.jar
ARG MLEAP_SPARK_URI=https://repo1.maven.org/maven2/ml/combust/mleap/mleap-spark_$SCALA_VERSION/$MLEAP_VERSION/mleap-spark_$SCALA_VERSION-$MLEAP_VERSION.jar
ARG MLEAP_SPARK_BASE_URI=https://repo1.maven.org/maven2/ml/combust/mleap/mleap-spark-base_$SCALA_VERSION/$MLEAP_VERSION/mleap-spark-base_$SCALA_VERSION-$MLEAP_VERSION.jar
ARG MLEAP_TENSOR_URI=https://repo1.maven.org/maven2/ml/combust/mleap/mleap-tensor_$SCALA_VERSION/$MLEAP_VERSION/mleap-tensor_$SCALA_VERSION-$MLEAP_VERSION.jar


#ARG MLEAP_BUNDLE_HDFS_URI=https://splice-demo.s3.amazonaws.com/mleap_spark3_repo/bundle-hdfs_2.12-0.17.0-SNAPSHOT.jar
#ARG MLEAP_CORE_URI=https://splice-demo.s3.amazonaws.com/mleap_spark3_repo/mleap-core_2.12-0.17.0-SNAPSHOT.jar
#ARG MLEAP_BUNDLE_ML_URI=https://splice-demo.s3.amazonaws.com/mleap_spark3_repo/bundle-ml_2.12-0.17.0-SNAPSHOT.jar
#ARG MLEAP_BASE_URI=https://splice-demo.s3.amazonaws.com/mleap_spark3_repo/mleap-base_2.12-0.17.0-SNAPSHOT.jar
#ARG MLEAP_RUNTIME_URI=https://splice-demo.s3.amazonaws.com/mleap_spark3_repo/mleap-runtime_2.12-0.17.0-SNAPSHOT.jar
#ARG MLEAP_SPARK_URI=https://splice-demo.s3.amazonaws.com/mleap_spark3_repo/mleap-spark_2.12-0.17.0-SNAPSHOT.jar
#ARG MLEAP_SPARK_BASE_URI=https://splice-demo.s3.amazonaws.com/mleap_spark3_repo/mleap-spark-base_2.12-0.17.0-SNAPSHOT.jar
#ARG MLEAP_TENSOR_URI=https://splice-demo.s3.amazonaws.com/mleap_spark3_repo/mleap-tensor_2.12-0.17.0-SNAPSHOT.jar

ARG SPRAY_JSON_URI=https://repo1.maven.org/maven2/io/spray/spray-json_$SCALA_VERSION/1.3.5/spray-json_$SCALA_VERSION-1.3.5.jar
ARG TYPESAFE_CONFIG_URI=https://repo1.maven.org/maven2/com/typesafe/config/1.4.0/config-1.4.0.jar
ARG SCALAPB_RUNTIME_URI=https://repo1.maven.org/maven2/com/thesamet/scalapb/scalapb-runtime_$SCALA_VERSION/0.9.0/scalapb-runtime_$SCALA_VERSION-0.9.0.jar
ARG SCALAPB_LENSES_URI=https://repo1.maven.org/maven2/com/thesamet/scalapb/lenses_$SCALA_VERSION/0.9.0/lenses_$SCALA_VERSION-0.9.0.jar
ARG SCALA_ARM_URI=https://repo1.maven.org/maven2/com/jsuereth/scala-arm_$SCALA_VERSION/2.0/scala-arm_$SCALA_VERSION-2.0.jar
ARG PROTOBUF_JAVA_URI=https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.11.1/protobuf-java-3.11.1.jar
RUN \
  curl -kLs $MLEAP_CORE_URI      -o $SRC_HOME/lib/mleap-core_$SCALA_VERSION-$MLEAP_VERSION.jar && \
  curl -kLs $MLEAP_RUNTIME_URI      -o $SRC_HOME/lib/mleap-runtime_$SCALA_VERSION-$MLEAP_VERSION.jar && \
  curl -kLs $MLEAP_BUNDLE_HDFS_URI      -o $SRC_HOME/lib/bundle-hdfs_$SCALA_VERSION-$MLEAP_VERSION.jar && \
  curl -kLs $MLEAP_BUNDLE_ML_URI        -o $SRC_HOME/lib/bundle-ml_$SCALA_VERSION-$MLEAP_VERSION.jar && \
  curl -kLs $MLEAP_BASE_URI             -o $SRC_HOME/lib/mleap-base_$SCALA_VERSION-$MLEAP_VERSION.jar && \
  curl -kLs $SPRAY_JSON_URI -o $SRC_HOME/lib/spray-json_$SCALA_VERSION-1.3.5.jar && \
  curl -kLs $TYPESAFE_CONFIG_URI -o $SRC_HOME/lib/config-1.4.0.jar && \
  curl -kLs $SCALAPB_RUNTIME_URI -o  $SRC_HOME/lib/scalapb-runtime_$SCALA_VERSION-0.9.0.jar && \
  curl -kLs $SCALAPB_LENSES_URI -o $SRC_HOME/lib/lenses_$SCALA_VERSION-0.9.0.jar && \
  curl -kLs $MLEAP_TENSOR_URI -o $SRC_HOME/lib/mleap-tensor_$SCALA_VERSION-$MLEAP_VERSION.jar && \
  curl -kLs $SCALA_ARM_URI -o $SRC_HOME/lib/scala-arm_$SCALA_VERSION-2.0.jar && \
  curl -kLs $PROTOBUF_JAVA_URI -o $SRC_HOME/lib/protobuf-java-3.11.1.jar && \
  curl -kLs $MLEAP_SPARK_URI -o $SRC_HOME/lib/mleap-spark_$SCALA_VERSION-$MLEAP_VERSION.jar && \
  curl -kLs $MLEAP_SPARK_BASE_URI -o $SRC_HOME/lib/mleap-spark-base_$SCALA_VERSION-$MLEAP_VERSION.jar

ARG MLRUNNER_VERSION='0.1.7'
ARG MLRUNNER_URI=http://repository.splicemachine.com/nexus/content/repositories/releases/com/splicemachine/mlrunner/$MLRUNNER_VERSION/mlrunner-$MLRUNNER_VERSION-shaded.jar
RUN curl -kLs $MLRUNNER_URI -o $SRC_HOME/lib/mlrunner.jar

# Install Shared Lib
COPY shared /tmp/shared
RUN pip3 install /tmp/shared && \
    rm -rf /tmp/shared

COPY bobby/src $SRC_HOME

# make the entrypoint executable by all users
RUN chmod a+x $SRC_HOME/scripts/entrypoint.sh
#RUN pip3 install -q --upgrade pyspark==3.1.1
VOLUME /var/lib/docker
EXPOSE 2375
CMD $SRC_HOME"/scripts/entrypoint.sh"
