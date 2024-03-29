ARG splice_base_image_version
FROM splicemachine/sm_k8_base:$splice_base_image_version

# Shiro config
ARG sm_version=3.0.0.1944
ARG shiro_core_version=shiro-core-1.2.3
ARG shiro_web_version=shiro-web-1.2.3
ARG splice_shiro_version=splice-shiro-$sm_version
ARG SHIRO_CORE_URI=https://splicemachine.s3.amazonaws.com/artifacts/$shiro_core_version.jar
ARG SHIRO_WEB_URI=https://splicemachine.s3.amazonaws.com/artifacts/$shiro_web_version.jar
ARG SPLICE_SHIRO_URI=https://splicemachine.s3.amazonaws.com/artifacts/splice-shiro-2.7.0.1915.jar

# ODBC Driver Configuration
ARG odbc_driver_version='2.8.77.0'
ARG ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

# Defining Root Directory
ENV SRC_HOME=/opt/mlflow
ENV BUILD_HOME=/tmp

# Bug w/ click
ENV LC_ALL en_US.utf-8
ENV LANG en_US.utf-8

COPY mlflow/build $BUILD_HOME

# Install development packages.
RUN yum clean all && \
    rm -rf /var/cache/yum && \
    yum -y install $(cat /$BUILD_HOME/yum_packages.txt) && \
    pip3 install -q -r $BUILD_HOME/requirements.txt && \
    cd $BUILD_HOME && \
    wget -O splice_odbc.tar.gz $ODBC_DRIVER_ARTIFACT && \
    tar xzf splice_odbc.tar.gz && \
    cd splice_odbc_linux64-${odbc_driver_version} && \
    sh ./install.sh -q && \
    rm -rf $BUILD_HOME/*


COPY shared $BUILD_HOME//shared
COPY mlflow/splice_mlflow_plugin $BUILD_HOME/splice_mlflow_plugin

RUN pip3 install --user /tmp/shared/. && \
    pip3 install  $BUILD_HOME/splice_mlflow_plugin && \
    rm -rf $BUILD_HOME/*

# Create source directory
RUN mkdir -p $SRC_HOME/lib

# Copy Source into Container
COPY mlflow/src $SRC_HOME
COPY mlflow/src/lib $SRC_HOME/lib

# Copy Shiro (and dependencies) for authentication
RUN \
  curl -kLs $SHIRO_CORE_URI      -o $SRC_HOME/lib/$shiro_core_version.jar && \
  curl -kLs $SHIRO_WEB_URI       -o $SRC_HOME/lib/$shiro_web_version.jar && \
  curl -kLs $SPLICE_SHIRO_URI    -o $SRC_HOME/lib/$splice_shiro_version.jar && \
  curl -kls https://repo1.maven.org/maven2/org/slf4j/slf4j-jdk14/1.7.28/slf4j-jdk14-1.7.28.jar -o $SRC_HOME/lib/slf4j-log4j12-1.7.28.jar && \
  curl -kls https://repo1.maven.org/maven2/org/slf4j/slf4j-jdk14/1.7.28/slf4j-jdk14-1.7.28-sources.jar -o $SRC_HOME/lib/slf4j-log4j12-1.7.28-sources.jar && \
  curl -kls https://repo1.maven.org/maven2/org/slf4j/slf4j-jdk14/1.7.28/slf4j-jdk14-1.7.28-javadoc.jar -o $SRC_HOME/lib/slf4j-log4j12-1.7.28-javadoc.jar && \
  curl -kls https://repo1.maven.org/maven2/ch/qos/logback/logback-classic/0.9.19/logback-classic-0.9.19.jar -o $SRC_HOME/lib/logback-classic-0.9.19.jar && \
  curl -kls https://repo1.maven.org/maven2/ch/qos/logback/logback-core/0.9.19/logback-core-0.9.19.jar -o $SRC_HOME/lib/logback-core-0.9.19.jar && \
  curl -kLs http://repository.splicemachine.com/nexus/content/groups/public/com/splicemachine/db-client/$sm_version/db-client-$sm_version.jar -o $SRC_HOME/lib/db-client.jar && \
  curl -kLs http://repository.splicemachine.com/nexus/content/groups/public/com/splicemachine/db-client/$sm_version/db-client-$sm_version-sources.jar -o $SRC_HOME/lib/db-client-sources.jar


# Bug fix for the mlflow 1.15 release
COPY mlflow/build/39d1c3be5f05_add_is_nan_constraint_for_metrics_tables_if_necessary.py /usr/local/lib/python3.6/site-packages/mlflow/store/db_migrations/versions/

ENV CLASSPATH=$SRC_HOME/lib/*:/usr/local/share/py4j/py4j0.10.8.1.jar

CMD $SRC_HOME"/scripts/entrypoint.sh"
