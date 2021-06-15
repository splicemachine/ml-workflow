FROM python:3.7.3-slim

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
ENV SRC_HOME=/opt/feature_store
ENV BUILD_HOME=/tmp

# Bug w/ click
ENV LC_ALL en_US.utf-8
ENV LANG en_US.utf-8

COPY feature_store/build $BUILD_HOME


## development packages, Install ODBC Driver, Install Python deps, Cleanup
RUN mkdir -p mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y $(cat $BUILD_HOME/apt-packages.txt) && \
    cd $BUILD_HOME && \
    wget -O splice_odbc.tar.gz $ODBC_DRIVER_ARTIFACT && \
    tar xzf splice_odbc.tar.gz && \
    cd splice_odbc_linux64-${odbc_driver_version} && \
    ./install.sh -q && \
    pip3 install -q --no-cache-dir -r $BUILD_HOME/requirements.txt && \
    apt-get purge -y wget unzip build-essential libffi-dev libssl-dev && \
    apt-get clean && \
    rm -rf $BUILD_HOME/* && \
    mkdir -p $SRC_HOME/lib


# Copy Shiro (and dependencies) for authentication
RUN \
  curl -kLs $SHIRO_CORE_URI      -o $SRC_HOME/lib/$shiro_core_version.jar && \
  curl -kLs $SHIRO_WEB_URI       -o $SRC_HOME/lib/$shiro_web_version.jar && \
  curl -kLs $SPLICE_SHIRO_URI    -o $SRC_HOME/lib/$splice_shiro_version.jar && \
  curl -kls https://repo1.maven.org/maven2/org/slf4j/slf4j-jdk14/1.7.28/slf4j-jdk14-1.7.28.jar -o $SRC_HOME/lib/slf4j-log4j12-1.7.28.jar && \
  curl -kls https://repo1.maven.org/maven2/org/slf4j/slf4j-jdk14/1.7.28/slf4j-jdk14-1.7.28-sources.jar -o $SRC_HOME/lib/slf4j-log4j12-1.7.28-sources.jar && \
  curl -kls https://repo1.maven.org/maven2/org/slf4j/slf4j-jdk14/1.7.28/slf4j-jdk14-1.7.28-javadoc.jar -o $SRC_HOME/lib/slf4j-log4j12-1.7.28-javadoc.jar && \
  curl -kLs http://repository.splicemachine.com/nexus/content/groups/public/com/splicemachine/db-client/$sm_version/db-client-$sm_version.jar -o $SRC_HOME/lib/db-client.jar && \
  curl -kLs http://repository.splicemachine.com/nexus/content/groups/public/com/splicemachine/db-client/$sm_version/db-client-$sm_version-sources.jar -o $SRC_HOME/lib/db-client-sources.jar

ENV CLASSPATH=$SRC_HOME/lib/*:/usr/local/share/py4j/py4j0.10.8.1.jar

COPY shared /tmp/shared
RUN pip3 install /tmp/shared/.
# Copy Source into Container
COPY feature_store/src $SRC_HOME
CMD $SRC_HOME"/scripts/entrypoint.sh"

