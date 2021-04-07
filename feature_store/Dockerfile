ARG splice_base_image_version
FROM splicemachine/sm_k8_base:$splice_base_image_version

# Shiro config
ARG sm_version=3.1.0.2007
ARG shiro_core_version=shiro-core-1.2.3
ARG shiro_web_version=shiro-web-1.2.3
ARG splice_shiro_version=splice-shiro-$sm_version
ARG SHIRO_CORE_URI=https://splicemachine.s3.amazonaws.com/artifacts/$shiro_core_version.jar
ARG SHIRO_WEB_URI=https://splicemachine.s3.amazonaws.com/artifacts/$shiro_web_version.jar
ARG SPLICE_SHIRO_URI=https://splicemachine.s3.amazonaws.com/artifacts/$splice_shiro_version.jar

# ODBC Driver Configuration
ARG odbc_driver_version='2.8.74.0'
ARG ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

# Defining Root Directory
ENV SRC_HOME=/opt/feature_store
ENV BUILD_HOME=/tmp

# Bug w/ click
ENV LC_ALL en_US.utf-8
ENV LANG en_US.utf-8

COPY feature_store/build $BUILD_HOME

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
RUN pip3 install -q -r $BUILD_HOME/requirements.txt

# Remove Build Directories and Create source directory
RUN rm -rf $BUILD_HOME/* && \
    mkdir -p $SRC_HOME/lib

# Copy Shiro (and dependencies) for authentication
RUN \
  curl -kLs $SHIRO_CORE_URI      -o $SRC_HOME/lib/$shiro_core_version.jar && \
  curl -kLs $SHIRO_WEB_URI       -o $SRC_HOME/lib/$shiro_web_version.jar && \
  curl -kLs $SPLICE_SHIRO_URI    -o $SRC_HOME/lib/$splice_shiro_version.jar && \
  curl -kls https://repo1.maven.org/maven2/org/slf4j/slf4j-jdk14/1.7.28/slf4j-jdk14-1.7.28.jar -o $SRC_HOME/lib/slf4j-log4j12-1.7.28.jar && \
  curl -kls https://repo1.maven.org/maven2/org/slf4j/slf4j-jdk14/1.7.28/slf4j-jdk14-1.7.28-sources.jar -o $SRC_HOME/lib/slf4j-log4j12-1.7.28-sources.jar && \
  curl -kls https://repo1.maven.org/maven2/org/slf4j/slf4j-jdk14/1.7.28/slf4j-jdk14-1.7.28-javadoc.jar -o $SRC_HOME/lib/slf4j-log4j12-1.7.28-javadoc.jar && \
  curl -kls https://repo1.maven.org/maven2/javax/javaee-api/7.0/javaee-api-7.0.jar                     -o $SRC_HOME/lib/javaee-api-7.0.jar && \
  curl -kls https://repo1.maven.org/maven2/io/jsonwebtoken/jjwt-api/0.11.2/jjwt-api-0.11.2.jar -o $SRC_HOME/lib/jjwt-api-0.11.2.jar && \
  curl -kls https://repo1.maven.org/maven2/io/jsonwebtoken/jjwt-impl/0.11.2/jjwt-impl-0.11.2.jar -o $SRC_HOME/lib/jjwt-impl-0.11.2.jar && \
  curl -kls https://repo1.maven.org/maven2/io/jsonwebtoken/jjwt-jackson/0.11.2/jjwt-jackson-0.11.2.jar -o $SRC_HOME/lib/jjwt-jackson-0.11.2.jar && \
  curl -kls https://repo1.maven.org/maven2/org/glassfish/javax.json/1.1.4/javax.json-1.1.4.jar -o $SRC_HOME/lib/javax.json-1.1.4.jar && \
  curl -kLs http://repository.splicemachine.com/nexus/content/groups/public/com/splicemachine/db-client/$sm_version/db-client-$sm_version.jar -o $SRC_HOME/lib/db-client.jar && \
  curl -kLs http://repository.splicemachine.com/nexus/content/groups/public/com/splicemachine/db-client/$sm_version/db-client-$sm_version-sources.jar -o $SRC_HOME/lib/db-client-sources.jar

ENV CLASSPATH=$SRC_HOME/lib/*:/usr/local/share/py4j/py4j0.10.8.1.jar

RUN wget -q \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh

ENV PATH="${PATH}:/root/miniconda3/bin"

# Install Python dependencies
RUN conda install -c anaconda postgresql

RUN adduser postgres && \
    usermod -aG root postgres && \
    ln -s /root/miniconda3/bin/pg_ctl /usr/bin/pg_ctl && \
    alias pip3=/usr/bin/pip3 && \
    alias python3=/usr/bin/python3

COPY shared /tmp/shared

RUN pip3 install /tmp/shared/.

# Copy Source into Container
COPY feature_store/src $SRC_HOME

CMD $SRC_HOME"/scripts/entrypoint.sh"

