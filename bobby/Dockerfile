FROM centos:7
MAINTAINER abaveja@splicemachine.com

ENV SPLICEMACHINE_PY_VERSION='6.0'
ARG sm_version=2.7.0.1828
ARG dbaas_build=1.0.1533
ARG spark_version=2.2.2
ARG hadoop_version=2.6
ARG cdh_version=5.14.0

ENV DIND_COMMIT='52379fa76dee07ca038624d639d9e14f4fb719ff'
ENV SPARK_VERSION=$spark_version
ENV HADOOP_VERSION=$hadoop_version
ENV SPARK_HOME=/usr/local/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
ENV JAVA_HOME=/usr/local/jdk1.8.0_121
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH
ENV MESOS_NATIVE_JAVA_LIBRARY=/usr/local/libmesos-bundle/lib/libmesos.so
ENV LD_LIBRARY_PATH=/usr/local/libmesos-bundle/lib:/native:/usr/local/lib:$LD_LIBRARY_PATH
ENV SPARK_SUBMIT_LIBRARY_PATH="$SPARK_SUBMIT_LIBRARY_PATH:/native"
ENV SPARK_SUBMIT_CLASSPATH="$SPARK_CLASSPATH:$SPARK_SUBMIT_CLASSPATH"
ENV SPARK_CONF_PREFIX=$SPARK_HOME/conf/
ENV SPARK_CLASSPATH="$SPARK_CONF_PREFIX/core-site.xml;$SPARK_CONF_PREFIX/fairscheduler.xml;$SPARK_CONF_PREFIX/hbase-site.xml;$SPARK_CONF_PREFIX/hdfs-site.xml"

# install yum packages and fix default encoding so click doesn't throw a fit

RUN yum clean all && \
    rm -rf /var/cache/yum && \
    yum -y install epel-release curl && \
    yum -y install git openssl openssl-devel unzip rsync lsof python java-1.8.0-openjdk-devel \
    python-pip python-devel wget gcc gcc-c++ which bind-utils net-tools which nc jq unzip && \
    yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo && \
    yum -y install docker-ce

# install python dependencies
COPY utilities/requirements.txt /tmp/requirements.txt
COPY ./target/splicemachine-jars/* $SPARK_HOME/jars/

# Install Python Splice Machine Package
RUN pip install -r /tmp/requirements.txt && \
    wget https://github.com/splicemachine/pysplice/archive/$SPLICEMACHINE_PY_VERSION.zip && \
    unzip $SPLICEMACHINE_PY_VERSION.zip && \
    cd pysplice-$SPLICEMACHINE_PY_VERSION && \
    pip install .

RUN curl -kLs "https://s3.amazonaws.com/splicemachine/$sm_version-$dbaas_build/artifacts/hadoop-2.6.0-cdh$cdh_version.tar.gz" | tar -xz && \
    ln -s /hadoop-2.6.0-cdh$cdh_version /hadoop && \
    curl -kLs "https://s3.amazonaws.com/splicemachine/artifacts/cdh${cdh_version}-native.tgz" | tar -xz && \
    chown -R root:root /native && \
    curl -kLs "https://s3.amazonaws.com/splicemachine/artifacts/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" | tar -xz -C /usr/local && \
    curl -kLso $SPARK_HOME/jars/shiro-core-1.2.3.jar https://s3.amazonaws.com/splicemachine/artifacts/shiro-core-1.2.3.jar && \
    curl -kLso $SPARK_HOME/jars/shiro-web-1.2.3.jar https://s3.amazonaws.com/splicemachine/artifacts/shiro-web-1.2.3.jar && \
    curl -kLso $SPARK_HOME/jars/splice-shiro-$sm_version.jar https://s3.amazonaws.com/splicemachine/artifacts/splice-shiro-$sm_version.jar && \
    curl -kLs "https://s3.amazonaws.com/splicemachine/$sm_version-$dbaas_build/artifacts/splice-zeppelin-0.7.3-bin-all.tgz" | tar -xz -C /var/tmp zeppelin-0.7.3-bin-all/interpreter/spark/dep && \
    curl -kLs "https://s3.amazonaws.com/splicemachine/$sm_version-$dbaas_build/artifacts/jdk-8u121-linux-x64.tar.gz" | tar -xz -C /usr/local && \
    curl -kLs "https://s3.amazonaws.com/splicemachine/$sm_version-$dbaas_build/artifacts/libmesos-bundle-1.11.3.tar.gz" | tar -xz -C /usr/local && \
    curl -kLso $SPARK_HOME/jars/mesos-1.5.0-shaded-protobuf.jar "https://s3.amazonaws.com/splicemachine/$sm_version-$dbass_build/artifacts/mesos-1.5.0-shaded-protobuf.jar" && \
    cp /var/tmp/zeppelin-0.7.3-bin-all/interpreter/spark/dep/hbase_*.jar $SPARK_HOME/jars/ && \
    rm -f /var/tmp/zeppelin-0.7.3-bin-all/interpreter/spark/dep/* && \
    rm -f /usr/lib/python2.7/site-packages/pyspark/jars/mesos-1.0.0-shaded-protobuf.jar && \
    rm -f /usr/lib/python2.7/site-packages/pyspark/jars/spark-mesos_2.11-2.2.2.jar && \
    cp $SPARK_HOME/jars/mesos-1.5.0-shaded-protobuf.jar /usr/lib/python2.7/site-packages/pyspark/jars/mesos-1.5.0-shaded-protobuf.jar

VOLUME /var/lib/docker
EXPOSE 2375

RUN wget -O /usr/local/bin/dind "https://raw.githubusercontent.com/docker/docker/${DIND_COMMIT}/hack/dind" && \
	chmod a+x /usr/local/bin/dind

#RUN rm -rf /usr/local/spark-2.1.1-bin-hadoop2.6/jars/slf4j-log4j12-1.7.10.jar

# Copy files
RUN mkdir -p /bob
COPY . /bob
# make the entrypoint executable by all users
RUN chmod a+x /bob/utilities/entrypoint.sh && \
    chmod a+x /bob/utilities/run_dind.sh && \
    cat /bob/goodies/bobby.txt

# do good stuff
CMD ["/bob/utilities/entrypoint.sh"]
