ARG splice_base_image_version=0.0.2
FROM splicemachine/sm_k8_base:$splice_base_image_version

ARG sm_version=3.0.0.1961
ARG spark_version=2.4.4

ENV kafka_client_version=2.2.1-cdh6.3.0
ENV db_client_version=3.0.0.1961
ENV nsds_version=3.0.0.1961-shaded-cdh6.3.0

# Jar Locations
ARG NSDS_JAR_URL=https://splice-releases.s3.amazonaws.com/$db_client_version/cluster/nsds/splice_spark2-$nsds_version.jar
ARG DB_CLIENT_JAR_URL=http://repository.splicemachine.com/nexus/content/groups/public/com/splicemachine/db-client/$db_client_version/db-client-$db_client_version.jar
ARG KAFKA_CLIENT_JAR_URL=https://repository.cloudera.com/artifactory/cloudera-repos/org/apache/kafka/kafka-clients/$kafka_client_version/kafka-clients-$kafka_client_version.jar

ENV SRC_HOME=/opt/init
ENV BUILD_HOME=/tmp

ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

COPY requirements.txt $BUILD_HOME/requirements.txt

# Install Apk Packages
RUN yum -y update && \
    yum install -y git curl python3 python3-pip

# Install Python Package
RUN pip3 install --upgrade wheel pip && \
    pip3 install -r $BUILD_HOME/requirements.txt

# Install Jars
RUN mkdir -p $SRC_HOME/spark_jars && \
    mkdir -p $SRC_HOME/kafka_jars && \
    curl -kLs $NSDS_JAR_URL -o $SRC_HOME/spark_jars/splice_spark2-$nsds_version.jar && \
    curl -kLs $DB_CLIENT_JAR_URL -o $SRC_HOME/kafka_jars/db-client-$db_client_version.jar && \
    curl -kLs $KAFKA_CLIENT_JAR_URL -o $SRC_HOME/kafka_jars/kafka-clients-$kafka_client_version.jar


COPY ./entrypoint.py /entrypoint.py

CMD ["python3", "/entrypoint.py"]