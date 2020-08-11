ARG splice_base_image_version=0.0.2
FROM splicemachine/sm_k8_base:$splice_base_image_version

ARG sm_version=3.0.0.1961
ARG spark_version=2.4.4

ENV db_client_version=3.0.0.1969

# Jar Locations
ARG NSDS_JAR_URL=https://splicemachine.s3.amazonaws.com/artifacts/splice_spark-"$db_client_version"-SNAPSHOT-shaded.jar

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
RUN mkdir -p $SRC_HOME/jars && \
    curl -kLs $NSDS_JAR_URL -o $SRC_HOME/jars/nsds.jar

COPY ./entrypoint.py /entrypoint.py

CMD ["python3", "/entrypoint.py"]