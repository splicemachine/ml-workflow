FROM centos:7
MAINTAINER abaveja@splicemachine.com

ENV SPLICEMACHINE_PY_VERSION='6.0'
ARG sm_version=2.7.0.1828
ARG dbaas_build=1.0.1533
ARG spark_version=2.2.2
ARG hadoop_version=2.6
ARG cdh_version=5.14.0

ENV DIND_COMMIT='52379fa76dee07ca038624d639d9e14f4fb719ff'

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
