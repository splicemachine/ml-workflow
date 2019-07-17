ARG splice_base_image_version
FROM splicemachine/splice_base:$splice_base_image_version

LABEL maintainer="nnygaard@splicemachine.com,bepstein@splicemachine.com,abaveja@splicemachine.com"

# Docker in Docker Configuration
ARG dind_commit='52379fa76dee07ca038624d639d9e14f4fb719ff'
ENV DIND_ARTIFACT='https://raw.githubusercontent.com/docker/docker/'$dind_commit'/hack/dind'

# ODBC Driver Configuration
ARG odbc_driver_version='2.7.63.0'
ENV ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

# Defining Root Directory
ENV BOBBY_SRC_HOME=/opt/bobby
ENV BOBBY_BUILD_HOME=/tmp
ENV BOBBY_WORKER_HOME=/opt/worker

# where MLFlow Server stores Artifacts (relative) in S3
ENV MLFLOW_PERSIST_PATH='/artifacts'

# Add the list of necessary environment packages for development.
COPY build $BOBBY_BUILD_HOME

# Add the repository for docker and IUS Repo fo Python3
RUN yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo && \
    yum install -y https://centos7.iuscommunity.org/ius-release.rpm

# Install development packages.
RUN yum clean all && \
    rm -rf /var/cache/yum && \
    yum -y install $(cat /$BOBBY_BUILD_HOME/yum_packages.txt)

# Install ODBC Driver
RUN cd $BOBBY_BUILD_HOME && \
    wget -O splice_odbc.tar.gz $ODBC_DRIVER_ARTIFACT && \
    tar xzf splice_odbc.tar.gz && \
    cd splice_odbc_linux64-${odbc_driver_version} && \
    sh ./install.sh -q


# Install Python dependencies
RUN pip3.6 install -r $BOBBY_BUILD_HOME/requirements.txt

# Install Dind
RUN wget -O /usr/local/bin/dind "${DIND_ARTIFACT}" && \
	chmod a+x /usr/local/bin/dind

# Clean up Build Dependencies and setup src directories
RUN rm -r $BOBBY_BUILD_HOME/* && \
    mkdir -p "$BOBBY_SRC_HOME" && \
    mkdir -p "$BOBBY_WORKER_HOME"

COPY src $BOBBY_SRC_HOME

# make the entrypoint executable by all users
RUN chmod a+x $BOBBY_SRC_HOME/scripts/entrypoint.sh && \
    chmod a+x $BOBBY_SRC_HOME/scripts/run_dind.sh

VOLUME /var/lib/docker
EXPOSE 2375

CMD $BOBBY_SRC_HOME"/scripts/entrypoint.sh"
