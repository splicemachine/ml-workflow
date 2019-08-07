ARG splice_base_image_version
FROM splicemachine/splice_base:$splice_base_image_version

LABEL maintainer="nnygaard@splicemachine.com,bepstein@splicemachine.com,abaveja@splicemachine.com"

# ml-workflow-lib Configuration

######################################################################
# TODO @AMRIT CHANGE THIS TO A RELEASE NUMBER BEFORE THIS IS RELEASE #
######################################################################

ARG github_PAT
ENV ML_WORKFLOW_LIB_PACKAGE="git+https://$github_PAT:@github.com/splicemachine/ml-workflow-lib.git"

# Docker in Docker Configuration
ARG dind_commit='52379fa76dee07ca038624d639d9e14f4fb719ff'
ENV DIND_ARTIFACT='https://raw.githubusercontent.com/docker/docker/'$dind_commit'/hack/dind'

# ODBC Driver Configuration
ARG odbc_driver_version='2.7.63.0'
ENV ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

# Defining Directories
ENV SRC_HOME=/opt/bobby
ENV BUILD_HOME=/tmp
ENV WORKER_HOME=/opt/worker

# Bug w/ click
ENV LC_ALL en_US.utf-8
ENV LANG en_US.utf-8

# Add the list of necessary environment packages for development.
COPY build $BUILD_HOME

# Add the repository for docker and IUS Repo fo Python3
RUN yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo && \
    yum install -y https://centos7.iuscommunity.org/ius-release.rpm

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
RUN echo 333ffdd
# Install Python dependencies
RUN pip3.6 install $ML_WORKFLOW_LIB_PACKAGE && \
    pip3.6 install -r $BUILD_HOME/requirements.txt

# Install Dind
RUN wget -O /usr/local/bin/dind "${DIND_ARTIFACT}" && \
	chmod a+x /usr/local/bin/dind

# Clean up Build Dependencies and setup src directories
RUN rm -r $BUILD_HOME/* && \
    mkdir -p "$SRC_HOME" && \
    mkdir -p "$WORKER_HOME"

COPY src $SRC_HOME

# make the entrypoint executable by all users
RUN chmod a+x $SRC_HOME/scripts/entrypoint.sh && \
    chmod a+x $SRC_HOME/scripts/run_dind.sh

VOLUME /var/lib/docker
EXPOSE 2375

CMD $SRC_HOME"/scripts/entrypoint.sh"
