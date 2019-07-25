ARG splice_base_image_version
FROM splicemachine/splice_base:$splice_base_image_version

# ml-workflow-lib Configuration
ARG github_PAT
ENV ML_WORKFLOW_LIB_PACKAGE="git+https://$github_PAT:@github.com/splicemachine/ml-workflow-lib.git"

# ODBC Driver Configuration
ARG odbc_driver_version='2.7.63.0'
ENV ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

# Defining Root Directory
ENV SRC_HOME=/opt/mlflow
ENV BUILD_HOME=/tmp

# Bug w/ click
ENV LC_ALL en_US.utf-8
ENV LANG en_US.utf-8

COPY build $BUILD_HOME

# Install IUS Repo for Python3.6
RUN yum install -y https://centos7.iuscommunity.org/ius-release.rpm

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
RUN echo ssssddeedddd
# Install Python dependencies
COPY splice_mlflow_plugin $BUILD_HOME/splice_mlflow_plugin
RUN pip3.6 install -r $BUILD_HOME/requirements.txt && \
    pip3.6 install $ML_WORKFLOW_LIB_PACKAGE && \
    pip3.6 install $BUILD_HOME/splice_mlflow_plugin

# Remove Build Directories and Create source directory
RUN rm -rf $BUILD_HOME/* && \
    mkdir -p $SRC_HOME

COPY src $SRC_HOME

CMD $SRC_HOME"/scripts/entrypoint.sh"