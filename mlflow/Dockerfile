ARG splice_base_image_version
FROM splicemachine/splice_base:$splice_base_image_version

# ODBC Driver Configuration
ARG odbc_driver_version='2.7.63.0'
ENV ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

# Defining Root Directory
ENV MLFLOW_SRC_HOME=/opt/mlflow
ENV MLFLOW_BUILD_HOME=/tmp

COPY build $MLFLOW_BUILD_HOME

# Install IUS Repo for Python3.6
RUN yum install -y https://centos7.iuscommunity.org/ius-release.rpm

# Install development packages.
RUN yum clean all && \
    rm -rf /var/cache/yum && \
    yum -y install $(cat /$MLFLOW_BUILD_HOME/yum_packages.txt

# Install ODBC Driver
RUN cd $MLFLOW_BUILD_HOME && \
    wget -O splice_odbc.tar.gz $ODBC_DRIVER_ARTIFACT && \
    tar xzf splice_odbc.tar.gz && \
    cd splice_odbc_linux64-${odbc_driver_version} && \
    sh ./install.sh -q

# Install Python dependencies
RUN pip3.6 install -r $MLFLOW_BUILD_HOME/requirements.txt

# Remove Build Directories and Create source directory
RUN rm -rf $MLFLOW_BUILD_HOME/* && \
    mkdir -p $MLFLOW_SRC_HOME

COPY src $MLFLOW_SRC_HOME

# run the app server
CMD $MLFLOW_SRC_HOME"/scripts/entrypoint.sh"
 # run it

