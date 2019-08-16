# maybe change to a lighter image...
ARG splice_base_image_version
FROM splicemachine/splice_base:$splice_base_image_version

LABEL maintainer="nnygaard@splicemachine.com,bepstein@splicemachine.com,abaveja@splicemachine.com"

# ODBC Driver Configuration
ARG odbc_driver_version='2.7.63.0'
ENV ODBC_DRIVER_ARTIFACT='https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-'$odbc_driver_version'.tar.gz'

ENV BUILD_HOME=/build

# Spark Configuration
ARG spark_version='2.2.2'
ARG hadoop_version='2.6'
ENV SPARK_ARTIFACT='https://archive.apache.org/dist/spark/spark-'$spark_version'/spark-'$spark_version'-bin-hadoop'$hadoop_version'.tgz'

COPY build $BUILD_HOME

# Install Yum Packages
RUN yum install -y https://centos7.iuscommunity.org/ius-release.rpm && \
    yum clean all && \
    rm -rf /var/cache/yum && \
    yum -y install $(cat /$BUILD_HOME/yum_packages.txt)

# Install ODBC Driver
RUN cd $BUILD_HOME && \
    wget -O splice_odbc.tar.gz $ODBC_DRIVER_ARTIFACT && \
    tar xzf splice_odbc.tar.gz && \
    cd splice_odbc_linux64-${odbc_driver_version} && \
    bash ./install.sh -q

# Spark needs the user to be non-root or it throws an error

# Install Pip modules
RUN pip3.6 install -r $BUILD_HOME/requirements.txt && \
    pip3.6 install 'pyspark=='$spark_version

# Delete Build Dependencies
RUN rm -rf $BUILD_HOME

USER jovyan
EXPOSE 8888

CMD jupyter notebook
