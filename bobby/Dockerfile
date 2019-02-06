ARG splice_base_image_version
FROM splicemachine/splice_base:$splice_base_image_version

LABEL maintainer="nnygaard@splicemachine.com,bepstein@splicemachine.com"

ARG pysplice_version
ARG dind_commit='52379fa76dee07ca038624d639d9e14f4fb719ff'

ENV DIND_ARTIFACT="https://raw.githubusercontent.com/docker/docker/${dind_commit}/hack/dind"
ENV PYSPLICE_VERSION=${pysplice_version}

VOLUME /var/lib/docker
EXPOSE 2375

# Add the list of necessary environment packages for development.
COPY ./yum_packages.txt /tmp/yum_packages.txt
COPY ./target/splicemachine-jars/* $SPARK_HOME/jars/
COPY ./requirements.txt /tmp/requirements.txt

# Add the repository for docker
RUN yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo

# Install development packages.
RUN yum clean all && \
    rm -rf /var/cache/yum && \
    yum -y install $(cat /tmp/yum_packages.txt)

## Install python2.7 and corresponding pip
RUN \
  curl --silent --show-error https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && \
  python /tmp/get-pip.py && \
  rm -rf /var/cache/yum && yum clean all

# Install Python dependencies
RUN pip install -r /tmp/requirements.txt

# Install pysplice
RUN \
  wget --no-check-certificate https://github.com/splicemachine/pysplice/archive/${PYSPLICE_VERSION}.zip && \
  unzip -o ${PYSPLICE_VERSION}.zip && \
  cd pysplice-${PYSPLICE_VERSION} && \
  pip install .

# Install Dind
RUN wget -O /usr/local/bin/dind "${DIND_ARTIFACT}" && \
	chmod a+x /usr/local/bin/dind

# Copy files
RUN mkdir -p /bob
COPY . /bob

# make the entrypoint executable by all users
RUN chmod a+x /bob/utilities/entrypoint.sh && \
    chmod a+x /bob/utilities/run_dind.sh && \
    unzip /bob/utilities/mlflow.zip && \
    cat /bob/goodies/bobby.txt

# we have to copy the mlflow binary because MLFlow has TONS of
# problems with their Java Artifact Store in Maven which effect deployment.
# this way is much easier

# do good stuff
CMD ["/bob/utilities/entrypoint.sh"]
