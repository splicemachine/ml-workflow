ARG splice_base_image_version
FROM splicemachine/splice_base:$splice_base_image_version

ENV DEBUG true

#https://github.com/mbaltrusitis/dockerfile-centos-buildpack-deps/blob/master/7/Dockerfile
RUN yum -y update && yum -y groupinstall 'Development Tools' && yum -y install \
    kernel-devel \
    ImageMagick-devel \
    bzip2-devel \
    libcurl \
    libcurl-devel \
    openssl-devel \
    libevent-devel \
    libffi-devel \
    glib2-devel \
    libjpeg-devel \
    mysql-devel \
    postgresql-devel \
    ncurses-devel \
    readline \
    readline-devel \
    sqlite-devel \
    openssl \
    openssl-devel \
    libxml2-devel \
    libxslt-devel \
    zlib-devel \
    libyaml-devel \
    wget \
    && yum clean all

RUN yum -y update && yum install -y \
    bzr \
    mercurial \
    && yum clean all

RUN yum -y update && yum install -y \
    tar \
&& yum clean all


#https://github.com/mbaltrusitis/dockerfile-centos-python/blob/master/3.5/Dockerfile
# http://bugs.python.org/issue19846
# > At the moment, setting "LANG=C" on a Linux system *fundamentally breaks Python 3*, and that's not OK.
#ENV LANG C.UTF-8
ENV LC_ALL en_US.utf-8
ENV LANG en_US.utf-8
#export LC_ALL=en_US.utf-8
#export LANG=en_US.utf-8

# gpg: key F73C700D: public key "Larry Hastings <larry@hastings.org>" imported
RUN gpg --keyserver ha.pool.sks-keyservers.net --recv-keys 97FC712E4C024BBEA48A61ED3A5CA953F73C700D

ENV PYTHON_VERSION 3.5.3

# if this is called "PIP_VERSION", pip explodes with "ValueError: invalid truth value '<VERSION>'"
ENV PYTHON_PIP_VERSION 9.0.1

RUN set -x \
    && mkdir -p /usr/src/python \
    && curl -SL "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_VERSION.tar.xz" -o python.tar.xz \
    && curl -SL "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_VERSION.tar.xz.asc" -o python.tar.xz.asc \
    && gpg --verify python.tar.xz.asc \
    && tar -xJC /usr/src/python --strip-components=1 -f python.tar.xz \
    && rm python.tar.xz* \
    && cd /usr/src/python \
    && ./configure --enable-shared \
    && make -j$(nproc) && make altinstall \
    && echo "/usr/local/lib/" > /etc/ld.so.conf.d/local.conf && ldconfig \
    && pip3.5 install --no-cache-dir --upgrade --ignore-installed pip==$PYTHON_PIP_VERSION \
    && find /usr/local \
        \( -type d -a -name test -o -name tests \) \
        -o \( -type f -a -name '*.pyc' -o -name '*.pyo' \) \
        -exec rm -rf '{}' + \
    && rm -rf /usr/src/python

# make some useful symlinks that are expected to exist
RUN cd /usr/local/bin \
    && ln -s easy_install-3.5 easy_install3 \
    && ln -s idle3.5 idle3 \
    && ln -s pydoc3.5 pydoc3 \
    && ln -s python3.5 python3 \
&& ln -s python-config3.5 python-config3


#yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel

# install apk dependencies
#RUN apk add bash rsync openjdk8 gcc g++ && \
#    cd /usr/lib/jvm/java-1.8-openjdk/jre/lib/amd64/ && \
#    ln -s server/libjvm.so .
RUN yum install -y bash rsync java-1.8.0-openjdk java-1.8.0-openjdk-devel gcc g++
#RUN yum install -y bash rsync openjdk8 gcc g++ && \
#    ls -alt /usr/lib/jvm/java-1.8-openjdk/jre/lib/amd64/ && \
#    cd /usr/lib/jvm/java-1.8-openjdk/jre/lib/amd64/ && \
#    ln -s server/libjvm.so .


WORKDIR /api

# Disable IPv6
# https://www.itzgeek.com/how-tos/linux/centos-how-tos/how-do-i-disable-ipv6-on-centos-7-rhel-7.html
#COPY config/sysctl.conf /etc/sysctl.conf
#RUN sysctl -p

# make it so we don't have to reinstall this every time we change our source code
COPY utilities/requirements.txt /api
RUN pip install -r requirements.txt

COPY . /api

# run the app server
CMD ["/api/utilities/entrypoint.sh"]
 # run it
