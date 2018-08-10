FROM docker:dind

MAINTAINER Amrit Baveja <abaveja@splicemachine.com>

# install apk dependencies and get openjdk to work.
RUN echo "http://dl-8.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories && \
    apk add bash rsync openjdk8 gcc gfortran python python-dev py-pip build-base wget \
    freetype-dev libpng-dev openblas-dev && \
    ln -s /usr/include/locale.h /usr/include/xlocale.h && \
    cd /usr/lib/jvm/java-1.8-openjdk/jre/lib/amd64/ && \
    ln -s server/libjvm.so .

# install python dependencies
COPY utilities/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# create a directory for storing app data
RUN mkdir -p /bob
COPY ./ /bob/

# make the entrypoint executable by all users
RUN chmod a+x /bob/utilities/entrypoint.sh && \
    cat /bob/goodies/bobby.txt

WORKDIR '/'

CMD ["/bob/utilities/entrypoint.sh"]
