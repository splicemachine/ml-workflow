FROM frolvlad/alpine-miniconda3

ENV DEBUG true

# install apk dependencies
RUN apk add bash rsync openjdk8 gcc g++ && \
    cd /usr/lib/jvm/java-1.8-openjdk/jre/lib/amd64/ && \
    ln -s server/libjvm.so .


WORKDIR /api

# Disable IPv6
# https://www.itzgeek.com/how-tos/linux/centos-how-tos/how-do-i-disable-ipv6-on-centos-7-rhel-7.html
COPY config/sysctl.conf /etc/sysctl.conf
RUN sysctl -p

# make it so we don't have to reinstall this every time we change our source code
COPY utilities/requirements.txt /api
RUN pip install -r requirements.txt

COPY . /api

# run the app server
CMD ["/api/utilities/entrypoint.sh"]
 # run it
