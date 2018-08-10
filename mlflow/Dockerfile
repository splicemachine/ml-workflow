FROM frolvlad/alpine-miniconda3

ENV DEBUG true

# install apk dependencies
RUN apk add bash rsync openjdk8 gcc g++ && \
    cd /usr/lib/jvm/java-1.8-openjdk/jre/lib/amd64/ && \
    ln -s server/libjvm.so .


WORKDIR /api

# make it so we don't have to reinstall this every time we change our source code
COPY utilities/requirements.txt /api
RUN pip install -r requirements.txt

COPY . /api

# run the app server
CMD ["/tmp/entrypoint.sh"]
 # run it
