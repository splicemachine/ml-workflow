FROM centos:7
MAINTAINER Amrit Baveja <abaveja@splicemachine.com>

COPY utilities/requirements.txt /tmp/requirements.txt

RUN yum --enablerepo=extras install -y epel-release bash && \
    yum install -y python python-devel python-pip && \
    pip install -r /tmp/requirements.txt && \
    mkdir -p /app

COPY . /app
RUN chmod a+x /app/utilities/entrypoint.sh

CMD ["/app/utilities/entrypoint.sh"]
