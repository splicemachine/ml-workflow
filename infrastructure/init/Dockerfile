ARG splice_base_image_version=0.0.2
FROM splicemachine/sm_k8_base:$splice_base_image_version


ENV SRC_HOME=/opt/init
ENV BUILD_HOME=/tmp

ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

COPY requirements.txt $BUILD_HOME/requirements.txt

# Install Apk Packages
RUN yum -y update && \
    yum install -y gcc gcc-c++ unixODBC-devel unixODBC python3 python3-devel python3-pip

# Install Python Package
RUN pip3 install --upgrade wheel pip && \
    pip3 install -r $BUILD_HOME/requirements.txt

COPY ./entrypoint.py /entrypoint.py

CMD ["python3", "/entrypoint.py"]
