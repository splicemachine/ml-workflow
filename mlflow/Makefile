NAME = splicemachine/sm_k8_mlflow
VERSION = 0.1.10
SPLICE_BASE_IMAGE_VERSION = 0.0.2
SPLICE_BUILD=3.0.0.1946
ML_WORKFLOW_LIB_VERSION=0.0.9-dev
ODBC_DRIVER_VERSION=2.7.63.0

GITHUB_PAT=""

.PHONY: all build clean push realclean

all: build

run:
	docker run $(NAME):$(VERSION)

build:
	docker build --rm --build-arg splice_base_image_version=${SPLICE_BASE_IMAGE_VERSION} \
          --build-arg sm_version=${SPLICE_BUILD} --build-arg github_PAT=${GITHUB_PAT} --build-arg ml_workflow_lib_version=${ML_WORKFLOW_LIB_VERSION} \
          --build-arg odbc_driver_version=${ODBC_DRIVER_VERSION} -t $(NAME):$(VERSION) .
	docker tag $(NAME):$(VERSION) $(NAME):latest

push:
	docker push $(NAME):latest
	docker push $(NAME):$(VERSION)

clean:
	-docker rmi $(NAME):latest
	-docker rmi $(NAME):$(VERSION)

realclean:
	-docker kill $(shell docker ps -q)
	-docker rm $(shell docker ps -a -q)
	-docker rmi $(shell docker images -q)
	-docker system prune -f -a

