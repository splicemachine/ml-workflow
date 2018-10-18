NAME = splicemachine/mlflow
VERSION = 7.0.0
SPLICE_BASE_IMAGE_VERSION = 0.0.4
STATIC_BASE_ARTIFACT_URI = https://s3.amazonaws.com/splicemachine/artifacts

GITTAG=$(shell git describe --tags)

ifeq ($(GITTAG), )
GITTAG=$(shell git describe --tags --dirty --always | sed 's/-dirty//' | sed 's/-/_/g')
endif

.PHONY: all build clean push realclean

all: build

run:
	docker run $(NAME):$(VERSION)

build:
	docker build --rm -t $(NAME):$(VERSION) .
	docker tag $(NAME):$(VERSION) $(NAME):latest
	docker tag $(NAME):$(VERSION) $(NAME):$(GITTAG)

build:
	docker build --rm --build-arg splice_base_image_version=${SPLICE_BASE_IMAGE_VERSION} \
      --build-arg static_base_artifact_uri=${STATIC_BASE_ARTIFACT_URI} -t $(NAME):$(VERSION) .
	docker tag $(NAME):$(VERSION) $(NAME):latest
	docker tag $(NAME):$(VERSION) $(NAME):$(GITTAG)


push:
	docker push $(NAME):latest
	docker push $(NAME):$(VERSION)
	docker push $(NAME):$(GITTAG)

clean:
	-docker rmi $(NAME):latest
	-docker rmi $(NAME):$(VERSION)
	-docker rmi $(NAME):$(GITTAG)

realclean:
	-docker kill $(shell docker ps -q)
	-docker rm $(shell docker ps -a -q)
	-docker rmi $(shell docker images -q)
	-docker system prune -f -a

