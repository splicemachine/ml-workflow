NAME = splicemachine/bobby
VERSION = 0.0.10
SPLICE_BASE_IMAGE_VERSION = 0.0.4

GITTAG=$(shell git describe --tags)

ifeq ($(GITTAG), )
GITTAG=$(shell git describe --tags --dirty --always | sed 's/-dirty//' | sed 's/-/_/g')
endif

.PHONY: all build clean push realclean

all: build

run:
	docker run \
	-e "JDBC_URL=jdbc:splice://nikhilsaccount-ml.splicemachine-dev.io:1527/splicedb;ssl=basic;user=splice;password=admin;" \
	-e "AWS_ACCESS_KEY_ID=AKIAI2LKZ2P4DLZS7FZA" \
	-e "AWS_SECRET_ACCESS_KEY=" \
	-e "AWS_DEFAULT_REGION=us-east-2" \
	-e "BOBBY_PORT=2375" \
	-p 2375:2375 \
	-e "USER=splice" \
	-e "PASSWORD=admin" \
	-e "S3_BUCKET_NAME=s3://splice-demo/machine-learning/sync" \
	-e "FRAMEWORK_NAME=nikhilsaccount-ml-c1e6c0a1d1" \
	-e "SAGEMAKER_ROLE=arn:aws:iam::953558963498:role/splice-sagemaker" \
	$(NAME):$(VERSION)

build:
	docker \
		build \
		--rm \
		--build-arg splice_base_image_version=${SPLICE_BASE_IMAGE_VERSION} \
		--build-arg pysplice_version=${PYSPLICE_VERSION} \
		-t $(NAME):$(VERSION) \
		.
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

