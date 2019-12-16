image_name := opendatacube/datacube-tests
image_tag := latest

all: docker

docker:
	@echo Building $(image_name):$(image_tag)
	docker build -t $(image_name):$(image_tag) .

debug: docker
	@docker run --rm -ti -v "$$(dirname $$(pwd))":/src/datacube-core $(image_name):$(image_tag) bash

PHONY: all clean docker
