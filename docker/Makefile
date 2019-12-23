image_name := opendatacube/datacube-tests
image_tag := latest
dkr := $(image_name):$(image_tag)

all: docker

docker:
	@echo Building $(dkr)
	docker build -t $(dkr) .

check:
	@docker run --rm -ti \
  -v "$$(dirname $$(pwd))":/src/datacube-core \
  $(dkr) \
  ./check-code.sh integration_tests/

debug:
	@docker run --rm -ti \
  -v "$$(dirname $$(pwd))":/src/datacube-core \
  $(dkr) \
  bash

PHONY: all clean docker check debug
