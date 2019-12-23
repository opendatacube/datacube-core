image_name := opendatacube/datacube-tests
image_tag := latest
dkr := $(image_name):$(image_tag)

all: docker

dist/datacube.tar.gz: ../setup.py
	(cd .. && python3 setup.py sdist --dist-dir ./docker/dist/)
	(cd dist && find . -type f -exec ln -sf '{}' datacube.tar.gz ';')

docker: dist/datacube.tar.gz
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

clean:
	rm dist/*

PHONY: all clean docker check debug
