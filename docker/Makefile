image_name := opendatacube/datacube-tests
image_tag := latest
dkr := $(image_name):$(image_tag)

all: docker

dist/datacube.whl: ../setup.py
	(cd .. && python3 setup.py bdist_wheel --dist-dir ./docker/dist/)
	(cd dist && find . -type f -name "datacube*whl" -exec ln -sf '{}' datacube.whl ';')

docker: dist/datacube.whl
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
