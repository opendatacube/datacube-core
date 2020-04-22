Docker for running test suite
=============================

- Pushed to docker hub as `opendatacube/datacube-tests`

Example Use:

```shell
git clone https://github.com/opendatacube/datacube-core.git
cd datacube-core
docker run --rm \
  -v $(pwd):/src/datacube-core \
  opendatacube/datacube-tests:latest \
  ./check-code.sh integration_tests
```
