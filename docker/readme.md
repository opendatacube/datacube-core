Docker for running test suite
=============================

- Pushed to docker hub as `opendatacube/datacube-tests`

Example Use:

```shell
git clone https://github.com/opendatacube/datacube-core.git
cd datacube-core
docker run --rm \
  -v $(pwd):/code \
  opendatacube/datacube-tests:latest \
  ./check-code.sh integration_tests
```

## Updating Dependencies

Run the command:

```shell
docker run --rm \
  -v $(pwd):/code -w /code/docker \
  -it opendatacube/datacube-tests \
  bash -c "python3 -m pip install pip-tools && pip-compile --upgrade --output-file=constraints.txt --strip-extras constraints.in"
```
