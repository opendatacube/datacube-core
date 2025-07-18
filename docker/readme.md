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
  -v $(pwd):/code -w /code \
  -it opendatacube/datacube-tests \
  bash -c "uv pip compile --all-extras --group dev --group doc --output-file=docker/constraints.txt pyproject.toml"
```
