#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

if [ "${1:-}" == "--with-docker" ]; then
    shift
    exec docker run -ti \
         -v $(pwd):/src/datacube-core \
         opendatacube/datacube-tests:latest \
         $0 $@
fi

pycodestyle tests integration_tests examples utils --max-line-length 120

pylint -j 2 --reports no datacube datacube_apps

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
pytest -r a --cov datacube --doctest-ignore-import-errors --durations=5 datacube tests datacube_apps $@

set +x

# Optinally validate example yaml docs.
if which yamllint;
then
    set -x
    yamllint $(find . \( -iname '*.yaml' -o -iname '*.yml' \) )
    set +x
fi
