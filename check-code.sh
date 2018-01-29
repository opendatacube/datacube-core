#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

pep8 tests integration_tests examples utils --max-line-length 120

pylint -j 2 --reports no datacube datacube_apps

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
pytest -vv -r sx --cov datacube --doctest-ignore-import-errors --durations=5 datacube tests datacube_apps $@

