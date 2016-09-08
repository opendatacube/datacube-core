#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

pep8 tests integration_tests --max-line-length 120

pylint -j 2 --reports no datacube examples datacube_apps

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
py.test -r sx --cov datacube --durations=5 datacube tests datacube_apps $@

