#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu

pylint --reports no datacube examples/*.py version.py

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
py.test --cov datacube datacube examples tests $@

