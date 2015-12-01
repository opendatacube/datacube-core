#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu

# Check all files other than 'compat.py'.
#   -> compat.py contains both Python 2 and 3 syntax, so will fail lint checks.
export py_files="datacube examples/*.py"

pylint --reports no ${py_files}

pep8 --max-line-length 120 ${py_files}

# Check for basic Python 3 incompatiblities.
pylint --py3k --reports no ${py_files}

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
py.test --cov datacube datacube examples tests $@

