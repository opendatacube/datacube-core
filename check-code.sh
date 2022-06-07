#!/usr/bin/env bash
# Convenience script for running CI-like checks.

set -eu
set -x

if [ "${1:-}" == "--with-docker" ]; then
    shift

    # Do `docker run -ti` only if running on a TTY
    if [ -t 0 ]; then
        ti="-ti"
    else
        ti=""
    fi

    exec docker run $ti \
         -e SKIP_STYLE_CHECK="${SKIP_STYLE_CHECK:-no}" \
         -v $(pwd):/code \
         opendatacube/datacube-tests:latest \
         $0 $@
fi

if [ "${SKIP_STYLE_CHECK:-no}" != "yes" ]; then
    pycodestyle tests integration_tests examples --max-line-length 120
    pylint -j 2 --reports no datacube
fi

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
pytest -r a \
       --cov datacube \
       --doctest-ignore-import-errors \
       --durations=5 \
       datacube \
       tests \
       $@

set +x

# Optionally validate example yaml docs.
if which yamllint;
then
    set -x
    yamllint $(find . \( -iname '*.yaml' -o -iname '*.yml' \) )
    set +x
fi
