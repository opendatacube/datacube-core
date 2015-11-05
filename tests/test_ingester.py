from __future__ import print_function, absolute_import

import pytest

from datacube.storage.ingester import run_ingest


# Mark as expected to fail (for now)
@pytest.mark.xfail
def test_ingest(tmpdir):
    print(tmpdir)

    storage_config = 'data/storage_config.yaml'
    ingest_config = 'data/ingest_config.yaml'

    run_ingest()

    assert 0

