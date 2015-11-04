from __future__ import print_function, absolute_import

from datacube.ingester.ingester import run_ingest


def test_ingest(tmpdir):
    print(tmpdir)

    storage_config = 'data/storage_config.yaml'
    ingest_config = 'data/ingest_config.yaml'

    run_ingest()

    assert 0

