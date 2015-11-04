from __future__ import print_function, absolute_import

from datacube.ingester.ingester_cli import main


def test_ingest(tmpdir):
    print(tmpdir)
    main()
    assert 0
