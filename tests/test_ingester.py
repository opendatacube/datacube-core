from __future__ import print_function, absolute_import

from ingester_cli import main


def test_ingest(tmpdir):
    print(tmpdir)
    main()
    assert 0