from datacube.utils import read_documents, SimpleDocNav


def test_dataset_add(dataset_add_configs, index_empty, clirunner):
    p = dataset_add_configs

    clirunner(['metadata_type', 'add', p.metadata], expect_success=True)
    clirunner(['product', 'add', p.products], expect_success=True)
    clirunner(['dataset', 'add', p.datasets], expect_success=True)

    r = clirunner(['dataset', 'search'], expect_success=True)
    ds, *_ = list(SimpleDocNav(d) for _, d in read_documents(p.datasets))
    assert ds.id in r.output
    assert ds.sources['ab'].id in r.output
    assert ds.sources['ac'].sources['cd'].id in r.output
