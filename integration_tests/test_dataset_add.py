from datacube.utils import SimpleDocNav
from datacube.testutils import gen_dataset_test_dag, load_dataset_definition


def test_dataset_add(dataset_add_configs, index_empty, clirunner):
    p = dataset_add_configs
    index = index_empty
    r = clirunner(['dataset', 'add', p.datasets], expect_success=False)
    assert r.exit_code != 0
    assert 'Found no products' in r.output

    clirunner(['metadata_type', 'add', p.metadata], expect_success=True)
    clirunner(['product', 'add', p.products], expect_success=True)
    clirunner(['dataset', 'add', p.datasets], expect_success=True)
    clirunner(['dataset', 'add', p.datasets_bad1], expect_success=False)

    ds = load_dataset_definition(p.datasets)
    ds_bad1 = load_dataset_definition(p.datasets_bad1)

    r = clirunner(['dataset', 'search'], expect_success=True)
    assert ds.id in r.output
    assert ds_bad1.id not in r.output
    assert ds.sources['ab'].id in r.output
    assert ds.sources['ac'].sources['cd'].id in r.output

    ds_ = SimpleDocNav(gen_dataset_test_dag(1, force_tree=True))
    assert ds_.id == ds.id

    x = index.datasets.get(ds.id, include_sources=True)
    assert str(x.sources['ab'].id) == ds.sources['ab'].id
    assert str(x.sources['ac'].sources['cd'].id) == ds.sources['ac'].sources['cd'].id
