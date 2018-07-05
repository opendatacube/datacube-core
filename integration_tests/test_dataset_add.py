import yaml
import toolz

from datacube.utils import SimpleDocNav
from datacube.testutils import gen_dataset_test_dag, load_dataset_definition, write_files


def check_skip_lineage_test(clirunner, index):
    ds = SimpleDocNav(gen_dataset_test_dag(11, force_tree=True))

    prefix = write_files({'agdc-metadata.yml': yaml.safe_dump(ds.doc)})

    clirunner(['dataset', 'add', '--confirm-ignore-lineage', '--product', 'A', str(prefix)])

    ds_ = index.datasets.get(ds.id, include_sources=True)
    assert ds_ is not None
    assert str(ds_.id) == ds.id
    assert ds_.sources == {}

    assert index.datasets.get(ds.sources['ab'].id) is None
    assert index.datasets.get(ds.sources['ac'].id) is None
    assert index.datasets.get(ds.sources['ae'].id) is None
    assert index.datasets.get(ds.sources['ac'].sources['cd'].id) is None


def check_no_product_match(clirunner, index):
    ds = SimpleDocNav(gen_dataset_test_dag(22, force_tree=True))

    prefix = write_files({'agdc-metadata.yml': yaml.safe_dump(ds.doc)})

    r = clirunner(['dataset', 'add',
                   '--product', 'A',
                   str(prefix)])
    assert 'ERROR Dataset metadata did not match product signature' in r.output

    r = clirunner(['dataset', 'add',
                   '--product', 'A',
                   '--product', 'B',
                   str(prefix)])
    assert 'ERROR No matching Product found for dataset' in r.output

    ds_ = index.datasets.get(ds.id, include_sources=True)
    assert ds_ is None

    # Ignore lineage but fail to match main dataset
    r = clirunner(['dataset', 'add',
                   '--product', 'B',
                   '--confirm-ignore-lineage',
                   str(prefix)])

    assert 'ERROR Dataset metadata did not match product signature' in r.output
    assert index.datasets.has(ds.id) is False


def check_with_existing_lineage(clirunner, index):
    """
      A -> B
      |    |
      |    v
      +--> C -> D
      |
      +--> E

    Add nodes BCE(D) with auto-matching, then add node A with product restricted to A only.
    """

    ds = SimpleDocNav(gen_dataset_test_dag(33, force_tree=True))

    child_docs = [ds.sources[x].doc for x in ('ab', 'ac', 'ae')]

    prefix = write_files({'lineage.yml':
                          yaml.safe_dump_all(child_docs),
                          'main.yml':
                          yaml.safe_dump(ds.doc),
                          })

    clirunner(['dataset', 'add', str(prefix/'lineage.yml')])
    assert index.datasets.get(ds.sources['ae'].id) is not None
    assert index.datasets.get(ds.sources['ab'].id) is not None
    assert index.datasets.get(ds.sources['ac'].id) is not None

    clirunner(['dataset', 'add',
               '--no-auto-add-lineage',
               '--product', 'A',
               str(prefix/'main.yml')])

    assert index.datasets.get(ds.id) is not None


def check_inconsistent_lineage(clirunner, index):
    """
      A -> B
      |    |
      |    v
      +--> C -> D
      |
      +--> E

    Add node E,
    then try adding A with modified E in the lineage, should fail to add ABCD
    """
    ds = SimpleDocNav(gen_dataset_test_dag(1313, force_tree=True))

    child_docs = [ds.sources[x].doc for x in ('ae',)]
    modified_doc = toolz.assoc_in(ds.doc, 'lineage.source_datasets.ae.label'.split('.'), 'modified')

    prefix = write_files({'lineage.yml':
                          yaml.safe_dump_all(child_docs),
                          'main.yml':
                          yaml.safe_dump(modified_doc),
                          })

    clirunner(['dataset', 'add', str(prefix/'lineage.yml')])
    assert index.datasets.get(ds.sources['ae'].id) is not None

    r = clirunner(['dataset', 'add',
                   str(prefix/'main.yml')])

    assert 'ERROR Inconsistent lineage dataset' in r.output

    assert index.datasets.get(ds.id) is None
    assert index.datasets.get(ds.sources['ab'].id) is None
    assert index.datasets.get(ds.sources['ac'].id) is None
    assert index.datasets.get(ds.sources['ac'].sources['cd'].id) is None


def check_missing_lineage(clirunner, index):
    """
      A -> B
      |    |
      |    v
      +--> C -> D
      |
      +--> E

    Use --no-auto-add-lineage
    """
    ds = SimpleDocNav(gen_dataset_test_dag(44, force_tree=True))
    child_docs = [ds.sources[x].doc for x in ('ae', 'ab', 'ac')]

    prefix = write_files({'lineage.yml': yaml.safe_dump_all(child_docs),
                          'main.yml': yaml.safe_dump(ds.doc),
                          })

    r = clirunner(['dataset', 'add',
                   '--no-auto-add-lineage',
                   str(prefix/'main.yml')])

    assert 'ERROR Following lineage datasets are missing' in r.output
    assert index.datasets.has(ds.id) is False

    # now add lineage and try again
    clirunner(['dataset', 'add', str(prefix/'lineage.yml')])
    assert index.datasets.has(ds.sources['ae'].id)
    r = clirunner(['dataset', 'add',
                   '--no-auto-add-lineage',
                   str(prefix/'main.yml')])

    assert index.datasets.has(ds.id)


def check_missing_metadata_doc(clirunner):
    prefix = write_files({'im.tiff': ''})
    r = clirunner(['dataset', 'add', str(prefix/'im.tiff')])
    assert "ERROR No supported metadata docs found for dataset" in r.output


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

    check_skip_lineage_test(clirunner, index)
    check_no_product_match(clirunner, index)
    check_with_existing_lineage(clirunner, index)
    check_inconsistent_lineage(clirunner, index)
    check_missing_metadata_doc(clirunner)
    check_missing_lineage(clirunner, index)

    # check --product=nosuchproduct
    r = clirunner(['dataset', 'add', '--product', 'nosuchproduct', p.datasets],
                  expect_success=False)

    assert "ERROR Supplied product name" in r.output
    assert r.exit_code != 0
