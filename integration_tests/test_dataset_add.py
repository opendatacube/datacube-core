# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import math

import pytest
import toolz
import yaml

from datacube.index import Index
from datacube.index.hl import Doc2Dataset
from datacube.model import MetadataType
from datacube.testutils import gen_dataset_test_dag, load_dataset_definition, write_files, dataset_maker
from datacube.utils import SimpleDocNav
from datacube.scripts.dataset import _resolve_uri


def check_skip_lineage_test(clirunner, index):
    ds = SimpleDocNav(gen_dataset_test_dag(11, force_tree=True))

    prefix = write_files({'agdc-metadata.yml': yaml.safe_dump(ds.doc)})

    clirunner(['dataset', 'add', '--confirm-ignore-lineage', '--product', 'A', str(prefix)])

    ds_ = index.datasets.get(ds.id, include_sources=True)
    assert ds_ is not None
    assert ds_.id == ds.id
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
    assert 'ERROR' in r.output
    assert 'Dataset metadata did not match product signature' in r.output

    r = clirunner(['dataset', 'add',
                   '--product', 'A',
                   '--product', 'B',
                   str(prefix)])
    assert 'ERROR' in r.output
    assert 'No matching Product found for dataset' in r.output

    ds_ = index.datasets.get(ds.id, include_sources=True)
    assert ds_ is None

    # Ignore lineage but fail to match main dataset
    r = clirunner(['dataset', 'add',
                   '--product', 'B',
                   '--confirm-ignore-lineage',
                   str(prefix)])

    assert 'ERROR' in r.output
    assert 'Dataset metadata did not match product signature' in r.output
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

    prefix = write_files({'lineage.yml': yaml.safe_dump_all(child_docs),
                          'main.yml': yaml.safe_dump(ds.doc),
                          })

    clirunner(['dataset', 'add', str(prefix / 'lineage.yml')])
    assert index.datasets.get(ds.sources['ae'].id) is not None
    assert index.datasets.get(ds.sources['ab'].id) is not None
    assert index.datasets.get(ds.sources['ac'].id) is not None

    clirunner(['dataset', 'add',
               '--no-auto-add-lineage',
               '--product', 'A',
               str(prefix / 'main.yml')])

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

    prefix = write_files({'lineage.yml': yaml.safe_dump_all(child_docs),
                          'main.yml': yaml.safe_dump(modified_doc),
                          })

    clirunner(['dataset', 'add', str(prefix / 'lineage.yml')])
    assert index.datasets.get(ds.sources['ae'].id) is not None

    r = clirunner(['dataset', 'add',
                   str(prefix / 'main.yml')])

    assert 'ERROR Inconsistent lineage dataset' in r.output

    assert index.datasets.has(ds.id) is False
    assert index.datasets.has(ds.sources['ab'].id) is False
    assert index.datasets.has(ds.sources['ac'].id) is False
    assert index.datasets.has(ds.sources['ac'].sources['cd'].id) is False

    # now again but skipping verification check
    r = clirunner(['dataset', 'add', '--no-verify-lineage',
                   str(prefix / 'main.yml')])

    assert index.datasets.has(ds.id)
    assert index.datasets.has(ds.sources['ab'].id)
    assert index.datasets.has(ds.sources['ac'].id)
    assert index.datasets.has(ds.sources['ac'].sources['cd'].id)


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
                   str(prefix / 'main.yml')])

    assert 'ERROR Following lineage datasets are missing' in r.output
    assert index.datasets.has(ds.id) is False

    # now add lineage and try again
    clirunner(['dataset', 'add', str(prefix / 'lineage.yml')])
    assert index.datasets.has(ds.sources['ae'].id)
    r = clirunner(['dataset', 'add',
                   '--no-auto-add-lineage',
                   str(prefix / 'main.yml')])

    assert index.datasets.has(ds.id)


def check_missing_metadata_doc(clirunner):
    prefix = write_files({'im.tiff': ''})
    r = clirunner(['dataset', 'add', str(prefix / 'im.tiff')])
    assert "ERROR No supported metadata docs found for dataset" in r.output


def check_no_confirm(clirunner, path):
    r = clirunner(['dataset', 'add', '--ignore-lineage', str(path)], expect_success=False)
    assert r.exit_code != 0
    assert 'Use --confirm-ignore-lineage from non-interactive scripts' in r.output


def check_bad_yaml(clirunner, index):
    prefix = write_files({'broken.yml': '"'})
    r = clirunner(['dataset', 'add', str(prefix / 'broken.yml')])
    assert 'ERROR Failed reading documents from ' in r.output


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_dataset_add_no_id(dataset_add_configs, index_empty, clirunner):
    p = dataset_add_configs
    index = index_empty
    ds_no_id = load_dataset_definition(p.datasets_no_id)

    clirunner(['metadata', 'add', p.metadata])
    clirunner(['product', 'add', p.products])

    # Check .hl.Doc2Dataset
    doc2ds = Doc2Dataset(index)
    _ds, _err = doc2ds(ds_no_id, 'file:///something')
    assert _err == 'No id defined in dataset doc'


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_dataset_add(dataset_add_configs, index_empty, clirunner):
    p = dataset_add_configs
    index = index_empty
    r = clirunner(['dataset', 'add', p.datasets], expect_success=False)
    assert r.exit_code != 0
    assert 'Found no matching products' in r.output

    clirunner(['metadata', 'add', p.metadata])
    clirunner(['product', 'add', p.products])
    clirunner(['dataset', 'add', p.datasets])
    clirunner(['dataset', 'add', p.datasets_bad1])
    clirunner(['dataset', 'add', p.datasets_eo3])

    ds = load_dataset_definition(p.datasets)
    ds_bad1 = load_dataset_definition(p.datasets_bad1)

    # Check .hl.Doc2Dataset
    doc2ds = Doc2Dataset(index)
    _ds, _err = doc2ds(ds.doc, 'file:///something')
    assert _err is None
    assert _ds.id == ds.id
    assert _ds.metadata_doc == ds.doc

    # Check dataset search
    r = clirunner(['dataset', 'search'], expect_success=True)
    assert str(ds.id) in r.output
    assert str(ds_bad1.id) not in r.output
    assert str(ds.sources['ab'].id) in r.output
    assert str(ds.sources['ac'].sources['cd'].id) in r.output

    r = clirunner(['dataset', 'info', '-f', 'csv', str(ds.id)])
    assert str(ds.id) in r.output

    r = clirunner(['dataset', 'info', '-f', 'yaml', '--show-sources', str(ds.id)])
    assert str(ds.sources['ae'].id) in r.output

    r = clirunner(['dataset', 'info', '-f', 'yaml', '--show-derived', str(ds.sources['ae'].id)])
    assert str(ds.id) in r.output

    ds_ = SimpleDocNav(gen_dataset_test_dag(1, force_tree=True))
    assert ds_.id == ds.id

    x = index.datasets.get(ds.id, include_sources=True)
    assert x.sources['ab'].id == ds.sources['ab'].id
    assert x.sources['ac'].sources['cd'].id == ds.sources['ac'].sources['cd'].id

    check_skip_lineage_test(clirunner, index)
    check_no_product_match(clirunner, index)
    check_with_existing_lineage(clirunner, index)
    check_inconsistent_lineage(clirunner, index)
    check_missing_metadata_doc(clirunner)
    check_missing_lineage(clirunner, index)
    check_no_confirm(clirunner, p.datasets)
    check_bad_yaml(clirunner, index)

    # check --product=nosuchproduct
    r = clirunner(['dataset', 'add', '--product', 'nosuchproduct', p.datasets],
                  expect_success=False)

    assert "ERROR Supplied product name" in r.output
    assert r.exit_code != 0

    # test dataset add eo3
    r = clirunner(['dataset', 'add', p.datasets_eo3])
    assert r.exit_code == 0

    ds_eo3 = load_dataset_definition(p.datasets_eo3)
    assert ds_eo3.location is not None

    _ds = index.datasets.get(ds_eo3.id, include_sources=True)
    assert sorted(_ds.sources) == ['a', 'bc1', 'bc2']
    assert _ds.crs == 'EPSG:3857'
    assert _ds.extent is not None
    assert _ds.extent.crs == _ds.crs
    assert _ds.uris == [ds_eo3.location]
    assert 'location' not in _ds.metadata_doc


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_dataset_add_ambiguous_products(dataset_add_configs, index_empty, clirunner):
    p = dataset_add_configs
    index = index_empty

    dss = [SimpleDocNav(dataset_maker(i)(
        'A',
        product_type='eo',
        flag_a='a',
        flag_b='b')) for i in [1, 2]]

    prefix = write_files({
        'products.yml': '''
name: A
description: test product A
metadata_type: minimal
metadata:
    product_type: eo
    flag_a: a

---
name: B
description: test product B
metadata_type: minimal
metadata:
    product_type: eo
    flag_b: b
    ''',
        'dataset1.yml': yaml.safe_dump(dss[0].doc),
        'dataset2.yml': yaml.safe_dump(dss[1].doc),
    })

    clirunner(['metadata', 'add', p.metadata])
    clirunner(['product', 'add', str(prefix / 'products.yml')])

    pp = list(index.products.get_all())
    assert len(pp) == 2

    for ds, i in zip(dss, (1, 2)):
        r = clirunner(['dataset', 'add', str(prefix / ('dataset%d.yml' % i))])
        assert 'ERROR Auto match failed' in r.output
        assert 'matches several products' in r.output
        assert index.datasets.has(ds.id) is False

    # check that forcing product works
    ds, fname = dss[0], 'dataset1.yml'
    r = clirunner(['dataset', 'add',
                   '--product', 'A',
                   str(prefix / fname)])

    assert index.datasets.has(ds.id) is True

    # check that forcing via exclude works
    ds, fname = dss[1], 'dataset2.yml'
    r = clirunner(['dataset', 'add',
                   '--exclude-product', 'B',
                   str(prefix / fname)])

    assert index.datasets.has(ds.id) is True


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_dataset_add_with_nans(dataset_add_configs, index_empty, clirunner):
    p = dataset_add_configs
    index = index_empty

    clirunner(['metadata', 'add', p.metadata])
    clirunner(['product', 'add', p.products])

    mk = dataset_maker(0)

    c = mk('C', product_type='C',
           val_is_nan=math.nan,
           val_is_inf=math.inf,
           val_is_neginf=-math.inf)

    b = mk('B', sources={'bc': c}, product_type='B')
    a = mk('A', sources={'ac': c}, product_type='A')

    prefix = write_files({
        'dataset.yml': yaml.safe_dump_all([a, b]),
    })

    r = clirunner(['dataset', 'add',
                   '--auto-add-lineage',
                   '--verify-lineage',
                   str(prefix / 'dataset.yml')])

    assert "ERROR" not in r.output

    a, b, c = [SimpleDocNav(v) for v in (a, b, c)]

    assert index.datasets.bulk_has([a.id, b.id, c.id]) == [True, True, True]

    c_doc = index.datasets.get(c.id).metadata_doc

    assert c_doc['val_is_nan'] == 'NaN'
    assert c_doc['val_is_inf'] == 'Infinity'
    assert c_doc['val_is_neginf'] == '-Infinity'


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_dataset_add_inconsistent_measurements(dataset_add_configs, index_empty, clirunner):
    p = dataset_add_configs
    index = index_empty
    mk = dataset_maker(0)

    # not set, empty, subset, full set, super-set
    ds1 = SimpleDocNav(mk('A', product_type='eo', ))
    ds2 = SimpleDocNav(mk('B', product_type='eo', measurements={}))
    ds3 = SimpleDocNav(mk('C', product_type='eo', measurements={
        'red': {}
    }))
    ds4 = SimpleDocNav(mk('D', product_type='eo', measurements={
        'red': {},
        'green': {},
    }))
    ds5 = SimpleDocNav(mk('E', product_type='eo', measurements={
        'red': {},
        'green': {},
        'extra': {},
    }))

    dss = (ds1, ds2, ds3, ds4, ds5)
    docs = [ds.doc for ds in dss]

    prefix = write_files({
        'products.yml': '''
name: eo
description: test product
metadata_type: with_measurements
metadata:
    product_type: eo

measurements:
    - name: red
      dtype: int16
      nodata: -999
      units: '1'

    - name: green
      dtype: int16
      nodata: -999
      units: '1'
    ''',
        'dataset.yml': yaml.safe_dump_all(docs),
    })

    clirunner(['metadata', 'add', p.metadata])
    r = clirunner(['product', 'add', str(prefix / 'products.yml')])

    pp = list(index.products.get_all())
    assert len(pp) == 1

    r = clirunner(['dataset', 'add', str(prefix / 'dataset.yml')])
    print(r.output)

    r = clirunner(['dataset', 'search', '-f', 'csv'])
    assert str(ds1.id) not in r.output
    assert str(ds2.id) not in r.output
    assert str(ds3.id) not in r.output
    assert str(ds4.id) in r.output
    assert str(ds5.id) in r.output


def dataset_archive_prep(dataset_add_configs, index_empty, clirunner):
    p = dataset_add_configs
    index = index_empty

    clirunner(['metadata', 'add', p.metadata])
    clirunner(['product', 'add', p.products])
    clirunner(['dataset', 'add', p.datasets])

    ds = load_dataset_definition(p.datasets)

    assert index.datasets.has(ds.id) is True

    return p, index, ds


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_dataset_archive_dry_run(dataset_add_configs, index_empty, clirunner):
    p, index, ds = dataset_archive_prep(dataset_add_configs, index_empty, clirunner)

    non_existent_uuid = '00000000-1036-5607-a62f-fde5e3fec985'

    # Single valid UUID is detected and not archived
    single_valid_uuid = clirunner(['dataset', 'archive', '--dry-run', str(ds.id)])
    assert str(ds.id) in single_valid_uuid.output
    assert index.datasets.has(ds.id) is True

    # Single invalid UUID is detected
    single_invalid_uuid = clirunner(['dataset', 'archive', '--dry-run', non_existent_uuid], expect_success=False)
    assert single_invalid_uuid.exit_code is -1
    assert non_existent_uuid in single_invalid_uuid.output
    assert index.datasets.has(ds.id) is True

    # Valid and invalid UUIDs
    # The single invalid UUID should halt all operations
    valid_and_invalid_uuid = clirunner(['dataset',
                                        'archive',
                                        '--dry-run',
                                        str(ds.id),
                                        non_existent_uuid],
                                       expect_success=False)
    assert non_existent_uuid in valid_and_invalid_uuid.output
    assert single_invalid_uuid.exit_code is -1
    assert index.datasets.has(ds.id) is True

    valid_and_invalid_uuid = clirunner(['dataset',
                                        'archive',
                                        '--dry-run',
                                        '--archive-derived',
                                        str(ds.id),
                                        non_existent_uuid
                                        ],
                                       expect_success=False)
    assert single_invalid_uuid.exit_code is -1
    assert non_existent_uuid in valid_and_invalid_uuid.output
    assert index.datasets.has(ds.id) is True

    # Multiple Valid UUIDs
    # Not archived in the database and are shown in output
    multiple_valid_uuid = clirunner([
        'dataset', 'archive', '--dry-run',
        str(ds.sources['ae'].id), str(ds.sources['ab'].id)])
    assert str(ds.sources['ae'].id) in multiple_valid_uuid.output
    assert str(ds.sources['ab'].id) in multiple_valid_uuid.output
    assert index.datasets.has(ds.sources['ae'].id) is True
    assert index.datasets.has(ds.sources['ab'].id) is True

    archive_derived = clirunner(['dataset',
                                 'archive',
                                 '--dry-run',
                                 '--archive-derived',
                                 str(ds.sources['ae'].id),
                                 str(ds.sources['ab'].id)
                                 ])
    assert str(ds.id) in archive_derived.output
    assert str(ds.sources['ae'].id) in archive_derived.output
    assert index.datasets.has(ds.id) is True


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_dataset_archive_restore_invalid(dataset_add_configs, index_empty, clirunner):
    p, index, ds = dataset_archive_prep(dataset_add_configs, index_empty, clirunner)

    non_existent_uuid = '00000000-1036-5607-a62f-fde5e3fec985'

    # With non-existent uuid, operations should halt.
    r = clirunner(['dataset', 'archive', str(ds.id), non_existent_uuid], expect_success=False)
    r = clirunner(['dataset', 'info', str(ds.id)])
    assert 'status: archived' not in r.output
    assert index.datasets.has(ds.id) is True

    # With non-existent uuid, operations should halt.
    d_id = ds.sources['ac'].sources['cd'].id
    r = clirunner(['dataset', 'archive', '--archive-derived', str(d_id), non_existent_uuid], expect_success=False)
    r = clirunner(['dataset', 'info', str(ds.id), str(ds.sources['ab'].id), str(ds.sources['ac'].id)])
    assert 'status: active' in r.output
    assert 'status: archived' not in r.output
    assert index.datasets.has(ds.id) is True


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_dataset_archive_restore(dataset_add_configs, index_empty, clirunner):
    p, index, ds = dataset_archive_prep(dataset_add_configs, index_empty, clirunner)

    # Run for real
    r = clirunner(['dataset', 'archive', str(ds.id)])
    r = clirunner(['dataset', 'info', str(ds.id)])
    assert 'status: archived' in r.output

    # restore dry run
    r = clirunner(['dataset', 'restore', '--dry-run', str(ds.id)])
    r = clirunner(['dataset', 'info', str(ds.id)])
    assert 'status: archived' in r.output

    # restore for real
    r = clirunner(['dataset', 'restore', str(ds.id)])
    r = clirunner(['dataset', 'info', str(ds.id)])
    assert 'status: active' in r.output

    # archive derived
    d_id = ds.sources['ac'].sources['cd'].id
    r = clirunner(['dataset', 'archive', '--archive-derived', str(d_id)])
    r = clirunner(['dataset', 'info', str(ds.id), str(ds.sources['ab'].id), str(ds.sources['ac'].id)])
    assert 'status: active' not in r.output
    assert 'status: archived' in r.output

    # restore derived
    r = clirunner(['dataset', 'restore', '--restore-derived', str(d_id)])
    r = clirunner(['dataset', 'info', str(ds.id), str(ds.sources['ab'].id), str(ds.sources['ac'].id)])
    assert 'status: active' in r.output
    assert 'status: archived' not in r.output


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_dataset_add_http(dataset_add_configs, index: Index, default_metadata_type: MetadataType, httpserver,
                          clirunner):
    # pytest-localserver also looks good, it's been around for ages, but httpserver is the new cool
    p = dataset_add_configs

    httpserver.expect_request('/metadata_types.yaml').respond_with_data(open(p.metadata).read())
    httpserver.expect_request('/products.yaml').respond_with_data(open(p.products).read())
    httpserver.expect_request('/datasets.yaml').respond_with_data(open(p.datasets).read())
    # check that the request is served
    #    assert requests.get(httpserver.url_for("/dataset.yaml")).yaml() == {'foo': 'bar'}

    clirunner(['metadata', 'add', httpserver.url_for('/metadata_types.yaml')])
    clirunner(['product', 'add', httpserver.url_for('/products.yaml')])
    # clirunner(['dataset', 'add', p.datasets])
    clirunner(['dataset', 'add', httpserver.url_for('/datasets.yaml')])

    ds = load_dataset_definition(p.datasets)
    assert index.datasets.has(ds.id)


def xtest_dataset_add_fails(clirunner, index):
    result = clirunner(['dataset', 'add', 'bad_path.yaml'], expect_success=False)
    assert result.exit_code != 0, "Surely not being able to add a dataset when requested should return an error."


def test_resolve_uri():
    def doc(loc=None):
        return SimpleDocNav(dict(location=loc, id='4d9fd75c-1309-4712-93b5-f0d9c6fdd8ab'))

    uri = "file:///a"
    override = 'https://a.com/b.yaml'
    assert _resolve_uri(uri, doc()) is uri
    assert _resolve_uri(uri, doc([])) is uri
    assert _resolve_uri(uri, doc({})) is uri
    assert _resolve_uri(uri, doc(object())) is uri
    assert _resolve_uri(uri, doc(override)) is override
    assert _resolve_uri(uri, doc([override])) is override
    assert _resolve_uri(uri, doc([override, "something"])) is override
    assert _resolve_uri(uri, doc((override, "something"))) is override
