"""
Test utility functions from :module:`datacube.utils`


"""
import os
import pathlib
import string
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import rasterio
import toolz
import xarray as xr
from dateutil.parser import parse
from hypothesis import given
from hypothesis.strategies import integers, text
from pandas import to_datetime

from datacube.helpers import write_geotiff
from datacube.utils.math import num2numpy
from datacube.model import MetadataType
from datacube.model.utils import xr_apply, traverse_datasets, flatten_datasets, dedup_lineage
from datacube.testutils import mk_sample_product, make_graph_abcde, gen_dataset_test_dag, dataset_maker
from datacube.utils import gen_password, write_user_secret_file, slurp, read_documents, InvalidDocException, \
    SimpleDocNav
from datacube.utils.changes import check_doc_unchanged, get_doc_changes, MISSING, DocumentMismatchError
from datacube.utils.dates import date_sequence
from datacube.utils.generic import map_with_lookahead
from datacube.utils.math import clamp
from datacube.utils.py import sorted_items
from datacube.utils.uris import uri_to_local_path, mk_part_uri, get_part_from_uri, as_url, is_url, \
    without_lineage_sources, normalise_path, default_base_dir


def test_stats_dates():
    # Winter for 1990
    winter_1990 = list(date_sequence(start=to_datetime('1990-06-01'), end=to_datetime('1990-09-01'), step_size='3m',
                                     stats_duration='3m'))
    assert winter_1990 == [(parse('1990-06-01'), parse('1990-09-01'))]

    # Every winter from 1990 - 1992
    three_years_of_winter = list(date_sequence(start=to_datetime('1990-06-01'), end=to_datetime('1992-09-01'),
                                               step_size='1y',
                                               stats_duration='3m'))
    assert three_years_of_winter == [(parse('1990-06-01'), parse('1990-09-01')),
                                     (parse('1991-06-01'), parse('1991-09-01')),
                                     (parse('1992-06-01'), parse('1992-09-01'))]

    # Full years from 1990 - 1994
    five_full_years = list(date_sequence(start=to_datetime('1990-01-01'), end=to_datetime('1995'), step_size='1y',
                                         stats_duration='1y'))
    assert five_full_years == [(parse('1990-01-01'), parse('1991-01-01')),
                               (parse('1991-01-01'), parse('1992-01-01')),
                               (parse('1992-01-01'), parse('1993-01-01')),
                               (parse('1993-01-01'), parse('1994-01-01')),
                               (parse('1994-01-01'), parse('1995-01-01'))]

    # Every season (three months), starting in March, from 1990 until end 1992-02
    two_years_of_seasons = list(date_sequence(start=to_datetime('1990-03-01'), end=to_datetime('1992-03'),
                                              step_size='3m',
                                              stats_duration='3m'))
    assert len(two_years_of_seasons) == 8
    assert two_years_of_seasons == [(parse('1990-03-01'), parse('1990-06-01')),
                                    (parse('1990-06-01'), parse('1990-09-01')),
                                    (parse('1990-09-01'), parse('1990-12-01')),
                                    (parse('1990-12-01'), parse('1991-03-01')),
                                    (parse('1991-03-01'), parse('1991-06-01')),
                                    (parse('1991-06-01'), parse('1991-09-01')),
                                    (parse('1991-09-01'), parse('1991-12-01')),
                                    (parse('1991-12-01'), parse('1992-03-01'))]  # Leap year!

    # Every month from 1990-01 to 1990-06
    monthly = list(date_sequence(start=to_datetime('1990-01-01'), end=to_datetime('1990-07-01'), step_size='1m',
                                 stats_duration='1m'))
    assert len(monthly) == 6

    # Complex
    # I want the average over 5 years


def test_uri_to_local_path():
    if os.name == 'nt':
        assert 'C:\\tmp\\test.tmp' == str(uri_to_local_path('file:///C:/tmp/test.tmp'))
        assert '\\\\remote\\path\\file.txt' == str(uri_to_local_path('file://remote/path/file.txt'))

    else:
        assert '/tmp/something.txt' == str(uri_to_local_path('file:///tmp/something.txt'))

        with pytest.raises(ValueError):
            uri_to_local_path('file://remote/path/file.txt')

    assert uri_to_local_path(None) is None

    with pytest.raises(ValueError):
        uri_to_local_path('ftp://example.com/tmp/something.txt')


@given(integers(), integers(), integers())
def test_clamp(x, lower_bound, upper_bound):
    if lower_bound > upper_bound:
        lower_bound, upper_bound = upper_bound, lower_bound
    new_x = clamp(x, lower_bound, upper_bound)

    # If x was already between the bounds, it shouldn't have changed
    if lower_bound <= x <= upper_bound:
        assert new_x == x
    assert lower_bound <= new_x <= upper_bound


@given(integers(min_value=10, max_value=30))
def test_gen_pass(n_bytes):
    password1 = gen_password(n_bytes)
    password2 = gen_password(n_bytes)
    assert len(password1) >= n_bytes
    assert len(password2) >= n_bytes
    assert password1 != password2


@given(text(alphabet=string.digits + string.ascii_letters + ' ,:.![]?', max_size=20))
def test_write_user_secret_file(txt):
    fname = u".tst-datacube-uefvwr4cfkkl0ijk.txt"

    write_user_secret_file(txt, fname)
    txt_back = slurp(fname)
    os.remove(fname)
    assert txt == txt_back
    assert slurp(fname) is None


doc_changes = [
    (1, 1, []),
    ({}, {}, []),
    ({'a': 1}, {'a': 1}, []),
    ({'a': {'b': 1}}, {'a': {'b': 1}}, []),
    ([1, 2, 3], [1, 2, 3], []),
    ([1, 2, [3, 4, 5]], [1, 2, [3, 4, 5]], []),
    (1, 2, [((), 1, 2)]),
    ([1, 2, 3], [2, 1, 3], [((0,), 1, 2), ((1,), 2, 1)]),
    ([1, 2, [3, 4, 5]], [1, 2, [3, 6, 7]], [((2, 1), 4, 6), ((2, 2), 5, 7)]),
    ({'a': 1}, {'a': 2}, [(('a',), 1, 2)]),
    ({'a': 1}, {'a': 2}, [(('a',), 1, 2)]),
    ({'a': 1}, {'b': 1}, [(('a',), 1, MISSING), (('b',), MISSING, 1)]),
    ({'a': {'b': 1}}, {'a': {'b': 2}}, [(('a', 'b'), 1, 2)]),
    ({}, {'b': 1}, [(('b',), MISSING, 1)]),
    ({'a': {'c': 1}}, {'a': {'b': 1}}, [(('a', 'b'), MISSING, 1), (('a', 'c'), 1, MISSING)])
]


@pytest.mark.parametrize("v1, v2, expected", doc_changes)
def test_get_doc_changes(v1, v2, expected):
    rval = get_doc_changes(v1, v2)
    assert rval == expected


def test_get_doc_changes_w_baseprefix():
    rval = get_doc_changes({}, None, base_prefix=('a',))
    assert rval == [(('a',), {}, None)]


@pytest.mark.parametrize("v1, v2, expected", doc_changes)
def test_check_doc_unchanged(v1, v2, expected):
    if expected != []:
        with pytest.raises(DocumentMismatchError):
            check_doc_unchanged(v1, v2, 'name')
    else:
        # No Error Raised
        check_doc_unchanged(v1, v2, 'name')


def test_more_check_doc_unchanged():
    # No exception raised
    check_doc_unchanged({'a': 1}, {'a': 1}, 'Letters')

    with pytest.raises(DocumentMismatchError, message='Letters differs from stored (a: 1!=2)'):
        check_doc_unchanged({'a': 1}, {'a': 2}, 'Letters')

    with pytest.raises(DocumentMismatchError, message='Letters differs from stored (a.b: 1!=2)'):
        check_doc_unchanged({'a': {'b': 1}}, {'a': {'b': 2}}, 'Letters')


def test_write_geotiff(tmpdir, odc_style_xr_dataset):
    """Ensure the geotiff helper writer works, and supports datasets smaller than 256x256."""
    filename = tmpdir + '/test.tif'

    assert len(odc_style_xr_dataset.latitude) < 256

    write_geotiff(filename, odc_style_xr_dataset)

    assert filename.exists()

    with rasterio.open(str(filename)) as src:
        written_data = src.read(1)

        assert (written_data == odc_style_xr_dataset['B10']).all()


def test_write_geotiff_time_index_deprecated():
    """The `time_index` parameter to `write_geotiff()` was a poorly thought out addition and is now deprecated."""

    with pytest.raises(ValueError):
        write_geotiff("", None, time_index=1)


def test_testutils_mk_sample():
    pp = mk_sample_product('tt', measurements=[('aa', 'int16', -999),
                                               ('bb', 'float32', np.nan)])
    assert set(pp.measurements) == {'aa', 'bb'}

    pp = mk_sample_product('tt', measurements=['aa', 'bb'])
    assert set(pp.measurements) == {'aa', 'bb'}

    pp = mk_sample_product('tt', measurements=[dict(name=n) for n in ['aa', 'bb']])
    assert set(pp.measurements) == {'aa', 'bb'}

    with pytest.raises(ValueError):
        mk_sample_product('tt', measurements=[None])


def test_testutils_write_files():
    from datacube.testutils import write_files, assert_file_structure

    files = {'a.txt': 'string',
             'aa.txt': ('line1\n', 'line2\n')}

    pp = write_files(files)
    assert pp.exists()
    assert_file_structure(pp, files)

    # test that we detect missing files
    (pp / 'a.txt').unlink()

    with pytest.raises(AssertionError):
        assert_file_structure(pp, files)

    with pytest.raises(AssertionError):
        assert_file_structure(pp, {'aa.txt': 3})

    with pytest.raises(ValueError):
        write_files({'tt': 3})


def test_without_lineage_sources():
    def mk_sample(v):
        return dict(lineage={'source_datasets': v, 'a': 'a', 'b': 'b'},
                    aa='aa',
                    bb=dict(bb='bb'))

    spec = mk_sample_product('tt')

    x = {'a': 1}
    assert without_lineage_sources(x, spec) == x
    assert without_lineage_sources(x, spec, inplace=True) == x

    x = {'a': 1, 'lineage': {}}
    assert without_lineage_sources(x, spec) == x
    assert without_lineage_sources(x, spec, inplace=True) == x

    x = mk_sample(1)
    assert without_lineage_sources(x, spec) != x
    assert x['lineage']['source_datasets'] == 1

    x = mk_sample(2)
    assert without_lineage_sources(x, spec, inplace=True) == x
    assert x['lineage']['source_datasets'] == {}

    assert mk_sample(10) != mk_sample({})
    assert without_lineage_sources(mk_sample(10), spec) == mk_sample({})
    assert without_lineage_sources(mk_sample(10), spec, inplace=True) == mk_sample({})

    # check behaviour when `sources` is not defined for the type
    no_sources_type = MetadataType({
        'name': 'eo',
        'description': 'Sample',
        'dataset': dict(
            id=['id'],
            label=['ga_label'],
            creation_time=['creation_dt'],
            measurements=['image', 'bands'],
            format=['format', 'name'],
        )
    }, dataset_search_fields={})

    assert without_lineage_sources(mk_sample(10), no_sources_type) == mk_sample(10)
    assert without_lineage_sources(mk_sample(10), no_sources_type, inplace=True) == mk_sample(10)


def test_map_with_lookahead():
    def if_one(x):
        return 'one' + str(x)

    def if_many(x):
        return 'many' + str(x)

    assert list(map_with_lookahead(iter([]), if_one, if_many)) == []
    assert list(map_with_lookahead(iter([1]), if_one, if_many)) == [if_one(1)]
    assert list(map_with_lookahead(range(5), if_one, if_many)) == list(map(if_many, range(5)))
    assert list(map_with_lookahead(range(10), if_one=if_one)) == list(range(10))
    assert list(map_with_lookahead(iter([1]), if_many=if_many)) == [1]


def test_part_uri():
    base = 'file:///foo.txt'

    for i in range(10):
        assert get_part_from_uri(mk_part_uri(base, i)) == i

    assert get_part_from_uri('file:///f.txt') is None
    assert get_part_from_uri('file:///f.txt#something_else') is None
    assert get_part_from_uri('file:///f.txt#part=aa') == 'aa'
    assert get_part_from_uri('file:///f.txt#part=111') == 111


def test_read_docs_from_local_path(sample_document_files):
    _test_read_docs_impl(sample_document_files)


def test_read_docs_from_file_uris(sample_document_files):
    uris = [('file://' + doc, ndocs) for doc, ndocs in sample_document_files]
    _test_read_docs_impl(uris)


def test_read_docs_from_s3(sample_document_files):
    """
    Use a mocked S3 bucket to test reading documents from S3
    """
    boto3 = pytest.importorskip('boto3')
    moto = pytest.importorskip('moto')

    os.environ['AWS_ACCESS_KEY_ID'] = 'fake'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'fake'

    with moto.mock_s3():
        s3 = boto3.resource('s3', region_name='us-east-1')
        bucket = s3.create_bucket(Bucket='mybucket')

        mocked_s3_objs = []
        for abs_fname, ndocs in sample_document_files:
            if abs_fname.endswith('gz') or abs_fname.endswith('nc'):
                continue

            fname = Path(abs_fname).name
            bucket.upload_file(abs_fname, fname)

            mocked_s3_objs.append(('s3://mybucket/' + fname, ndocs))

        _test_read_docs_impl(mocked_s3_objs)


def _test_read_docs_impl(sample_document_files):
    # Test case for returning URIs pointing to documents
    for filepath, num_docs in sample_document_files:
        all_docs = list(read_documents(filepath, uri=True))
        assert len(all_docs) == num_docs

        for uri, doc in all_docs:
            assert isinstance(doc, dict)
            assert isinstance(uri, str)

        url = as_url(filepath)
        if num_docs > 1:
            expect_uris = [as_url(url) + '#part={}'.format(i) for i in range(num_docs)]
        else:
            expect_uris = [as_url(url)]

        assert [f for f, _ in all_docs] == expect_uris


def test_xr_apply():
    src = xr.DataArray(np.asarray([1, 2, 3], dtype='uint8'), dims=['time'])
    dst = xr_apply(src, lambda _, v: v, dtype='float32')

    assert dst.dtype.name == 'float32'
    assert dst.shape == src.shape
    assert dst.values.tolist() == [1, 2, 3]

    dst = xr_apply(src, lambda _, v: v)
    assert dst.dtype.name == 'uint8'
    assert dst.shape == src.shape
    assert dst.values.tolist() == [1, 2, 3]

    dst = xr_apply(src, lambda idx, _, v: idx[0] + v, with_numeric_index=True)
    assert dst.dtype.name == 'uint8'
    assert dst.shape == src.shape
    assert dst.values.tolist() == [0 + 1, 1 + 2, 2 + 3]


def test_sorted_items():
    aa = dict(c=1, b={}, a=[])

    assert ''.join(k for k, _ in sorted_items(aa)) == 'abc'
    assert ''.join(k for k, _ in sorted_items(aa, key=lambda x: x)) == 'abc'
    assert ''.join(k for k, _ in sorted_items(aa, reverse=True)) == 'cba'

    remap = dict(c=0, a=1, b=2)
    assert ''.join(k for k, _ in sorted_items(aa, key=lambda x: remap[x])) == 'cab'


def test_dataset_maker():
    mk = dataset_maker(0)
    assert mk('aa') == mk('aa')

    a = SimpleDocNav(mk('A'))
    b = SimpleDocNav(mk('B'))

    assert a.id != b.id
    assert a.doc['creation_dt'] == b.doc['creation_dt']
    assert isinstance(a.id, str)
    assert a.sources == {}

    a1, a2 = [dataset_maker(i)('A', product_type='eo') for i in (0, 1)]
    assert a1['id'] != a2['id']
    assert a1['creation_dt'] != a2['creation_dt']
    assert a1['product_type'] == 'eo'

    c = SimpleDocNav(mk('C', sources=dict(a=a.doc, b=b.doc)))
    assert c.sources['a'].doc is a.doc
    assert c.sources['b'].doc is b.doc


def test_traverse_datasets():
    """
      A -> B
      |    |
      |    v
      +--> C -> D
      |
      +--> E
    """

    def node(name, **kwargs):
        return SimpleNamespace(id=name, sources=kwargs)

    A, *_ = make_graph_abcde(node)

    def visitor(node, name=None, depth=0, out=None):
        s = '{}:{}:{:d}'.format(node.id, name if name else '..', depth)
        out.append(s)

    with pytest.raises(ValueError):
        traverse_datasets(A, visitor, mode='not-a-real-mode')

    expect_preorder = '''
A:..:0
B:ab:1
C:bc:2
D:cd:3
C:ac:1
D:cd:2
E:ae:1
'''.lstrip().rstrip()

    expect_postorder = '''
D:cd:3
C:bc:2
B:ab:1
D:cd:2
C:ac:1
E:ae:1
A:..:0
'''.lstrip().rstrip()

    for mode, expect in zip(['pre-order', 'post-order'],
                            [expect_preorder, expect_postorder]):
        out = []
        traverse_datasets(A, visitor, mode=mode, out=out)
        assert '\n'.join(out) == expect

    fv = flatten_datasets(A)

    assert len(fv['A']) == 1
    assert len(fv['C']) == 2
    assert len(fv['E']) == 1
    assert set(fv.keys()) == set('ABCDE')


def test_simple_doc_nav():
    """
      A -> B
      |    |
      |    v
      +--> C -> D
      |
      +--> E
    """

    def node(name, **kwargs):
        return dict(id=name, lineage=dict(source_datasets=kwargs))

    A, _, C, _, _ = make_graph_abcde(node)
    rdr = SimpleDocNav(A)

    assert rdr.doc == A
    assert rdr.doc_without_lineage_sources == node('A')
    assert isinstance(rdr.sources['ae'], SimpleDocNav)
    assert rdr.sources['ab'].sources['bc'].doc == C
    assert rdr.doc_without_lineage_sources is rdr.doc_without_lineage_sources
    assert rdr.sources is rdr.sources
    assert isinstance(rdr.sources_path, tuple)

    def visitor(node, name=None, depth=0, out=None):
        s = '{}:{}:{:d}'.format(node.id, name if name else '..', depth)
        out.append(s)

    expect_preorder = '''
A:..:0
B:ab:1
C:bc:2
D:cd:3
C:ac:1
D:cd:2
E:ae:1
'''.lstrip().rstrip()

    expect_postorder = '''
D:cd:3
C:bc:2
B:ab:1
D:cd:2
C:ac:1
E:ae:1
A:..:0
'''.lstrip().rstrip()

    for mode, expect in zip(['pre-order', 'post-order'],
                            [expect_preorder, expect_postorder]):
        out = []
        traverse_datasets(rdr, visitor, mode=mode, out=out)
        assert '\n'.join(out) == expect

    fv = flatten_datasets(rdr)

    assert len(fv['A']) == 1
    assert len(fv['C']) == 2
    assert len(fv['E']) == 1
    assert set(fv.keys()) == set('ABCDE')

    fv, dg = flatten_datasets(rdr, with_depth_grouping=True)

    assert len(fv['A']) == 1
    assert len(fv['C']) == 2
    assert len(fv['E']) == 1
    assert set(fv.keys()) == set('ABCDE')
    assert isinstance(dg, list)
    assert len(dg) == 4
    assert [len(l) for l in dg] == [1, 3, 2, 1]

    def to_set(xx):
        return set(x.id for x in xx)

    assert [set(s) for s in ('A',
                             'BCE',
                             'CD',
                             'D')] == [to_set(xx) for xx in dg]


def test_dedup():
    ds0 = SimpleDocNav(gen_dataset_test_dag(1, force_tree=True))

    # make sure ds0 has duplicate C nodes with equivalent data
    assert ds0.sources['ab'].sources['bc'].doc is not ds0.sources['ac'].doc
    assert ds0.sources['ab'].sources['bc'].doc == ds0.sources['ac'].doc

    ds = SimpleDocNav(dedup_lineage(ds0))
    assert ds.sources['ab'].sources['bc'].doc is ds.sources['ac'].doc
    assert ds.sources['ab'].sources['bc'].sources['cd'].doc is ds.sources['ac'].sources['cd'].doc

    # again but with raw doc
    ds = SimpleDocNav(dedup_lineage(ds0.doc))
    assert ds.sources['ab'].sources['bc'].doc is ds.sources['ac'].doc
    assert ds.sources['ab'].sources['bc'].sources['cd'].doc is ds.sources['ac'].sources['cd'].doc

    # Test that we detect inconsistent metadata for duplicate entries
    ds0 = SimpleDocNav(gen_dataset_test_dag(3, force_tree=True))
    ds0.sources['ac'].doc['label'] = 'Modified'
    ds0 = SimpleDocNav(ds0.doc)
    assert ds0.sources['ab'].sources['bc'].doc != ds0.sources['ac'].doc

    with pytest.raises(InvalidDocException, match=r'Inconsistent metadata .*'):
        dedup_lineage(ds0)

    # Test that we detect inconsistent lineage subtrees for duplicate entries

    # Subtest 1: different set of keys
    ds0 = SimpleDocNav(gen_dataset_test_dag(7, force_tree=True))
    srcs = toolz.get_in(ds0.sources_path, ds0.sources['ac'].doc)

    assert 'cd' in srcs
    srcs['cd'] = {}
    ds0 = SimpleDocNav(ds0.doc)

    with pytest.raises(InvalidDocException, match=r'Inconsistent lineage .*'):
        dedup_lineage(ds0)

    # Subtest 2: different values for "child" nodes
    ds0 = SimpleDocNav(gen_dataset_test_dag(7, force_tree=True))
    srcs = toolz.get_in(ds0.sources_path, ds0.sources['ac'].doc)

    assert 'cd' in srcs
    srcs['cd']['id'] = '7fe57724-ed44-4beb-a3ab-c275339049be'
    ds0 = SimpleDocNav(ds0.doc)

    with pytest.raises(InvalidDocException, match=r'Inconsistent lineage .*'):
        dedup_lineage(ds0)

    # Subtest 3: different name for child
    ds0 = SimpleDocNav(gen_dataset_test_dag(7, force_tree=True))
    srcs = toolz.get_in(ds0.sources_path, ds0.sources['ac'].doc)

    assert 'cd' in srcs
    srcs['CD'] = srcs['cd']
    del srcs['cd']
    ds0 = SimpleDocNav(ds0.doc)

    with pytest.raises(InvalidDocException, match=r'Inconsistent lineage .*'):
        dedup_lineage(ds0)


def test_default_base_dir():
    def set_pwd(p):
        os.environ['PWD'] = str(p)

    pwd_backup = os.environ.get('PWD', None)
    cwd = Path('.').resolve()

    # Default base dir (once resolved) will never be different from cwd
    assert default_base_dir().resolve() == cwd

    # should work when PWD is not set
    os.environ.pop('PWD', None)
    assert 'PWD' not in os.environ
    assert default_base_dir() == cwd

    # should work when PWD is not absolute path
    set_pwd('this/is/not/a/valid/path')
    assert default_base_dir() == cwd

    # should be cwd when PWD points to some other dir
    set_pwd(cwd / 'deeper')
    assert default_base_dir() == cwd

    set_pwd(cwd.parent)
    assert default_base_dir() == cwd

    # PWD == cwd
    set_pwd(cwd)
    assert default_base_dir() == cwd

    # TODO:
    # - create symlink to current directory in temp
    # - set PWD to that link
    # - make sure that returned path is the same as symlink and different from cwd

    # restore environment (should probably do it even test fails, eh)
    if pwd_backup:
        set_pwd(pwd_backup)


def test_normalise_path():
    cwd = Path('.').resolve()
    assert normalise_path('.').resolve() == cwd

    p = Path('/a/b/c/d.txt')
    assert normalise_path(p) == Path(p)
    assert normalise_path(str(p)) == Path(p)

    base = Path('/a/b/')
    p = Path('c/d.txt')
    assert normalise_path(p, base) == (base / p)
    assert normalise_path(str(p), str(base)) == (base / p)
    assert normalise_path(p) == (cwd / p)

    with pytest.raises(ValueError):
        normalise_path(p, 'not/absolute/path')


def test_testutils_gtif(tmpdir):
    from datacube.testutils import mk_test_image
    from datacube.testutils.io import write_gtiff, rio_slurp

    w, h, dtype, nodata, ndw = 96, 64, 'int16', -999, 7

    aa = mk_test_image(w, h, dtype, nodata, nodata_width=ndw)
    bb = mk_test_image(w, h, dtype, nodata=None)

    assert aa.shape == (h, w)
    assert aa.dtype.name == dtype
    assert aa[10, 30] == (30 << 8) | 10
    assert aa[10, 11] == nodata
    assert bb[10, 11] == (11 << 8) | 10

    aa5 = np.stack((aa,) * 5)

    fname = pathlib.Path(str(tmpdir / "aa.tiff"))
    fname5 = pathlib.Path(str(tmpdir / "aa5.tiff"))

    aa_meta = write_gtiff(fname, aa, nodata=nodata,
                          blocksize=128,
                          resolution=(100, -100),
                          offset=(12300, 11100),
                          overwrite=True)

    aa5_meta = write_gtiff(str(fname5), aa5, nodata=nodata,
                           resolution=(100, -100),
                           offset=(12300, 11100),
                           overwrite=True)

    assert fname.exists()
    assert fname5.exists()

    assert aa_meta['gbox'].shape == (h, w)
    assert aa_meta['path'] is fname

    aa_, aa_meta_ = rio_slurp(fname)
    aa5_, aa5_meta_ = rio_slurp(fname5)

    assert aa_meta_['path'] is fname

    (sx, _, tx,
     _, sy, ty, *_) = aa5_meta_['transform']

    assert (tx, ty) == (12300, 11100)
    assert (sx, sy) == (100, -100)

    np.testing.assert_array_equal(aa, aa_)
    np.testing.assert_array_equal(aa5, aa5_)

    assert aa_meta_['transform'] == aa_meta['transform']
    assert aa5_meta_['transform'] == aa5_meta['transform']

    # check that overwrite is off by default
    with pytest.raises(IOError):
        write_gtiff(fname, aa, nodata=nodata,
                    blocksize=128)

    # check that overwrite re-writes file
    write_gtiff(fname, bb[:32, :32],
                overwrite=True)

    bb_, _ = rio_slurp(fname)
    np.testing.assert_array_equal(bb[:32, :32], bb_)

    with pytest.raises(ValueError):
        write_gtiff(fname, np.zeros((3, 4, 5, 6)))


def test_testutils_geobox():
    from datacube.testutils.io import dc_crs_from_rio, rio_geobox
    from rasterio.crs import CRS
    from affine import Affine

    assert rio_geobox({}) is None

    A = Affine(10, 0, 4676,
               0, -10, 171878)

    shape = (100, 640)
    h, w = shape
    crs = CRS.from_epsg(3578)

    meta = dict(width=w, height=h, transform=A, crs=crs)
    gbox = rio_geobox(meta)

    assert gbox.shape == shape
    assert gbox.crs.epsg == 3578
    assert gbox.transform == A

    crs_ = dc_crs_from_rio(CRS.from_wkt(crs.wkt))
    assert crs_.epsg is None


@pytest.mark.parametrize("test_input,expected", [
    ("/foo/bar/file.txt", False),
    ("file:///foo/bar/file.txt", True),
    ("test.bar", False),
    ("s3://mybucket/objname.tiff", True),
    ("ftp://host.name/filename.txt", True),
    ("https://host.name.com/path/file.txt", True),
    ("http://host.name.com/path/file.txt", True),
    ("sftp://user:pass@host.name.com/path/file.txt", True),
    ("file+gzip://host.name.com/path/file.txt", True),
    ("bongo:host.name.com/path/file.txt", False),
])
def test_is_url(test_input, expected):
    assert is_url(test_input) == expected


@pytest.fixture
def sample_document_files(data_folder):
    files = [('multi_doc.yml', 3),
             ('multi_doc.yml.gz', 3),
             ('multi_doc.nc', 3),
             ('single_doc.yaml', 1),
             ('sample.json', 1)]

    files = [(str(os.path.join(data_folder, f)), num_docs)
             for f, num_docs in files]

    return files


def test_num2numpy():
    assert num2numpy(None, 'int8') is None
    assert num2numpy(-1, 'int8').dtype == np.dtype('int8')
    assert num2numpy(-1, 'int8').dtype == np.int8(-1)

    assert num2numpy(-1, 'uint8') is None
    assert num2numpy(256, 'uint8') is None
    assert num2numpy(-1, 'uint16') is None
    assert num2numpy(-1, 'uint32') is None
    assert num2numpy(-1, 'uint8', ignore_range=True) == np.uint8(255)

    assert num2numpy(0, 'uint8') == 0
    assert num2numpy(255, 'uint8') == 255
    assert num2numpy(-128, 'int8') == -128
    assert num2numpy(127, 'int8') == 127
    assert num2numpy(128, 'int8') is None

    assert num2numpy(3.3, np.dtype('float32')).dtype == np.dtype('float32')
    assert num2numpy(3.3, np.float32).dtype == np.dtype('float32')
    assert num2numpy(3.3, np.float64).dtype == np.dtype('float64')
