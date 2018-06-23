import pytest
from datetime import datetime, date


# test extent_meta table
@pytest.mark.usefixtures("load_extents")
def test_extent_meta(initialised_postgres_db):
    """
    Test datacube.drivers.postgres.PostgresDbAPI get_db_extent_meta function.
    """

    # get a year long extent meta
    with initialised_postgres_db.connect() as conn:
        res = conn.get_extent_meta(11, '1Y')
        assert res['dataset_type_ref'] == 11
        assert res['start'] == date(year=2015, month=3, day=19)
        assert res['end'] == date(year=2018, month=6, day=14)
        assert res['offset_alias'] == '1Y'
        assert res['crs'] == 'EPSG:4326'


# test extent table
@pytest.mark.usefixtures("load_extents")
def test_extent_slice(initialised_postgres_db):
    """
    Test datacube.drivers.postgres.PostgresDbAPI get_db_extent function.
    """
    # get extents
    with initialised_postgres_db.connect() as conn:
        res = conn.get_extent_slice(11, date(year=2016, month=1, day=1), '1Y')
        assert res is not None
        assert all(key in ['type', 'coordinates'] for key in res.keys())


# test ranges table
@pytest.mark.usefixtures("load_ranges")
def test_dataset_type_range(initialised_postgres_db):
    """
    Test datacube.drivers.postgres.PostgresDbAPI get_ranges function.
    """
    with initialised_postgres_db.connect() as conn:
        res = conn.get_dataset_type_range(11)
        assert res['time_min'].date() == datetime(year=2015, month=1, day=1).date()
        assert res['time_max'].date() == datetime(year=2018, month=1, day=1).date()
        assert res['crs'] == 'EPSG:4326'
        # assert bounds
        bound_saved = bounds = {'left': 0, 'bottom': 0, 'right': 1.5, 'top': 1.5}
        bound = res['bounds']
        assert bound['left'] == bound_saved['left']
        assert bound['bottom'] == bound_saved['bottom']
        assert bound['right'] == bound_saved['right']
        assert bound['top'] == bound_saved['top']


def assert_multipolygon(p1, p2):
    if not len(p1) == len(p2):
        raise AssertionError('Multipolygons has different sizes')
    else:
        if not len(p1) == 0:
            p1_outer = p1[0]
            p2_outer = p2[0]
            assert p1_outer == p2_outer
            if len(p1) == 2:
                p1_inner = p1[1]
                p2_inner = p2[1]
                assert p1_inner == p2_inner


# test datacube.index._products.ProductResources.extent function
@pytest.mark.usefixtures("load_extents")
def test_product_extent(index, extent_data):
    extent_meta, extent_slice = extent_data
    extent = index.products.extent_slice(dataset_type_id=11, start='1-1-2015',
                                         offset_alias='1Y').__geo_interface__['coordinates']
    assert_multipolygon(extent, extent_slice[0]['geometry']['coordinates'])

    # test different time representations
    extent = index.products.extent_slice(dataset_type_id=11, start='01-01-2015',
                                         offset_alias='1Y').__geo_interface__['coordinates']
    assert_multipolygon(extent, extent_slice[0]['geometry']['coordinates'])

    extent = index.products.extent_slice(dataset_type_id=11, start=datetime(year=2015, month=1, day=1),
                                         offset_alias='1Y').__geo_interface__['coordinates']
    assert_multipolygon(extent, extent_slice[0]['geometry']['coordinates'])

    extent = index.products.extent_slice(dataset_type_id=11, start=date(year=2015, month=1, day=1),
                                         offset_alias='1Y').__geo_interface__['coordinates']
    assert_multipolygon(extent, extent_slice[0]['geometry']['coordinates'])

    extent = index.products.extent_slice(dataset_type_id=11, start='2015',
                                         offset_alias='1Y').__geo_interface__['coordinates']
    assert_multipolygon(extent, extent_slice[0]['geometry']['coordinates'])

    extent = index.products.extent_slice(dataset_type_id=11, start='01-2015',
                                         offset_alias='1Y').__geo_interface__['coordinates']
    assert_multipolygon(extent, extent_slice[0]['geometry']['coordinates'])

    extent = index.products.extent_slice(dataset_type_id=11, start='1-1-2015',
                                         offset_alias='1Y').__geo_interface__['coordinates']
    assert_multipolygon(extent, extent_slice[0]['geometry']['coordinates'])


# test datacube.index._products.ProductResources.extent_periodic function
@pytest.mark.usefixtures("load_extents")
def test_yearly_extents(index, extent_data):
    extent_meta, extent_slice = extent_data
    extents = index.products.extent_periodic(dataset_type_id=11, start='01-01-2015', end='01-01-2017',
                                             offset_alias='1Y')
    for item in extents:
        assert_multipolygon(item['extent'].__geo_interface__['coordinates'],
                            extent_slice[0]['geometry']['coordinates'])


# test datacube.index._products.ProductResources.extent_periodic function
@pytest.mark.usefixtures("load_extents")
def test_monthly_extents(index, extent_data):
    extent_meta, extent_slice = extent_data
    extents = index.products.extent_periodic(dataset_type_id=32, start='01-01-2018', end='01-05-2018',
                                             offset_alias='1M')
    for item in extents:
        assert_multipolygon(item['extent'].__geo_interface__['coordinates'],
                            extent_slice[0]['geometry']['coordinates'])


# test datacube.index._products.ProductResources.ranges function
@pytest.mark.usefixtures("load_ranges")
def test_ranges(index):
    ranges = index.products.ranges('ls8_nbar_test')
    assert ranges['dataset_type_ref'] == 11
