# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest

from datacube.model import Range
from datacube.index import Index
from datacube.utils.geometry import CRS


@pytest.mark.parametrize('datacube_env_name', ('experimental',))
def test_create_spatial_index(index: Index):
    assert list(index.spatial_indexes()) == []
    # WKT CRS which cannot be mapped to an EPSG number.
    assert not index.create_spatial_index(CRS(
        'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]]'
        ',PRIMEM["Weird",22.3],UNIT["Degree",0.017453292519943295]]'
    ))
    assert index.create_spatial_index(CRS("EPSG:4326"))
    assert list(index.spatial_indexes()) == [CRS("EPSG:4326")]
    assert index.create_spatial_index(CRS("EPSG:3577"))
    assert index.create_spatial_index(CRS("WGS-84"))
    assert set(index.spatial_indexes(refresh=True)) == {CRS("EPSG:3577"), CRS("EPSG:4326")}


@pytest.mark.parametrize('datacube_env_name', ('experimental',))
def test_spatial_index_maintain(index: Index, ls8_eo3_product, eo3_ls8_dataset_doc):
    index.create_spatial_index(CRS("EPSG:4326"))
    index.create_spatial_index(CRS("EPSG:3577"))
    assert set(index.spatial_indexes(refresh=True)) == {CRS("EPSG:3577"), CRS("EPSG:4326")}
    from datacube.index.hl import Doc2Dataset
    resolver = Doc2Dataset(index, products=[ls8_eo3_product.name], verify_lineage=False)
    ds, err = resolver(*eo3_ls8_dataset_doc)
    assert err is None and ds is not None
    ds = index.datasets.add(ds)
    assert ds
    index.datasets.archive([ds.id])
    index.datasets.purge([ds.id])
    # Can't really read yet, but seems to write at least


@pytest.mark.parametrize('datacube_env_name', ('experimental',))
def test_spatial_index_populate(index: Index,
                                ls8_eo3_product,
                                wo_eo3_product,
                                ls8_eo3_dataset, ls8_eo3_dataset2,
                                ls8_eo3_dataset3, ls8_eo3_dataset4,
                                wo_eo3_dataset):
    index.create_spatial_index(CRS("EPSG:4326"))
    index.create_spatial_index(CRS("EPSG:3577"))
    assert set(index.spatial_indexes(refresh=True)) == {CRS("EPSG:3577"), CRS("EPSG:4326")}
    assert index.update_spatial_index(
        crses=[CRS("EPSG:4326")],
        dataset_ids=[ls8_eo3_dataset.id, ls8_eo3_dataset2.id]
    ) == 2
    assert index.update_spatial_index(product_names=[ls8_eo3_product.name]) == 8
    assert index.update_spatial_index() == 10
    assert index.update_spatial_index(
        crses=[CRS("EPSG:4326")],
        product_names=[wo_eo3_product.name],
        dataset_ids=[ls8_eo3_dataset.id]
    ) == 2
    assert index.update_spatial_index(product_names=[ls8_eo3_product.name], dataset_ids=[ls8_eo3_dataset.id]) == 8


@pytest.mark.parametrize('datacube_env_name', ('experimental',))
def test_spatial_index_crs_validity(index: Index,
                                    ls8_eo3_product, ls8_eo3_dataset,
                                    africa_s2_eo3_product, africa_eo3_dataset):
    epsg4326 = CRS("EPSG:4326")
    epsg3577 = CRS("EPSG:3577")
    index.create_spatial_index(epsg4326)
    index.create_spatial_index(epsg3577)
    assert set(index.spatial_indexes(refresh=True)) == {epsg4326, epsg3577}
    assert index.update_spatial_index(crses=[epsg4326]) == 2
    assert index.update_spatial_index(crses=[epsg3577]) == 2


def test_spatial_index_crs_santise():
    epsg4326 = CRS("EPSG:4326")
    epsg3577 = CRS("EPSG:3577")
    from datacube.drivers.postgis._api import PostgisDbAPI
    from datacube.utils.geometry import polygon
    # EPSG:4326 polygons to be converted in EPSG:3577
    # Equal to entire valid region
    entire = polygon((
        (112.85, -43.7),
        (112.85, -9.86),
        (153.69, -9.86),
        (153.69, -43.7),
        (112.85, -43.7)), crs=epsg4326)
    # inside valid region
    valid = polygon((
        (130.15, -25.7),
        (130.15, -19.86),
        (135.22, -19.86),
        (135.22, -25.7),
        (130.15, -25.7)), crs=epsg4326)
    # completely outside valid region
    invalid = polygon((
        (-10.15, 25.7),
        (-10.15, 33.86),
        (5.22, 33.86),
        (5.22, 25.7
        (-10.15, 25.7)), crs=epsg4326)
    # intersects valid region
    partial = polygon((
        (103.15, -25.7),
        (103.15, -19.86),
        (135.22, -19.86),
        (135.22, -25.7),
        (103.15, -25.7)), crs=epsg4326)

    assert PostgisDbAPI._sanitise_extent(entire, epsg3577) == entire.to_crs("EPSG:3577")
    assert PostgisDbAPI._sanitise_extent(valid, epsg3577) == valid.to_crs("EPSG:3577")
    assert PostgisDbAPI._sanitise_extent(invalid, epsg3577) is None
    assert PostgisDbAPI._sanitise_extent(partial, epsg3577).area < partial.to_crs("EPSG:3577").area


@pytest.mark.parametrize('datacube_env_name', ('experimental',))
def test_spatial_search(index,
                   ls8_eo3_dataset, ls8_eo3_dataset2,
                   ls8_eo3_dataset3, ls8_eo3_dataset4):
    index.create_spatial_index(CRS("EPSG:4326"))
    index.create_spatial_index(CRS("EPSG:3577"))
    dss = index.datasets.search_eager(lat=Range(begin=-37.5, end=37.0), lon=Range(begin=148.5, end=149.0))
    dssids = [ds.id for ds in dss]
    assert len(dssids) == 2
    assert ls8_eo3_dataset.id in dssids
    assert ls8_eo3_dataset2.id in dssids
