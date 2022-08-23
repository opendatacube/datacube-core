# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest

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
    assert index.update_spatial_index(crses=[CRS("EPSG:4326")], dataset_ids=[ls8_eo3_dataset.id, ls8_eo3_dataset2.id]) == 2
    assert index.update_spatial_index(product_names=[ls8_eo3_product.name]) == 8
    assert index.update_spatial_index() == 10
    assert index.update_spatial_index(crses=[CRS("EPSG:4326")], product_names=[wo_eo3_product.name], dataset_ids=[ls8_eo3_dataset.id]) == 2
    assert index.update_spatial_index(product_names=[ls8_eo3_product.name], dataset_ids=[ls8_eo3_dataset.id]) == 8
