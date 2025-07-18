# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime

import pytest
from odc.geo import CRS

from datacube.index import Index
from datacube.model import Range


@pytest.mark.parametrize("datacube_env_name", ("postgis",))
def test_index_environment(index: Index) -> None:
    assert index.environment.index_driver in ("postgis")


@pytest.mark.parametrize("datacube_env_name", ("postgis",))
def test_create_drop_spatial_index(index: Index) -> None:
    # Default spatial index for 4326
    assert list(index.spatial_indexes()) == [CRS("epsg:4326")]
    # WKT CRS which cannot be mapped to an EPSG number.
    assert not index.create_spatial_index(
        CRS(
            'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]]'
            ',PRIMEM["Weird",22.3],UNIT["Degree",0.017453292519943295]]'
        )
    )
    assert list(index.spatial_indexes()) == [CRS("epsg:4326")]
    assert index.create_spatial_index(CRS("epsg:3577"))
    assert index.create_spatial_index(CRS("WGS-84"))
    assert set(index.spatial_indexes()) == {CRS("epsg:3577"), CRS("epsg:4326")}
    assert set(index.spatial_indexes(refresh=True)) == {
        CRS("epsg:3577"),
        CRS("epsg:4326"),
    }
    assert index.drop_spatial_index(CRS("epsg:3577"))
    assert index.spatial_indexes() == [CRS("epsg:4326")]
    assert index.spatial_indexes(refresh=True) == [CRS("epsg:4326")]


@pytest.mark.parametrize("datacube_env_name", ("postgis",))
@pytest.mark.filterwarnings("ignore::antimeridian.FixWindingWarning")
def test_spatial_index_maintain(
    index: Index, ls8_eo3_product, eo3_ls8_dataset_doc
) -> None:
    index.create_spatial_index(CRS("EPSG:3577"))
    assert set(index.spatial_indexes(refresh=True)) == {
        CRS("EPSG:3577"),
        CRS("EPSG:4326"),
    }
    from datacube.index.hl import Doc2Dataset

    resolver = Doc2Dataset(index, products=[ls8_eo3_product.name], verify_lineage=False)
    ds, err = resolver(*eo3_ls8_dataset_doc)
    assert err is None and ds is not None
    ds = index.datasets.add(ds, False)
    assert ds
    index.datasets.archive([ds.id])
    index.datasets.purge([ds.id])
    # Can't really read yet, but seems to write at least


@pytest.mark.parametrize("datacube_env_name", ("postgis",))
@pytest.mark.filterwarnings("ignore::antimeridian.FixWindingWarning")
def test_spatial_index_populate(
    index: Index,
    ls8_eo3_product,
    wo_eo3_product,
    ls8_eo3_dataset,
    ls8_eo3_dataset2,
    ls8_eo3_dataset3,
    ls8_eo3_dataset4,
    wo_eo3_dataset,
) -> None:
    index.create_spatial_index(CRS("EPSG:3577"))
    assert set(index.spatial_indexes(refresh=True)) == {
        CRS("EPSG:3577"),
        CRS("EPSG:4326"),
    }
    assert (
        index.update_spatial_index(
            crses=[CRS("EPSG:4326")],
            dataset_ids=[ls8_eo3_dataset.id, ls8_eo3_dataset2.id],
        )
        == 2
    )
    assert index.update_spatial_index(product_names=[ls8_eo3_product.name]) == 8
    assert index.update_spatial_index() == 10
    assert (
        index.update_spatial_index(
            crses=[CRS("EPSG:4326")],
            product_names=[wo_eo3_product.name],
            dataset_ids=[ls8_eo3_dataset.id],
        )
        == 2
    )
    assert (
        index.update_spatial_index(
            product_names=[ls8_eo3_product.name], dataset_ids=[ls8_eo3_dataset.id]
        )
        == 8
    )


@pytest.mark.parametrize("datacube_env_name", ("postgis",))
def test_spatial_index_crs_validity(
    index: Index,
    ls8_eo3_product,
    ls8_eo3_dataset,
    africa_s2_eo3_product,
    africa_eo3_dataset,
) -> None:
    # TODO: potentially include africa_eo3_dataset2 in this test
    epsg4326 = CRS("EPSG:4326")
    epsg3577 = CRS("EPSG:3577")
    index.create_spatial_index(epsg3577)
    assert set(index.spatial_indexes(refresh=True)) == {epsg4326, epsg3577}
    assert index.update_spatial_index(crses=[epsg3577]) == 2


def spatial_index_crs_sanitise_helper() -> None:
    epsg4326 = CRS("EPSG:4326")
    epsg3857 = CRS("EPSG:3857")
    epsg3832 = CRS("EPSG:3832")
    from odc.geo.geom import polygon

    # 4326, crossing the Prime-Meridian, wound clockwise
    pm_4326_cw = polygon(
        (
            (-2.0, 25.0),
            (2.0, 25.0),
            (2.0, 23.0),
            (-2.0, 23.0),
            (-2.0, 25.0),
        ),
        crs=epsg4326,
    )

    # 4326, crossing the Prime-Meridian, wound counter-clockwise
    pm_4326_ccw = polygon(
        (
            (-2.0, 25.0),
            (-2.0, 23.0),
            (2.0, 23.0),
            (2.0, 25.0),
            (-2.0, 25.0),
        ),
        crs=epsg4326,
    )

    # 4326, crossing the Prime-Meridian, wound clockwise
    am_4326_ccw = polygon(
        (
            (178.0, 25.0),
            (178.0, 23.0),
            (-178.0, 23.0),
            (-178.0, 25.0),
            (178.0, 25.0),
        ),
        crs=epsg4326,
    )

    # FIXME: assert on something for this item.
    # 4326, crossing the Prime-Meridian, wound counter-clockwise
    am_4326_cw = polygon(  # noqa: F841
        (
            (178.0, 25.0),
            (-178.0, 25.0),
            (-178.0, 23.0),
            (178.0, 23.0),
            (178.0, 25.0),
        ),
        crs=epsg4326,
    )

    pm_3857 = polygon(
        (
            (-222638, 2875744),
            (-222638, 2632018),
            (222638, 2632018),
            (222638, 2632018),
            (-222638, 2875744),
        ),
        crs=epsg3857,
    )

    am_3857 = polygon(
        (
            (-19814869, 2875744),
            (19814869, 2632018),
            (-19814869, 2875744),
        ),
        crs=epsg3857,
    )

    am_3832 = polygon(
        (
            (3116945, 2857692),
            (3562223, 2857692),
            (3562223, 2615329),
            (3116945, 2615329),
            (3116945, 2857692),
        ),
        crs=epsg3832,
    )

    from datacube.drivers.postgis._spatial import sanitise_extent

    assert sanitise_extent(pm_4326_ccw, epsg4326) == pm_4326_ccw
    assert sanitise_extent(pm_4326_cw, epsg4326) == pm_4326_ccw
    assert sanitise_extent(pm_4326_ccw, epsg3857) == pm_4326_ccw.to_crs(epsg3857)

    sanitised = sanitise_extent(am_4326_ccw, epsg4326)
    assert sanitised.type == "MultiPolygon"
    # We used to check for the exact new geometry here, but antimeridian 0.4
    # changed the algorithm to use great circle instead of 2d for the splitting.
    assert len(list(sanitised.geoms)) == 2

    assert sanitise_extent(pm_4326_ccw, epsg3857) == pm_4326_ccw.to_crs(epsg3857)
    assert sanitise_extent(pm_3857, epsg3857).geom.equals_exact(pm_3857.geom, 1e-6)
    assert sanitise_extent(am_3857, epsg3857).type == "MultiPolygon"

    assert sanitise_extent(am_3832, epsg3832) == am_3832
    assert sanitise_extent(am_3832, epsg4326).type == "MultiPolygon"


def test_spatial_index_crs_sanitise() -> None:
    import warnings

    from antimeridian import FixWindingWarning

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FixWindingWarning)
        spatial_index_crs_sanitise_helper()


@pytest.mark.parametrize("datacube_env_name", ("postgis",))
def test_spatial_extent(
    index,
    ls8_eo3_dataset,
    ls8_eo3_dataset2,
    ls8_eo3_dataset3,
    ls8_eo3_dataset4,
    africa_s2_eo3_product,
    africa_eo3_dataset,
) -> None:
    # TODO: include africa_eo3_dataset2 in this test
    epsg4326 = CRS("EPSG:4326")
    epsg3577 = CRS("EPSG:3577")
    index.create_spatial_index(epsg3577)
    index.update_spatial_index(crses=[epsg3577])

    with pytest.raises(KeyError):
        index.products.spatial_extent("spaghetti_product")

    ext1 = index.datasets.spatial_extent([ls8_eo3_dataset.id], crs=epsg4326)
    ext2 = index.datasets.spatial_extent([ls8_eo3_dataset2.id], crs=epsg4326)
    ext12 = index.datasets.spatial_extent(
        [ls8_eo3_dataset.id, ls8_eo3_dataset2.id], crs=epsg4326
    )
    assert ext1 is not None and ext2 is not None and ext12 is not None
    assert ext1 == ext2
    assert ext12.difference(ext1).area < 0.001
    assert ls8_eo3_dataset.extent.to_crs(epsg4326).intersects(ext1)
    assert ls8_eo3_dataset.extent.to_crs(epsg4326).intersects(ext12)
    assert ls8_eo3_dataset2.extent.to_crs(epsg4326).intersects(ext2)
    assert ls8_eo3_dataset2.extent.to_crs(epsg4326).intersects(ext12)
    extau12 = index.datasets.spatial_extent(
        [ls8_eo3_dataset.id, ls8_eo3_dataset2.id], crs=epsg3577
    )
    extau12africa = index.datasets.spatial_extent(
        [ls8_eo3_dataset.id, ls8_eo3_dataset2.id, africa_eo3_dataset.id], crs=epsg3577
    )
    assert extau12.area < extau12africa.area
    ext3 = index.datasets.spatial_extent(ids=[ls8_eo3_dataset3.id], crs=epsg4326)
    ext1234 = index.datasets.spatial_extent(
        [
            ls8_eo3_dataset.id,
            ls8_eo3_dataset2.id,
            ls8_eo3_dataset3.id,
            ls8_eo3_dataset4.id,
        ],
        crs=epsg4326,
    )
    assert ext1.difference(ext1234).area < 0.001
    assert ext3.difference(ext1234).area < 0.001
    ext1_3577 = index.datasets.spatial_extent([ls8_eo3_dataset.id], crs=epsg3577)
    assert ext1_3577.intersects(ls8_eo3_dataset.extent._to_crs(epsg3577))

    ext_ls8 = index.products.spatial_extent(ls8_eo3_dataset.product, crs=epsg4326)
    assert ext_ls8 == ext1234
    ext_ls8 = index.products.spatial_extent(ls8_eo3_dataset.product.name, crs=epsg4326)
    assert ext_ls8 == ext1234


@pytest.mark.parametrize("datacube_env_name", ("postgis",))
def test_spatial_search(
    index, ls8_eo3_dataset, ls8_eo3_dataset2, ls8_eo3_dataset3, ls8_eo3_dataset4
) -> None:
    epsg4326 = CRS("EPSG:4326")
    epsg3577 = CRS("EPSG:3577")
    index.create_spatial_index(epsg3577)
    index.update_spatial_index(crses=[epsg3577])
    # Test old style lat/lon search
    dss = index.datasets.search(
        product=ls8_eo3_dataset.product.name,
        lat=Range(begin=-37.5, end=37.0),
        lon=Range(begin=148.5, end=149.0),
    )
    dssids = [ds.id for ds in dss]
    assert len(dssids) == 2
    assert ls8_eo3_dataset.id in dssids
    assert ls8_eo3_dataset2.id in dssids
    # Test polygons
    exact1_4326 = ls8_eo3_dataset.extent.to_crs(epsg4326)
    exact1_3577 = ls8_eo3_dataset.extent.to_crs(epsg3577)
    exact3_4326 = ls8_eo3_dataset3.extent.to_crs(epsg4326)
    exact3_3577 = ls8_eo3_dataset3.extent.to_crs(epsg3577)
    dssids = {
        ds.id
        for ds in index.datasets.search(
            product=ls8_eo3_dataset.product.name, geopolygon=exact1_4326
        )
    }
    assert len(dssids) == 2
    assert ls8_eo3_dataset.id in dssids
    assert ls8_eo3_dataset2.id in dssids
    assert (
        index.datasets.count(
            product=ls8_eo3_dataset.product.name, geopolygon=exact1_4326
        )
        == 2
    )
    dssids = [
        ds.id
        for ds in index.datasets.search(
            product=ls8_eo3_dataset.product.name, geopolygon=exact1_3577
        )
    ]
    assert len(dssids) == 2
    assert ls8_eo3_dataset.id in dssids
    assert ls8_eo3_dataset2.id in dssids
    assert (
        index.datasets.count(
            product=ls8_eo3_dataset.product.name, geopolygon=exact1_3577
        )
        == 2
    )
    dssids = [
        ds.id
        for ds in index.datasets.search(
            product=ls8_eo3_dataset.product.name, geopolygon=exact3_4326
        )
    ]
    assert len(dssids) == 2
    assert ls8_eo3_dataset3.id in dssids
    assert ls8_eo3_dataset3.id in dssids
    assert (
        index.datasets.count(
            product=ls8_eo3_dataset.product.name, geopolygon=exact3_4326
        )
        == 2
    )
    dssids = [
        ds.id
        for ds in index.datasets.search(
            product=ls8_eo3_dataset.product.name, geopolygon=exact3_3577
        )
    ]
    assert len(dssids) == 2
    assert ls8_eo3_dataset3.id in dssids
    assert ls8_eo3_dataset3.id in dssids
    assert (
        index.datasets.count(
            product=ls8_eo3_dataset.product.name, geopolygon=exact3_3577
        )
        == 2
    )


@pytest.mark.parametrize("datacube_env_name", ("postgis",))
def test_temporal_extents(
    index, ls8_eo3_dataset, ls8_eo3_dataset2, ls8_eo3_dataset3, ls8_eo3_dataset4
) -> None:
    start, end = index.products.temporal_extent(ls8_eo3_dataset.product)
    assert start == datetime.datetime(
        2013, 4, 4, 0, 58, 34, 682275, tzinfo=datetime.timezone.utc
    )
    assert end == datetime.datetime(
        2016, 5, 28, 23, 50, 59, 149573, tzinfo=datetime.timezone.utc
    )
    start2, end2 = index.products.temporal_extent(ls8_eo3_dataset.product.name)
    assert start == start2 and end == end2
    start2, end2 = index.datasets.temporal_extent(
        [
            ls8_eo3_dataset.id,
            ls8_eo3_dataset2.id,
            ls8_eo3_dataset3.id,
            ls8_eo3_dataset4.id,
        ]
    )
    assert start == start2 and end == end2
