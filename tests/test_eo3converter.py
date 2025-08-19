# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
# pylint: disable=unused-argument,unused-variable,missing-module-docstring,wrong-import-position,import-error
# pylint: disable=redefined-outer-name,protected-access,import-outside-toplevel

import os
import uuid
from typing import Any

import pystac
import pytest
from affine import Affine
from odc.geo.geom import Geometry
from odc.stac._mdtools import RasterCollectionMetadata, has_proj_ext, has_raster_ext
from pystac.extensions.eo import EOExtension
from pystac.extensions.item_assets import ItemAssetsExtension
from pystac.extensions.projection import ProjectionExtension
from toolz import dicttoolz

from datacube.metadata import ds2stac, infer_dc_product, stac2ds
from datacube.metadata._eo3converter import _compute_uuid, _item_to_ds
from datacube.model import Dataset, Product
from datacube.testutils.io import native_geobox

from .common import NO_WARN_CFG, STAC_CFG, mk_stac_item


def test_infer_product_collection(
    sentinel_stac_collection: pystac.Collection,
    sentinel_stac_ms_with_raster_ext: pystac.Item,
) -> None:
    assert has_raster_ext(sentinel_stac_collection) is True
    product = infer_dc_product(sentinel_stac_collection)
    assert product.stac is not None
    assert product.measurements["SCL"].dtype == "uint8"
    assert product.measurements["SCL"].get("band") is None
    # check aliases from eo extension
    assert product.canonical_measurement("red") == "B04"
    assert product.canonical_measurement("green") == "B03"
    assert product.canonical_measurement("blue") == "B02"

    # check band2grid
    md: RasterCollectionMetadata = product.stac
    b2g = md.band2grid
    assert b2g["B02"] == "default"
    assert b2g["B01"] == "g60"
    assert set(b2g.values()) == {"default", "g20", "g60"}

    # Check that we can use product derived this way on an Item
    item = sentinel_stac_ms_with_raster_ext.clone()

    ds = _item_to_ds(item, product)
    geobox = native_geobox(ds, basis="B02")
    assert geobox.shape == (10980, 10980)
    assert geobox.crs == "EPSG:32606"
    assert native_geobox(ds, basis="B01").shape == (1830, 1830)

    # Check unhappy path
    collection = sentinel_stac_collection.clone()
    item_assets = getattr(collection, "item_assets", None)
    if item_assets is not None:
        # newer pystac
        item_assets.clear()
    else:
        collection.stac_extensions.remove(ItemAssetsExtension.get_schema_uri())

    with pytest.raises(ValueError):
        infer_dc_product(collection)

    # Test bad overload
    with pytest.raises(TypeError):
        infer_dc_product([])


def test_infer_product_item(sentinel_stac_ms: pystac.Item) -> None:
    item = sentinel_stac_ms

    assert item.collection_id in STAC_CFG

    product = infer_dc_product(item, STAC_CFG)

    assert product.measurements["SCL"].dtype == "uint8"
    assert product.measurements["visual"].dtype == "uint8"
    # check aliases from eo extension
    assert product.canonical_measurement("red") == "B04"
    assert product.canonical_measurement("green") == "B03"
    assert product.canonical_measurement("blue") == "B02"
    # check aliases from config
    assert product.canonical_measurement("rededge1") == "B05"
    assert product.canonical_measurement("rededge2") == "B06"
    assert product.canonical_measurement("rededge3") == "B07"

    assert product.stac is not None and set(product.stac.band2grid) == set(
        product.measurements
    )

    _stac = dicttoolz.dissoc(sentinel_stac_ms.to_dict(), "collection")
    item_no_collection = pystac.Item.from_dict(_stac)
    assert item_no_collection.collection_id is None

    product = infer_dc_product(item_no_collection)


def test_infer_product_raster_ext(
    sentinel_stac_ms_with_raster_ext: pystac.Item,
) -> None:
    item = sentinel_stac_ms_with_raster_ext.clone()
    assert has_raster_ext(item) is True
    product = infer_dc_product(item)

    assert product.measurements["SCL"].dtype == "uint8"
    assert product.measurements["visual"].dtype == "uint8"
    assert product.measurements["visual_2"].dtype == "uint8"
    assert product.measurements["visual_2"].band == 2
    assert product.measurements["visual_3"].band == 3

    # check aliases from eo extension
    assert product.canonical_measurement("red") == "B04"
    assert product.canonical_measurement("green") == "B03"
    assert product.canonical_measurement("blue") == "B02"
    assert product.stac is not None and set(product.stac.band2grid) | {
        "visual_2",
        "visual_3",
    } == set(product.measurements)


def test_item_to_ds(sentinel_stac_ms: pystac.Item) -> None:
    item0 = sentinel_stac_ms
    item = item0.clone()

    assert item.collection_id in STAC_CFG

    product = infer_dc_product(item, STAC_CFG)
    ds = _item_to_ds(item, product)

    assert set(ds.measurements) == set(product.measurements)
    assert ds.crs is not None
    assert ds.metadata.lat is not None
    assert ds.metadata.lon is not None
    assert ds.center_time is not None

    # this checks property remap, without changing
    # key names .platform would be None
    assert ds.metadata.platform == "Sentinel-2B"

    dss = list(stac2ds(iter([item, item, item]), STAC_CFG))
    assert len(dss) == 3
    assert len({id(ds.product) for ds in dss}) == 1

    # Test missing band case
    item = item0.clone()
    item.assets.pop("B01")
    ds = _item_to_ds(item, product, STAC_CFG)

    # Test no eo extension case
    item = item0.clone()
    item.stac_extensions.remove(EOExtension.get_schema_uri())
    product = infer_dc_product(item, STAC_CFG)
    with pytest.raises(ValueError):
        product.canonical_measurement("green")

    # Test multiple CRS path
    item = item0.clone()
    ProjectionExtension.ext(item.assets["B01"]).epsg = 3857
    assert ProjectionExtension.ext(item.assets["B01"]).crs_string == "EPSG:3857"
    infer_dc_product(item, NO_WARN_CFG)


def test_item_to_ds_no_proj(sentinel_stac_ms: pystac.Item) -> None:
    item0 = sentinel_stac_ms
    item = item0.clone()
    item.stac_extensions.remove(ProjectionExtension.get_schema_uri())
    del item.properties["proj:code"]
    assert has_proj_ext(item) is False

    product = infer_dc_product(item, STAC_CFG)

    assert item.geometry is not None
    geom = Geometry(item.geometry, "EPSG:4326")
    ds = _item_to_ds(item, product, STAC_CFG)
    assert ds.crs == "EPSG:4326"
    assert ds.extent is not None
    assert ds.extent.contains(geom)
    assert native_geobox(ds).shape == (1, 1)


def test_item_uuid() -> None:
    item1 = mk_stac_item("id1", custom_property=1)
    item2 = mk_stac_item("id2")

    # Check determinism
    assert _compute_uuid(item1) == _compute_uuid(item1)
    assert _compute_uuid(item2) == _compute_uuid(item2)
    assert _compute_uuid(item1) != _compute_uuid(item2)

    # Check random case
    assert _compute_uuid(item1, "random").version == 4
    assert _compute_uuid(item1, "random") != _compute_uuid(item1, "random")

    # Check "native" mode
    _id = uuid.uuid4()
    assert _compute_uuid(mk_stac_item(str(_id)), "native") == _id
    assert _compute_uuid(mk_stac_item(str(_id)), "auto") == _id

    # Check that extras are used
    id1 = _compute_uuid(item1, extras=["custom_property", "missing_property"])
    id2 = _compute_uuid(item1)

    assert id1.version == 5
    assert id2.version == 5
    assert id1 != id2


def test_issue_n6(usgs_landsat_stac_v1: pystac.Item) -> None:
    expected_bands = {
        "blue",
        "coastal",
        "green",
        "nir08",
        "red",
        "swir16",
        "swir22",
        "qa_aerosol",
        "qa_pixel",
        "qa_radsat",
    }
    p = infer_dc_product(usgs_landsat_stac_v1)
    assert set(p.measurements) == expected_bands


def test_partial_proj(partial_proj_stac: pystac.Item) -> None:
    (ds,) = list(stac2ds([partial_proj_stac]))
    assert ds.metadata_doc["grids"]["default"]["shape"] == (1, 1)


def test_noassets_case(no_bands_stac: Any) -> None:
    (ds,) = stac2ds([no_bands_stac])
    assert len(ds.measurements) == 0


def test_product_cache(sentinel_stac_ms: pystac.Item) -> None:
    item = sentinel_stac_ms
    # simulate a product that was not created via infer_dc_product
    # (and therefore did not have the stac attr set)
    product = infer_dc_product(item, STAC_CFG)
    product._stac = None
    # make sure it doesn't error when product_cache is provided
    (ds,) = stac2ds([item], STAC_CFG, {product.name: product})
    assert ds.id


def test_values_converted(s2_l2a_stac, s2_l2a_product) -> None:
    # check that values that need to be converted are correct
    (ds,) = stac2ds([s2_l2a_stac], STAC_CFG, {s2_l2a_product.name: s2_l2a_product})
    assert ds.id
    # instruments list converted to string
    assert ds.metadata.instrument == "MSI"
    # missing properties are filled in
    assert ds.metadata.format == "GeoTIFF"
    # aliased measurements are converted to their canonical names
    assert set(s2_l2a_product.measurements.keys()).issubset(ds.measurements.keys())


def test_accessories(sentinel_stac_ms: pystac.Item) -> None:
    # check accessories are correctly retrieved from assets
    expected_accs = [
        "preview",
        "safe-manifest",
        "granule-metadata",
        "inspire-metadata",
        "product-metadata",
        "datastrip-metadata",
        "tilejson",
        "rendered_preview",
    ]
    item = sentinel_stac_ms
    product = infer_dc_product(item, STAC_CFG)
    ds = _item_to_ds(item, product, STAC_CFG)
    assert set(ds.metadata_doc["accessories"].keys()) == set(expected_accs)


def test_ds2stac(eo3_dataset: Dataset):
    assert eo3_dataset.uri is not None
    output_stac = ds2stac(eo3_dataset).to_dict()
    assert output_stac["properties"]["instruments"] == ["oli", "tirs"]
    assert set(output_stac["assets"].keys()) == set(
        list(eo3_dataset.measurements.keys()) + list(eo3_dataset.accessories.keys())
    )
    assert output_stac["links"] == [
        {
            "rel": "self",
            "href": os.path.abspath(eo3_dataset.uri).replace(
                "odc-metadata.yaml", "stac-item.json"
            ),
            "type": "application/json",
        },
        {
            "rel": "odc_yaml",
            "href": eo3_dataset.uri,
            "type": "text/yaml",
            "title": "ODC Dataset YAML",
        },
    ]


def test_ds2stac_links(eo3_dataset: Dataset):
    eo3_dataset.uri = None
    output_stac = ds2stac(
        eo3_dataset,
        self_url="https://localhost/stac/eo3_dataset.json",
        stac_url="https://localhost/",
    ).to_dict()
    assert output_stac["links"] == [
        {
            "rel": "self",
            "href": "https://localhost/stac/eo3_dataset.json",
            "type": "application/json",
        },
        {
            "rel": "collection",
            "href": f"https://localhost/stac/collections/{eo3_dataset.product.name}",
        },
        {
            "title": "ODC Product Overview",
            "rel": "product_overview",
            "type": "text/html",
            "href": f"https://localhost/product/{eo3_dataset.product.name}",
        },
        {
            "title": "ODC Dataset Overview",
            "rel": "alternative",
            "type": "text/html",
            "href": f"https://localhost/dataset/{eo3_dataset.id}",
        },
    ]


def test_sources(ds_legacy_sources: Dataset, ds_ext_lineage: Dataset):
    assert ds2stac(ds_legacy_sources).to_dict()["properties"]["odc:lineage"] == {
        "level1": ["b5f234fe-bba8-5483-9bc0-250360d429cf"]
    }
    assert ds2stac(ds_ext_lineage).to_dict()["properties"]["odc:lineage"] == {
        "level1": ["b5f234fe-bba8-5483-9bc0-250360d429cf"]
    }


def test_roundtrip(eo3_dataset: Dataset, eo3_product: Product):
    original = eo3_dataset
    roundtrip = _item_to_ds(ds2stac(eo3_dataset), eo3_product)
    orig_doc = original.metadata_doc
    rt_doc = roundtrip.metadata_doc

    assert set(rt_doc.keys()) == set(orig_doc.keys())
    assert set(roundtrip.properties.keys()) == set(original.properties.keys())

    assert len(roundtrip.grids) == 2
    assert (
        list(roundtrip.grids["default"]["shape"]) == original.grids["default"]["shape"]
    )
    # assert list(roundtrip.grids["default"]["transform"]) == original.grids["default"]["transform"][:6]
    assert (
        roundtrip.grids["default"]["transform"]
        == Affine(*original.grids["default"]["transform"]).to_shapely()
    )
    # there's no way to conserve grid names when converting to stac, but values should still be the same
    assert (
        list(roundtrip.grids["g15"]["shape"]) == original.grids["panchromatic"]["shape"]
    )
    assert (
        roundtrip.grids["g15"]["transform"]
        == Affine(*original.grids["panchromatic"]["transform"]).to_shapely()
    )

    assert roundtrip.crs == original.crs
    assert roundtrip.extent.area == pytest.approx(original.extent.area, abs=0.001)
    assert all(
        x == pytest.approx(y)
        for x, y in zip(
            rt_doc["geometry"]["coordinates"][0], orig_doc["geometry"]["coordinates"][0]
        )
    )

    assert set(roundtrip.measurements.keys()) == set(original.measurements.keys())
    assert roundtrip.measurements["nbar_panchromatic"]["grid"] == "g15"
    assert set(roundtrip.accessories.keys()) == set(original.accessories.keys())
