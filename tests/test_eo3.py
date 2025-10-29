# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from copy import deepcopy
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from affine import Affine

from datacube.index.eo3 import (
    EO3Grid,
    EOConversionError,
    EOGridsError,
    add_eo3_parts,
    convert_eo_dataset,
    convert_eo_product,
    eo3_grid_spatial,
    is_doc_eo3,
    is_doc_geo,
    prep_eo3,
)
from datacube.metadata._stacconverter import infer_eo_product
from datacube.model import Dataset
from datacube.testutils import mk_sample_product
from datacube.utils.documents import InvalidDocException, parse_yaml

SAMPLE_DOC = """---
$schema: https://schemas.opendatacube.org/dataset
id: 7d41a4d0-2ab3-4da1-a010-ef48662ae8ef
crs: "EPSG:3857"
grids:
    default:
       shape: [100, 200]
       transform: [10, 0, 100000, 0, -10, 200000, 0, 0, 1]
lineage:
  src_a: ['7cf53cb3-5da7-483f-9f12-6056e3290b4e']
  src_b:
    - 'f5b9f582-d5ff-43c0-a49b-ef175abe429c'
    - '7f8c6e8e-6f6b-4513-a11c-efe466405509'
  src_empty: []
...
"""
# Crosses lon=180 line in Pacific, taken from one the Landsat scenes
# https://landsat-pds.s3.amazonaws.com/c1/L8/074/071/LC08_L1TP_074071_20190622_20190704_01_T1/index.html
#
SAMPLE_DOC_180 = """---
$schema: https://schemas.opendatacube.org/dataset
id: f884df9b-4458-47fd-a9d2-1a52a2db8a1a
crs: "EPSG:32660"
grids:
    default:
       shape: [7811, 7691]
       transform: [30, 0, 618285, 0, -30, -1642485, 0, 0, 1]
    pan:
       shape: [15621, 15381]
       transform: [15, 0, 618292.5, 0, -15, -1642492.5, 0, 0, 1]
lineage: {}
...
"""


@pytest.fixture
def sample_doc():
    return parse_yaml(SAMPLE_DOC)


@pytest.fixture
def sample_doc_180():
    return parse_yaml(SAMPLE_DOC_180)


@pytest.fixture
def eo3_product(eo3_metadata):
    return mk_sample_product("eo3_product", metadata_type=eo3_metadata)


def test_grid_points() -> None:
    identity = Affine.identity()
    grid = EO3Grid({"shape": (11, 22), "transform": identity})

    pts = grid.points()
    assert len(pts) == 4
    assert pts == [(0, 0), (22, 0), (22, 11), (0, 11)]
    pts_ = grid.points(ring=True)
    assert len(pts_) == 5
    assert pts == pts_[:4]
    assert pts_[0] == pts_[-1]

    grid = EO3Grid({"shape": (11, 22), "transform": Affine.translation(100, 0)})
    pts = grid.points()
    assert pts == [(100, 0), (122, 0), (122, 11), (100, 11)]


def test_bad_grids() -> None:
    identity = Affine.identity()
    bad_grids: list[dict[str, Any]] = [
        {},
        # No Shape
        {
            "transform": identity,
        },
        # Non 2-d Shape (NB: geospatial dimensions only.  Other dimensions are handled elsewhere.)
        {
            "shape": (1024,),
            "transform": identity,
        },
        {
            "shape": (1024, 564, 256),
            "transform": identity,
        },
        # No Transform
        {
            "shape": (1024, 256),
        },
        # Formally invalid affine transform (must be 6 or 9 elements)
        {
            "shape": (1024, 256),
            "transform": [343.3],
        },
        {
            "shape": (1024, 256),
            "transform": [343, 23345, 234, 9, -65.3],
        },
        {
            "shape": (1024, 256),
            "transform": [343, 23345, 234, 9, -65.3, 1, 0],
        },
        {
            "shape": (1024, 256),
            "transform": [
                343,
                23345,
                234,
                9,
                -65.3,
                1,
                0,
                7435.24563,
                0.0001234,
                888.888,
                3,
                3,
                2,
            ],
        },
        # Formally invalid affine transform (all elements must be numbers)
        {"shape": (1024, 256), "transform": [343, 23345, 234, 9, -65.3, "six"]},
        # Formally invalid affine transform (in 9 element form, last 3 numbers must be 0,0,1)
        {
            "shape": (1024, 256),
            "transform": [343, 23345, 234, 9, -65.3, 1, 3, 3, 2],
        },
    ]
    for bad_grid in bad_grids:
        with pytest.raises(ValueError):
            EO3Grid(bad_grid)


def test_eo3_grid_spatial_nogrids() -> None:
    with pytest.raises(ValueError, match=r"grids.foo"):
        eo3_grid_spatial(
            {
                "crs": "EPSG:4326",
                "grids": {
                    "default": {
                        "shape": (1024, 256),
                        "transform": [343, 23345, 234, 9, -65.3, 1],
                    }
                },
            },
            grid_name="foo",
        )


def test_is_eo3(sample_doc, sample_doc_180) -> None:
    assert is_doc_eo3(sample_doc) is True
    assert is_doc_eo3(sample_doc_180) is True

    # If there's no schema field at all, it's treated as legacy eo.
    assert is_doc_eo3({}) is False
    assert is_doc_eo3({"crs": "EPSG:4326"}) is False
    assert is_doc_eo3({"crs": "EPSG:4326", "grids": {}}) is False

    with pytest.raises(ValueError, match=r"Unsupported dataset schema.*"):
        _ = is_doc_eo3({"$schema": "https://schemas.opendatacube.org/eo4"})


def test_is_geo(sample_doc, sample_doc_180) -> None:
    assert is_doc_geo(sample_doc) is True
    assert is_doc_geo(sample_doc_180) is True

    assert is_doc_geo({}) is False
    assert is_doc_geo({"crs": "EPSG:4326"}) is False
    assert is_doc_geo({"crs": "EPSG:4326", "extent": "dummy_extent"}) is True


def test_add_eo3(sample_doc, sample_doc_180, eo3_product) -> None:
    doc = add_eo3_parts(sample_doc)
    assert doc is not sample_doc
    ds = Dataset(eo3_product, doc)
    assert ds.crs == "EPSG:3857"
    assert ds.extent is not None
    assert ds.extent.crs == "EPSG:3857"
    assert ds.metadata.lat.begin < ds.metadata.lat.end
    assert ds.metadata.lon.begin < ds.metadata.lon.end

    doc = dict(**sample_doc, geometry=ds.extent.buffer(-1).json)

    ds2 = Dataset(eo3_product, add_eo3_parts(doc))
    assert ds2.crs == "EPSG:3857"
    assert ds2.extent is not None
    assert ds2.extent.crs == "EPSG:3857"
    assert ds2.metadata.lat.begin < ds2.metadata.lat.end
    assert ds2.metadata.lon.begin < ds2.metadata.lon.end
    assert ds.extent.contains(ds2.extent)

    doc = add_eo3_parts(sample_doc_180)
    assert doc is not sample_doc_180
    ds = Dataset(eo3_product, doc)
    assert ds.crs == "EPSG:32660"
    assert ds.extent is not None
    assert ds.extent.crs == "EPSG:32660"
    assert ds.metadata.lat.begin < ds.metadata.lat.end
    assert ds.metadata.lon.begin < 180 < ds.metadata.lon.end

    doc = dict(**sample_doc)
    doc.pop("crs")
    with pytest.raises(ValueError):
        add_eo3_parts(doc)

    doc = dict(**sample_doc)
    doc.pop("grids")
    with pytest.raises(ValueError):
        add_eo3_parts(doc)


def test_prep_eo3(sample_doc, sample_doc_180, eo3_metadata) -> None:
    rdr = eo3_metadata.dataset_reader(prep_eo3(sample_doc))
    assert rdr.grid_spatial is not None
    assert rdr.lat.end > rdr.lat.begin
    assert rdr.lon.end > rdr.lon.begin
    assert "src_a" in rdr.sources
    assert "src_b1" in rdr.sources
    assert "src_b2" in rdr.sources
    assert "src_empty" not in rdr.sources

    rdr = eo3_metadata.dataset_reader(prep_eo3(sample_doc_180))
    assert rdr.grid_spatial is not None
    assert rdr.sources == {}
    assert rdr.lat.end > rdr.lat.begin
    assert rdr.lon.end > rdr.lon.begin
    assert rdr.lon.begin < 180 < rdr.lon.end

    non_eo3_doc: dict[str, Any] = {}
    assert prep_eo3(non_eo3_doc, auto_skip=True) is non_eo3_doc

    with pytest.raises(ValueError):
        prep_eo3(non_eo3_doc)


def test_prep_eo3_idempotency(sample_doc, sample_doc_180) -> None:
    # without lineage
    call1 = prep_eo3(sample_doc, remap_lineage=False)
    call2 = prep_eo3(call1, remap_lineage=False)
    assert call1 == call2
    call1 = prep_eo3(sample_doc_180, remap_lineage=False)
    call2 = prep_eo3(call1, remap_lineage=False)
    assert call1 == call2

    # with lineage
    call1 = prep_eo3(sample_doc)
    call2 = prep_eo3(call1)
    assert call1 == call2
    call1 = prep_eo3(sample_doc_180)
    call2 = prep_eo3(call1)
    assert call1 == call2


def test_val_eo3_offset() -> None:
    from datacube.model.eo3 import validate_eo3_offset

    # Simple offsets
    validate_eo3_offset("foo", "bar", ["properties", "ns:foo"])
    with pytest.raises(InvalidDocException):
        validate_eo3_offset("foo", "bar", ["not_properties", "ns:foo"])
    with pytest.raises(InvalidDocException):
        validate_eo3_offset("foo", "bar", ["properties", "nested", "ns:foo"])
    # Compound offsets
    validate_eo3_offset(
        "foo", "bar", [["properties", "ns:foo1"], ["properties", "ns:foo2"]]
    )
    with pytest.raises(InvalidDocException):
        validate_eo3_offset(
            "foo", "bar", [["properties", "ns:foo"], ["not_properties", "ns:foo"]]
        )
    with pytest.raises(InvalidDocException):
        validate_eo3_offset(
            "foo", "bar", [["properties", "nested", "ns:foo"], ["properties", "ns:foo"]]
        )


def test_val_eo3_offsets() -> None:
    from datacube.model.eo3 import validate_eo3_offsets

    # Scalar types
    validate_eo3_offsets("foo", "bar", {"offset": ["properties", "ns:foo"]})
    validate_eo3_offsets(
        "foo", "bar", {"type": "integer", "offset": ["properties", "ns:foo"]}
    )
    # Range Types
    validate_eo3_offsets(
        "foo",
        "bar",
        {
            "type": "numeric-range",
            "min_offset": ["properties", "ns:foo_min"],
            "max_offset": ["properties", "ns:foo_max"],
        },
    )
    # Missing offsets
    with pytest.raises(InvalidDocException):
        validate_eo3_offsets(
            "foo",
            "bar",
            {
                "type": "numeric",
            },
        )
    with pytest.raises(InvalidDocException):
        validate_eo3_offsets(
            "foo",
            "bar",
            {
                "type": "numeric-range",
                "max_offset": ["properties", "ns:foo_max"],
            },
        )
    with pytest.raises(InvalidDocException):
        validate_eo3_offsets(
            "foo",
            "bar",
            {"type": "integer-range", "min_offset": ["properties", "ns:foo"]},
        )


def test_eo3_compatible_type() -> None:
    from datacube.model.eo3 import validate_eo3_compatible_type

    test_doc = {
        "name": "eo3_test",
        "description": "eo3 test doc with issues",
        "dataset": {
            "invalid_system_field": ["whatever", "wherever"],
            "grid_spatial": ["grid_spatial", "projection"],
        },
    }
    with pytest.raises(InvalidDocException) as e:
        validate_eo3_compatible_type(test_doc)
    assert "invalid_system_field" in str(e.value)


def test_geom_from_eo3_proj() -> None:
    from datacube.drivers.postgis._spatial import extract_geometry_from_eo3_projection

    assert (
        extract_geometry_from_eo3_projection(
            {
                "spatial_reference": "EPSG:4326",
            }
        )
        is None
    )
    assert (
        extract_geometry_from_eo3_projection(
            {
                "spatial_reference": "EPSG:4326",
                "geo_ref_points": {
                    "ll": {"x": 10.0, "y": 10.0},
                    "ul": {"x": 10.0, "y": 20.0},
                    "ur": {"x": 20.0, "y": 20.0},
                    "lr": {"x": 20.0, "y": 10.0},
                },
            }
        )
        is not None
    )


def test_eo1_dataset_conversion(
    eo_dataset_doc,
    ls5_nbar_product,
    ls8_fc_albers_dataset,
    ls5_nbar_dataset,
    monkeypatch,
) -> None:
    eo_ds = Dataset(infer_eo_product(eo_dataset_doc), eo_dataset_doc)
    assert not eo_ds.is_eo3
    compat_ds = convert_eo_dataset(eo_ds)
    assert compat_ds.is_eo3
    assert compat_ds.properties
    assert compat_ds.accessories
    assert compat_ds.properties["datetime"] == compat_ds.center_time.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    assert compat_ds.metadata_doc["lineage"]["source_datasets"] == {
        "level1": "ee983642-1cd3-11e6-aaba-a0000100fe80"
    }

    eo_ds = Dataset(ls5_nbar_product, eo_dataset_doc)
    expected_ds_doc = {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": "bbf3e21c-82b0-11e5-9ba1-a0000100fe80",
        "label": "LS5_TM_NBAR_P54_GANBAR01-002_090_084_19900302",
        "product": {"name": "ls5_nbar_scene"},
        "crs": "EPSG:28355",
        "geometry": None,
        "extent": {
            "lon": {
                "begin": 148.48815577279413,
                "end": 151.19156117169499,
            },
            "lat": {
                "begin": -35.61237326356207,
                "end": -33.588046887860685,
            },
        },
        "grid_spatial": {
            "projection": {
                "geo_ref_points": {
                    "ul": {
                        "x": 638000.0,
                        "y": 6276000.0,
                    },
                    "ur": {
                        "x": 880025.0,
                        "y": 6276000.0,
                    },
                    "ll": {
                        "x": 638000.0,
                        "y": 6057975.0,
                    },
                    "lr": {
                        "x": 880025.0,
                        "y": 6057975.0,
                    },
                },
                "zone": -55,
                "spatial_reference": "EPSG:28355",
            },
        },
        "grids": {
            "default": {
                "shape": (8721, 9681),
                "transform": (25.0, 0.0, 638000.0, 0.0, -25.0, 6276000.0),
            },
        },
        "properties": {
            "datetime": "1990-03-02 23:11:16",
            "odc:processing_datetime": "2015-03-22 01:49:21",
            "odc:product_family": "nbar",
            "dtr:start_datetime": "1990-03-02T23:11:04",
            "dtr:end_datetime": "1990-03-02T23:11:28",
            "eo:instrument": "TM",
            "eo:platform": "landsat-5",
            "odc:file_format": "GeoTiff",
            "title": "LS5_TM_NBAR_P54_GANBAR01-002_090_084_19900302",
            "landsat:wrs_path": 90,
            "landsat:wrs_row": 84,
        },
        "measurements": {
            "1": {
                "path": "product/scene01/LS5_TM_NBAR_P54_GANBAR01-002_090_084_19900302_B10.tif",
            },
            "2": {
                "path": "product/scene01/LS5_TM_NBAR_P54_GANBAR01-002_090_084_19900302_B20.tif",
            },
            "3": {
                "path": "product/scene01/LS5_TM_NBAR_P54_GANBAR01-002_090_084_19900302_B30.tif",
            },
        },
        "accessories": {
            "checksum:sha1": {"path": "package.sha1"},
        },
        "lineage": {
            "source_datasets": {
                "level1": "ee983642-1cd3-11e6-aaba-a0000100fe80",
            },
        },
    }
    assert convert_eo_dataset(eo_ds).metadata_doc == expected_ds_doc

    no_gs_doc = deepcopy(eo_dataset_doc)
    no_gs_doc["grid_spatial"] = {}
    no_gs_ds = Dataset(ls5_nbar_product, no_gs_doc)
    with pytest.raises(EOConversionError) as e:
        convert_eo_dataset(no_gs_ds)
    assert "is missing spatial information" in str(e.value)

    # remove grid information from measurements to seek from browse
    new_doc = deepcopy(eo_dataset_doc)
    new_doc["image"]["bands"] = {
        "1": {
            "path": "product/scene01/LS5_TM_NBAR_P54_GANBAR01-002_090_084_19900302_B10.tif"
        },
        "2": {
            "path": "product/scene01/LS5_TM_NBAR_P54_GANBAR01-002_090_084_19900302_B20.tif"
        },
        "3": {
            "path": "product/scene01/LS5_TM_NBAR_P54_GANBAR01-002_090_084_19900302_B30.tif"
        },
    }
    eo_ds = Dataset(ls5_nbar_product, new_doc)
    with pytest.raises(EOGridsError):
        convert_eo_dataset(eo_ds)

    new_doc["browse"] = {
        "full": {"path": "browse.fr.jpg", "cell_size": 25.0, "file_type": "image/jpg"}
    }
    eo_ds = Dataset(ls5_nbar_product, new_doc)
    converted_ds = convert_eo_dataset(eo_ds)
    assert converted_ds.metadata_doc["grids"] == {
        "default": {
            "shape": (8721.0, 9681.0),
            "transform": (25.0, 0.0, 638000.0, 0.0, -25.0, 6276000.0),
        },
    }
    assert converted_ds._gs == eo_ds._gs
    assert converted_ds.accessories == {
        "checksum:sha1": {"path": "package.sha1"},
        "thumbnail:full": {
            "path": "browse.fr.jpg",
        },
    }

    # fallback to product for grid info
    converted_ds = convert_eo_dataset(ls8_fc_albers_dataset)
    assert converted_ds.metadata_doc["grids"] == {
        "default": {
            "shape": (4000, 4000),
            "transform": (25.0, 0.0, -1.0e06, 0.0, -25.0, -1.8e06),
        }
    }

    # open data to get info from there
    # patch RasterDatasetDataSource to avoid opening s3 files
    class RDRMock:
        shape = (4000, 4000)
        transform = Affine(25.00, 0.00, -1000000.00, 0.00, -25.00, -1800000.00)
        crs = "EPSG:3577"

    mock_ctxm = MagicMock()
    mock_ctxm.__enter__.return_value = RDRMock()
    mock_open = MagicMock()
    mock_open.return_value = mock_ctxm

    # ensure grid_spatial isn't required
    del ls8_fc_albers_dataset.metadata_doc["grid_spatial"]
    with patch("datacube.storage._rio.RasterDatasetDataSource.open", mock_open):
        converted_ds = convert_eo_dataset(ls8_fc_albers_dataset, True)
    assert converted_ds.metadata_doc["grids"] == {
        "default": {
            "shape": (4000, 4000),
            "transform": (25.0, 0.0, -1.0e06, 0.0, -25.0, -1.8e06),
        }
    }

    # crs from map_projection utm
    converted_ds = convert_eo_dataset(ls5_nbar_dataset)
    assert converted_ds.metadata_doc["crs"] == "EPSG:32755"
    assert str(converted_ds.crs) == "EPSG:32755"


def test_eo1_product_conversion(ls5_nbar_product) -> None:
    converted_product = convert_eo_product(ls5_nbar_product)
    assert converted_product.name == "ls5_nbar_scene"
    assert converted_product.definition["metadata_type"] == "eo3"
    assert converted_product.metadata_doc == {
        "product": {"name": "ls5_nbar_scene"},
        "properties": {
            "eo:platform": "landsat-5",
            "odc:product_family": "nbar",
            "eo:instrument": "TM",
            "odc:file_format": "GeoTIFF",
        },
    }
    assert converted_product.measurements == ls5_nbar_product.measurements
