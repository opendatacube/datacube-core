# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from affine import Affine
import pytest
from datacube.utils.documents import parse_yaml, InvalidDocException
from datacube.testutils import mk_sample_product
from datacube.model import Dataset

from datacube.index.eo3 import (
    EO3Grid,
    prep_eo3,
    add_eo3_parts,
    is_doc_eo3, eo3_grid_spatial, is_doc_geo,
)

SAMPLE_DOC = '''---
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
'''

# Crosses lon=180 line in Pacific, taken from one the Landsat scenes
# https://landsat-pds.s3.amazonaws.com/c1/L8/074/071/LC08_L1TP_074071_20190622_20190704_01_T1/index.html
#
SAMPLE_DOC_180 = '''---
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
'''


@pytest.fixture
def sample_doc():
    return parse_yaml(SAMPLE_DOC)


@pytest.fixture
def sample_doc_180():
    return parse_yaml(SAMPLE_DOC_180)


@pytest.fixture
def eo3_product(eo3_metadata):
    return mk_sample_product("eo3_product", metadata_type=eo3_metadata)


def test_grid_points():
    identity = list(Affine.translation(0, 0))
    grid = EO3Grid({
        "shape": (11, 22),
        "transform": identity
    })

    pts = grid.points()
    assert len(pts) == 4
    assert pts == [(0, 0), (22, 0), (22, 11), (0, 11)]
    pts_ = grid.points(ring=True)
    assert len(pts_) == 5
    assert pts == pts_[:4]
    assert pts_[0] == pts_[-1]

    grid = EO3Grid({
        "shape": (11, 22),
        "transform": tuple(Affine.translation(100, 0))
    })
    pts = grid.points()
    assert pts == [(100, 0), (122, 0), (122, 11), (100, 11)]

    for bad in [{},
                dict(shape=(1, 1)),
                dict(transform=identity)]:
        with pytest.raises(ValueError):
            grid = EO3Grid(bad)


def test_bad_grids():
    identity = list(Affine.translation(0, 0))
    bad_grids = [
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
            "transform": [343, 23345, 234, 9, -65.3, 1, 0, 7435.24563, 0.0001234, 888.888, 3, 3, 2],
        },
        # Formally invalid affine transform (all elements must be numbers)
        {
            "shape": (1024, 256),
            "transform": [343, 23345, 234, 9, -65.3, "six"]
        },
        # Formally invalid affine transform (in 9 element form, last 3 numbers must be 0,0,1)
        {
            "shape": (1024, 256),
            "transform": [343, 23345, 234, 9, -65.3, 1, 3, 3, 2],
        },
    ]
    for bad_grid in bad_grids:
        with pytest.raises(ValueError):
            grid = EO3Grid(bad_grid)


def test_eo3_grid_spatial_nogrids():
    with pytest.raises(ValueError, match="grids.foo"):
        oo = eo3_grid_spatial(
            {
                "crs": "EPSG:4326",
                "grids": {
                    "default": {
                        "shape": (1024, 256),
                        "transform": [343, 23345, 234, 9, -65.3, 1],
                    }
                }
            },
            grid_name="foo"
        )


def test_is_eo3(sample_doc, sample_doc_180):
    assert is_doc_eo3(sample_doc) is True
    assert is_doc_eo3(sample_doc_180) is True

    # If there's no schema field at all, it's treated as legacy eo.
    assert is_doc_eo3({}) is False
    assert is_doc_eo3({'crs': 'EPSG:4326'}) is False
    assert is_doc_eo3({'crs': 'EPSG:4326', 'grids': {}}) is False

    with pytest.raises(ValueError, match="Unsupported dataset schema.*"):
        is_doc_eo3({'$schema': 'https://schemas.opendatacube.org/eo4'})


def test_is_geo(sample_doc, sample_doc_180):
    assert is_doc_geo(sample_doc) is True
    assert is_doc_geo(sample_doc_180) is True

    assert is_doc_geo({}) is False
    assert is_doc_geo({'crs': 'EPSG:4326'}) is False
    assert is_doc_geo({'crs': 'EPSG:4326', 'extent': "dummy_extent"}) is True


def test_add_eo3(sample_doc, sample_doc_180, eo3_product):
    doc = add_eo3_parts(sample_doc)
    assert doc is not sample_doc
    ds = Dataset(eo3_product, doc)
    assert ds.crs == 'EPSG:3857'
    assert ds.extent is not None
    assert ds.extent.crs == 'EPSG:3857'
    assert ds.metadata.lat.begin < ds.metadata.lat.end
    assert ds.metadata.lon.begin < ds.metadata.lon.end

    doc = dict(**sample_doc,
               geometry=ds.extent.buffer(-1).json)

    ds2 = Dataset(eo3_product, add_eo3_parts(doc))
    assert ds2.crs == 'EPSG:3857'
    assert ds2.extent is not None
    assert ds2.extent.crs == 'EPSG:3857'
    assert ds2.metadata.lat.begin < ds2.metadata.lat.end
    assert ds2.metadata.lon.begin < ds2.metadata.lon.end
    assert ds.extent.contains(ds2.extent)

    doc = add_eo3_parts(sample_doc_180)
    assert doc is not sample_doc_180
    ds = Dataset(eo3_product, doc)
    assert ds.crs == 'EPSG:32660'
    assert ds.extent is not None
    assert ds.extent.crs == 'EPSG:32660'
    assert ds.metadata.lat.begin < ds.metadata.lat.end
    assert ds.metadata.lon.begin < 180 < ds.metadata.lon.end

    doc = dict(**sample_doc)
    doc.pop('crs')
    with pytest.raises(ValueError):
        add_eo3_parts(doc)

    doc = dict(**sample_doc)
    doc.pop('grids')
    with pytest.raises(ValueError):
        add_eo3_parts(doc)


def test_prep_eo3(sample_doc, sample_doc_180, eo3_metadata):
    rdr = eo3_metadata.dataset_reader(prep_eo3(sample_doc))
    assert rdr.grid_spatial is not None
    assert rdr.lat.end > rdr.lat.begin
    assert rdr.lon.end > rdr.lon.begin
    assert 'src_a' in rdr.sources
    assert 'src_b1' in rdr.sources
    assert 'src_b2' in rdr.sources
    assert 'src_empty' not in rdr.sources

    rdr = eo3_metadata.dataset_reader(prep_eo3(sample_doc_180))
    assert rdr.grid_spatial is not None
    assert rdr.sources == {}
    assert rdr.lat.end > rdr.lat.begin
    assert rdr.lon.end > rdr.lon.begin
    assert rdr.lon.begin < 180 < rdr.lon.end

    non_eo3_doc = {}
    assert prep_eo3(None) is None
    assert prep_eo3(non_eo3_doc, auto_skip=True) is non_eo3_doc

    with pytest.raises(ValueError):
        prep_eo3(non_eo3_doc)


def test_val_eo3_offset():
    from datacube.model.eo3 import validate_eo3_offset
    # Simple offsets
    validate_eo3_offset("foo", "bar", ["properties", "ns:foo"])
    with pytest.raises(InvalidDocException):
        validate_eo3_offset("foo", "bar", ["not_properties", "ns:foo"])
    with pytest.raises(InvalidDocException):
        validate_eo3_offset("foo", "bar", ["properties", "nested", "ns:foo"])
    # Compound offsets
    validate_eo3_offset("foo", "bar", [["properties", "ns:foo1"], ["properties", "ns:foo2"]])
    with pytest.raises(InvalidDocException):
        validate_eo3_offset("foo", "bar", [["properties", "ns:foo"], ["not_properties", "ns:foo"]])
    with pytest.raises(InvalidDocException):
        validate_eo3_offset("foo", "bar", [["properties", "nested", "ns:foo"], ["properties", "ns:foo"]])


def test_val_eo3_offsets():
    from datacube.model.eo3 import validate_eo3_offsets
    # Scalar types
    validate_eo3_offsets("foo", "bar", {
        "offset": ["properties", "ns:foo"]
    })
    validate_eo3_offsets("foo", "bar", {
        "type": "integer",
        "offset": ["properties", "ns:foo"]
    })
    # Range Types
    validate_eo3_offsets("foo", "bar", {
        "type": "numeric-range",
        "min_offset": ["properties", "ns:foo_min"],
        "max_offset": ["properties", "ns:foo_max"],
    })
    # Missing offsets
    with pytest.raises(InvalidDocException):
        validate_eo3_offsets("foo", "bar", {
            "type": "numeric",
        })
    with pytest.raises(InvalidDocException):
        validate_eo3_offsets("foo", "bar", {
            "type": "numeric-range",
            "max_offset": ["properties", "ns:foo_max"],
        })
    with pytest.raises(InvalidDocException):
        validate_eo3_offsets("foo", "bar", {
            "type": "integer-range",
            "min_offset": ["properties", "ns:foo"]
        })


def test_eo3_compatible_type():
    from datacube.model.eo3 import validate_eo3_compatible_type
    test_doc = {
        "name": "eo3_test",
        "description": "eo3 test doc with issues",
        "dataset": {
            "invalid_system_field": ["whatever", "wherever"],
            "grid_spatial": ["grid_spatial", "projection"]
        }
    }
    with pytest.raises(InvalidDocException) as e:
        validate_eo3_compatible_type(test_doc)
    assert "invalid_system_field" in str(e.value)


def test_geom_from_eo3_proj():
    from datacube.drivers.postgis._spatial import extract_geometry_from_eo3_projection
    assert extract_geometry_from_eo3_projection({
        "spatial_reference": "EPSG:4326",
    }) is None
    assert extract_geometry_from_eo3_projection({
        "spatial_reference": "EPSG:4326",
        "geo_ref_points": {
            "ll": {"x": 10.0, "y": 10.0},
            "ul": {"x": 10.0, "y": 20.0},
            "ur": {"x": 20.0, "y": 20.0},
            "lr": {"x": 20.0, "y": 10.0},
        }
    }) is not None
