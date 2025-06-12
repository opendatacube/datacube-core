# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pickle
from copy import deepcopy

import numpy
import pytest
from odc.geo import CRS, BoundingBox
from odc.geo.geom import polygon

from datacube.api.core import output_geobox
from datacube.model import (
    GridSpec,
    Measurement,
    MetadataType,
    Product,
    Range,
    ranges_overlap,
)
from datacube.storage import measurement_paths
from datacube.testutils import (
    mk_sample_dataset,
    mk_sample_product,
    suppress_deprecations,
)
from datacube.testutils.geom import AlbersGS
from datacube.utils.documents import InvalidDocException


def test_gridspec() -> None:
    with suppress_deprecations():
        gs = GridSpec(  # Coverage test of deprecated class
            crs=CRS("EPSG:4326"),
            tile_size=(1, 1),
            resolution=(-0.1, 0.1),
            origin=(10, 10),
        )
    poly = polygon(
        [(10, 12.2), (10.8, 13), (13, 10.8), (12.2, 10), (10, 12.2)],
        crs=CRS("EPSG:4326"),
    )
    cells = dict(list(gs.tiles_from_geopolygon(poly)))
    assert set(cells.keys()) == {(0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (2, 0), (2, 1)}
    assert numpy.isclose(
        cells[(2, 0)].coordinates["longitude"].values,
        numpy.linspace(12.05, 12.95, num=10),
    ).all()
    assert numpy.isclose(
        cells[(2, 0)].coordinates["latitude"].values,
        numpy.linspace(10.95, 10.05, num=10),
    ).all()

    # check geobox_cache
    cache = {}
    poly = gs.tile_geobox((3, 4)).extent
    ((c1, gbox1),) = list(gs.tiles_from_geopolygon(poly, geobox_cache=cache))
    ((c2, gbox2),) = list(gs.tiles_from_geopolygon(poly, geobox_cache=cache))

    assert c1 == (3, 4) and c2 == c1
    assert gbox1 is gbox2

    assert "4326" in str(gs)
    assert "4326" in repr(gs)
    assert gs == gs
    assert (gs == {}) is False


def test_gridspec_upperleft() -> None:
    """Test to ensure grid indexes can be counted correctly from bottom left or top left"""
    tile_bbox = BoundingBox(
        left=1934400.0,
        top=2414800.0,
        right=2084400.000,
        bottom=2264800.000,
        crs=CRS("EPSG:5070"),
    )
    bbox = BoundingBox(
        left=1934615, top=2379460, right=1937615, bottom=2376460, crs=CRS("EPSG:5070")
    )
    # Upper left - validated against WELD product tile calculator
    # http://globalmonitoring.sdstate.edu/projects/weld/tilecalc.php
    with suppress_deprecations():
        gs = GridSpec(
            crs=CRS("EPSG:5070"),
            tile_size=(-150000, 150000),
            resolution=(-30, 30),
            origin=(3314800.0, -2565600.0),
        )  # Coverage test  of deprecated class
    cells = dict(list(gs.tiles(bbox)))
    assert set(cells.keys()) == {(30, 6)}
    assert cells[(30, 6)].extent.boundingbox == tile_bbox

    with suppress_deprecations():
        gs = GridSpec(
            crs=CRS("EPSG:5070"),
            tile_size=(150000, 150000),
            resolution=(-30, 30),
            origin=(14800.0, -2565600.0),
        )
    cells = dict(list(gs.tiles(bbox)))
    assert set(cells.keys()) == {
        (30, 15)
    }  # WELD grid spec has 21 vertical cells -- 21 - 6 = 15
    assert cells[(30, 15)].extent.boundingbox == tile_bbox


def test_dataset_basics() -> None:
    ds = mk_sample_dataset([{"name": "a"}])
    assert ds == ds
    assert ds != "33"
    assert (ds == "33") is False
    assert str(ds) == repr(ds)

    ds = mk_sample_dataset([{"name": "a"}], uri=None, geobox=None)
    assert not ds.uri
    assert ds.uri_scheme == ""
    assert ds.crs is None
    assert ds.bounds is None
    assert ds.extent is None
    assert ds.transform is None


def test_dataset_measurement_paths() -> None:
    format_ = "GeoTiff"

    ds = mk_sample_dataset(
        [{"name": n, "path": n + ".tiff"} for n in "a b c".split(" ")],
        uri="file:///tmp/datataset.yml",
        format=format_,
    )

    assert ds.local_uri == ds.uri
    assert ds.legacy_uri() == ds.uri
    assert ds.uri_scheme == "file"
    assert ds.format == format_
    paths = measurement_paths(ds)

    for k, v in paths.items():
        assert v == "file:///tmp/" + k + ".tiff"

    ds._uris = []
    ds.uri = None
    assert ds.local_uri is None
    with pytest.raises(ValueError):
        measurement_paths(ds)


def test_product_basics() -> None:
    product = mk_sample_product("test_product")
    assert product.name == "test_product"
    assert "test_product" in str(product)
    assert "test_product" in repr(product)
    assert product == product
    assert product == mk_sample_product("test_product")
    assert not (product == mk_sample_product("other"))
    assert not (product == [()])
    assert hash(product) == hash(mk_sample_product("test_product"))
    assert "time" in dir(product.metadata)

    assert product.measurements == product.lookup_measurements()
    assert product.lookup_measurements(["red"]) == product.lookup_measurements("red")


def test_product_dimensions() -> None:
    product = mk_sample_product("test_product")
    with suppress_deprecations():
        assert product.grid_spec is None
    assert product.dimensions == ("time", "y", "x")

    product = mk_sample_product("test_product", with_grid_spec=True)
    with suppress_deprecations():
        assert product.grid_spec is not None
    assert product.dimensions == ("time", "y", "x")

    partial_storage = product.definition["storage"]
    partial_storage.pop("tile_size")
    product = mk_sample_product("tt", storage=partial_storage)
    with suppress_deprecations():
        assert product.grid_spec is None


def test_product_gridspec() -> None:
    product = mk_sample_product("old_product", with_grid_spec=True)
    with pytest.deprecated_call():
        assert product.grid_spec is not None
    assert product.to_dict()["tile_shape"] == (4000.0, 4000.0)

    storage = {
        "crs": "EPSG:3577",
        "resolution": 25,
        "tile_shape": {"x": 4000.0, "y": 4000.0},
    }
    product = mk_sample_product("new_product", storage=storage)
    assert product.grid_spec is not None
    assert not isinstance(product.grid_spec, GridSpec)
    assert product.to_dict()["resolution"].xy == (25, -25)


def test_product_nodata_nan() -> None:
    # When storing .nan to JSON in DB it becomes a string with value "NaN"
    # Make sure it is converted back to real NaN
    product = mk_sample_product(
        "test",
        measurements=[
            {"name": "_nan", "dtype": "float32", "nodata": "NaN"},
            {"name": "_inf", "dtype": "float32", "nodata": "Infinity"},
            {"name": "_neg_inf", "dtype": "float32", "nodata": "-Infinity"},
        ],
    )
    for m in product.measurements.values():
        assert isinstance(m.nodata, float)

    assert numpy.isnan(product.measurements["_nan"].nodata)
    assert product.measurements["_inf"].nodata == numpy.inf
    assert product.measurements["_neg_inf"].nodata == -numpy.inf


def test_product_scale_factor() -> None:
    product = mk_sample_product(
        "test", measurements=[{"name": "red", "scale_factor": 33, "add_offset": -5}]
    )
    assert product.validate(product.definition) is None
    assert product.measurements["red"].scale_factor == 33
    assert product.measurements["red"].add_offset == -5
    attrs = product.measurements["red"].dataarray_attrs()
    assert attrs["scale_factor"] == 33
    assert attrs["add_offset"] == -5


def test_product_load_hints() -> None:
    product = mk_sample_product(
        "test_product", load={"crs": "epsg:3857", "resolution": {"x": 10, "y": -10}}
    )

    assert "load" in product.definition
    assert Product.validate(product.definition) is None

    hints = product._extract_load_hints()
    assert hints["crs"] == CRS("epsg:3857")
    assert hints["resolution"].yx == (-10, 10)
    assert "align" not in hints

    product = mk_sample_product(
        "test_product",
        load={
            "crs": "epsg:3857",
            "align": {"x": 5, "y": 6},
            "resolution": {"x": 10, "y": -10},
        },
    )

    hints = product.load_hints()
    assert hints["output_crs"] == CRS("epsg:3857")
    assert hints["resolution"].yx == (-10, 10)
    assert hints["align"] == (6, 5)
    assert product.default_crs == CRS("epsg:3857")
    assert product.default_resolution.yx == (-10, 10)
    assert product.default_align == (6, 5)

    product = mk_sample_product(
        "test_product",
        load={
            "crs": "epsg:4326",
            "align": {"longitude": 0.5, "latitude": 0.6},
            "resolution": {"longitude": 1.2, "latitude": -1.1},
        },
    )

    hints = product.load_hints()
    assert hints["output_crs"] == CRS("epsg:4326")
    assert hints["resolution"].yx == (-1.1, 1.2)
    assert hints["align"] == (0.6, 0.5)

    # check it's cached
    assert product.load_hints() is product.load_hints()

    # check schema: crs and resolution are compulsory
    for k in ("resolution", "crs"):
        doc = deepcopy(product.definition)
        assert Product.validate(doc) is None

        doc["load"].pop(k)
        assert k not in doc["load"]

        with pytest.raises(InvalidDocException):
            Product.validate(doc)

    # check GridSpec leakage doesn't happen for fully defined gridspec
    product = mk_sample_product("test", with_grid_spec=True)
    with suppress_deprecations():
        assert product.grid_spec is not None
    assert product.load_hints() == {}

    # check for fallback into partially defined `storage:`
    product = mk_sample_product(
        "test", storage={"crs": "EPSG:3857", "resolution": {"x": 10, "y": -10}}
    )
    with suppress_deprecations():
        assert product.grid_spec is None
    assert product.default_resolution.yx == (-10, 10)
    assert product.default_crs == CRS("EPSG:3857")

    # check for fallback into partially defined `storage:`
    # no resolution -- no hints
    product = mk_sample_product("test", storage={"crs": "EPSG:3857"})
    with suppress_deprecations():
        assert product.grid_spec is None
    assert product.load_hints() == {}

    # check misspelled load hints
    product = mk_sample_product(
        "test_product",
        load={"crs": "epsg:4326", "resolution": {"longtude": 1.2, "latitude": -1.1}},
    )
    assert product.load_hints() == {}


def test_measurement() -> None:
    # Can create a measurement
    m = Measurement(name="t", dtype="uint8", nodata=255, units="1")

    # retrieve it's vital stats
    assert m.name == "t"
    assert m.dtype == "uint8"
    assert m.nodata == 255
    assert m.units == "1"

    # retrieve the information required for filling a DataArray
    assert m.dataarray_attrs() == {"nodata": 255, "units": "1"}

    # Can add a new attribute by name and ensure it updates the DataArray attrs too
    m["bob"] = 10
    assert m.bob == 10
    assert m.dataarray_attrs() == {"nodata": 255, "units": "1", "bob": 10}

    m["none"] = None
    assert m.none is None

    # Resampling method is special and *not* needed for DataArray attrs
    m["resampling_method"] = "cubic"
    assert "resampling_method" not in m.dataarray_attrs()

    # It's possible to copy and update a Measurement instance
    m2 = m.copy()
    assert m2.bob == 10
    assert m2.dataarray_attrs() == m.dataarray_attrs()

    assert repr(m2) == repr(m)

    # Must specify *all* required keys. name, dtype, nodata and units
    with pytest.raises(ValueError) as e:
        Measurement(name="x", units="1", nodata=0)

    assert "required keys missing:" in str(e.value)
    assert "dtype" in str(e.value)


def test_measurement_equality() -> None:
    m1 = Measurement(name="t", dtype="uint8", nodata=255, units="1")
    m2 = Measurement(name="t", dtype="uint8", nodata=255, units="1")
    assert m1 == m2
    g1 = AlbersGS.tile_geobox((15, -40))
    assert m1 != g1


@pytest.mark.parametrize("protocol", range(pickle.HIGHEST_PROTOCOL))
def test_measurement_pickling(protocol) -> None:
    m = Measurement(canonical_name="s", name="t", dtype="uint8", nodata=255, units="1")

    serialised_m = pickle.dumps(m, protocol=protocol)

    restored_m = pickle.loads(serialised_m)

    assert m == restored_m


def test_measurement_cloudpickle() -> None:
    import cloudpickle

    m = Measurement(canonical_name="s", name="t", dtype="uint8", nodata=255, units="1")
    serialised = cloudpickle.dumps(m)

    deserialised = cloudpickle.loads(serialised)
    assert m == deserialised


def test_output_geobox_load_hints() -> None:
    geobox0 = AlbersGS.tile_geobox((15, -40))

    geobox = output_geobox(
        load_hints={"output_crs": geobox0.crs, "resolution": geobox0.resolution},
        geopolygon=geobox0.extent,
    )
    assert geobox == geobox0


def test_like_geobox() -> None:
    geobox = AlbersGS.tile_geobox((15, -40))
    assert output_geobox(like=geobox) is geobox


def test_output_geobox_fail_paths() -> None:
    with pytest.raises(ValueError):
        output_geobox()

    with pytest.raises(ValueError):
        output_geobox(output_crs="EPSG:4326")  # need resolution as well

    # need bounds
    with pytest.raises(ValueError):
        output_geobox(output_crs="EPSG:4326", resolution=(1, 1))


def test_metadata_type() -> None:
    m = MetadataType(
        {
            "name": "eo",
            "dataset": {
                "id": ["id"],
                "label": ["ga_label"],
                "creation_time": ["creation_dt"],
                "measurements": ["image", "bands"],
                "sources": ["lineage", "source_datasets"],
                "format": ["format", "name"],
            },
        },
        dataset_search_fields={},
    )

    assert "eo" in str(m)
    assert "eo" in repr(m)
    assert m.name == "eo"
    assert m.description is None
    assert m.dataset_reader({}) is not None

    # again but without dataset_search_fields
    m = MetadataType(m.definition)
    assert "eo" in str(m)
    assert "eo" in repr(m)
    assert m.name == "eo"
    assert m.description is None
    assert m.dataset_reader({}) is not None


def test_ranges_overlap() -> None:
    assert not ranges_overlap(Range(begin=1, end=5), Range(begin=11, end=15))
    assert not ranges_overlap(Range(begin=1, end=5), Range(begin=5, end=11))
    assert not ranges_overlap(Range(begin=5, end=11), Range(begin=1, end=5))
    assert ranges_overlap(Range(begin=1, end=5), Range(begin=1, end=5))
    assert ranges_overlap(Range(begin=1, end=15), Range(begin=10, end=25))
    assert ranges_overlap(Range(begin=10, end=25), Range(begin=1, end=15))
    assert ranges_overlap(Range(begin=1, end=25), Range(begin=10, end=15))
    assert ranges_overlap(Range(begin=10, end=15), Range(begin=1, end=25))
