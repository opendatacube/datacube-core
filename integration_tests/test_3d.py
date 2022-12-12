# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import numpy
import pytest
import yaml

try:
    from yaml import CSafeDumper as SafeDumper  # type: ignore
    from yaml import CSafeLoader as SafeLoader  # type: ignore
except ImportError:
    from yaml import SafeLoader, SafeDumper  # type: ignore

import rasterio
import xarray as xr
from affine import Affine
from datacube.api.core import Datacube
from datacube.utils import geometry

pytest.importorskip("dcio_example.xarray_3d")  # skip this test if 3d driver is not installed
_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.DEBUG)

PROJECT_ROOT = Path(__file__).parents[1]
GEDI_TEST_DATA = PROJECT_ROOT / "tests" / "data" / "lbg" / "gedi"

GEDI_PRODUCT = SimpleNamespace(
    dataset_types=GEDI_TEST_DATA / "GEDI02_B_3d_format.yaml",
    crs="EPSG:4326",
)
# Params common to 2D and 3D GEDI products

GEDI_PRODUCTS = {
    "2D": SimpleNamespace(
        name="gedi_l2b",
        measurements=["cover", "pai"],
        wavelengths=None,  # None for 2D products
        index_yaml=GEDI_TEST_DATA / "{product_id}" / "__{product_id}_gedi_l2b.yaml",
    ),
    "3D": SimpleNamespace(  # 3D with wavelength range
        name="gedi_l2b_{measurement}",
        measurements=["cover_z", "pai_z"],
        wavelengths=(20, 30),  # (5, 150),  # Heights in GEDI `*_z` products
        index_yaml=GEDI_TEST_DATA
        / "{product_id}"
        / "__{product_id}_gedi_l2b_{measurement}.yaml",
    ),
    "3D2": SimpleNamespace(  # 3D with single wavelength value
        name="gedi_l2b_{measurement}",
        measurements=["cover_z", "pai_z"],
        wavelengths=40,  # (5, 150),  # Heights in GEDI `*_z` products
        index_yaml=GEDI_TEST_DATA
        / "{product_id}"
        / "__{product_id}_gedi_l2b_{measurement}.yaml",
    ),
}

GEDI_PRODUCT_IDS = [
    SimpleNamespace(
        pid="GEDI02_B_2019228092851_O03828_T04236_02_001_01",
        size=(548, 415),
        affine=Affine(
            0.00027778,
            0.0,
            149.04036673536626,
            0.0,
            -0.00027778,
            -35.30377268593081,
        ),
        time=("2019-08", "2019-08"),
    ),
    SimpleNamespace(
        pid="GEDI02_B_2019294155401_O04856_T03859_02_001_01",
        size=(551, 420),
        affine=Affine(
            0.00027778,
            0.0,
            149.03966950632497,
            0.0,
            -0.00027778,
            -35.30265746130061,
        ),
        time=("2019-10", "2019-10"),
    ),
]
# Product IDs must be in chronological order


def custom_dumb_fuser(dst, src):
    dst[:] = src[:]


ignore_me = pytest.mark.xfail(
    True, reason="get_data/get_description still to be fixed in Unification"
)


@pytest.fixture(scope="module")
def original_data():
    """Load and cache the original NetCDF data from disk.

    This is the same data that gets indexed, hence the files have the suffix
    `.xarray_3d` because they are 3D NetCDF files which need the `xarray_3d` ODC
    driver (as opposed to the normal NetCDF driver). All available files get
    loaded once per session and returned as a dict of `{product_id_measurement:
    data_array}`.
    """
    data = {}
    for path in GEDI_TEST_DATA.rglob("*.xarray_3d"):
        datum = xr.open_dataset(path, engine="netcdf4")["array"]
        # Rename x/y to lat/lon because of the crs used
        # Index as file name without .xarray_3d extension
        data[path.name[:-10]] = datum.rename({"x": "longitude", "y": "latitude"})
    yield data


@pytest.fixture(scope="function", params=["3D", "3D2", "2D"])
def product_def(request):
    """GEDI 3D and 2D product parameters."""
    yield GEDI_PRODUCTS[request.param]


@pytest.fixture
def invalid_dataset_type_paths(tmpdir):
    """Prepare a series of invalid dataset type yamls.

    Returns: a dict of {<expected error message>: <yaml path>} for each invalid
    yaml.
    """
    with GEDI_PRODUCT.dataset_types.open() as fh:
        documents = [
            doc
            for doc in yaml.load_all(fh, Loader=SafeLoader)
            if doc["name"] == "gedi_l2b_cover_z"
        ]
    assert len(documents) == 1, "Test cannot alter product definition"
    invalid_docs = {}

    # Error: Missing extra_dimensions
    doc = deepcopy(documents[0])
    del doc["extra_dimensions"]  # Dropped item
    invalid_docs["extra_dimensions is not defined"] = doc

    # Error: extra_dimensions.name mismatch extra_dim.dimension
    doc = deepcopy(documents[0])
    doc["extra_dimensions"][0]["name"] = "invalid"  # Spurious name
    invalid_docs["Dimension z is not defined in extra_dimension"] = doc

    # Error: extra_dimensions.values length mismatch extra_dim.spectral_definition_map
    doc = deepcopy(documents[0])
    doc["measurements"][0]["spectral_definition"] = [
        {
            "wavelength": [w, w + 1],
            "response": [w / 10, (w + 1) / 10],
        }
        for w in doc["extra_dimensions"][0]["values"]
        + [-9999]  # Spurious trailing item
    ]
    invalid_docs[
        "spectral_definition should be the same length as values for extra_dim z"
    ] = doc

    # Error: mismatching spectral wavelength and response
    doc = deepcopy(documents[0])
    doc["measurements"][0]["spectral_definition"] = [
        {
            "wavelength": [w, w + 1, w + 2],  # Wavelength longer than response
            "response": [w / 10, (w + 1) / 10],
        }
        for w in doc["extra_dimensions"][0]["values"]
    ]
    invalid_docs[
        "spectral_definition_map: wavelength should be the same length as response"
    ] = doc

    invalid_paths = {}
    for name, invalid_doc in invalid_docs.items():
        path = Path(tmpdir) / (
            name.lower().replace(" ", "_").replace(".", "_") + ".yaml"
        )
        with path.open("w") as fh:
            yaml.dump(invalid_doc, fh, Dumper=SafeDumper)
        invalid_paths[name] = path
    yield invalid_paths


@pytest.fixture
def product_with_spectral_map(tmpdir):
    """Create a copy of input yaml with a spectral map.

    Returns the path string to the copy.
    """
    with GEDI_PRODUCT.dataset_types.open() as fh:
        documents = [
            doc
            for doc in yaml.load_all(fh, Loader=SafeLoader)
            if doc["name"] == "gedi_l2b_cover_z"
        ]
    assert len(documents) == 1, "Test cannot alter product definition"
    doc = deepcopy(documents[0])
    doc["measurements"][0]["spectral_definition"] = [
        {
            "wavelength": [w, w + 1],
            "response": [w / 10, (w + 1) / 10],
        }
        for w in doc["extra_dimensions"][0]["values"]
    ]
    path = Path(tmpdir) / GEDI_PRODUCT.dataset_types.name
    with path.open("w") as fh:
        yaml.dump(doc, fh, Dumper=SafeDumper)
    return path


@pytest.fixture(
    scope="function",
    params=["Without_spectral_map", "With_spectral_map"],
)
def dataset_types(product_with_spectral_map, request):
    """GEDI datasets types with/out spectral_map."""
    if request.param.startswith("Without"):
        yield GEDI_PRODUCT.dataset_types
    else:
        yield product_with_spectral_map


@pytest.mark.usefixtures("default_metadata_type")
def test_missing_extra_dimensions(clirunner, invalid_dataset_type_paths):
    """Test error on invalid product definition."""
    for expected_msg, path in invalid_dataset_type_paths.items():
        with pytest.raises(ValueError) as exc_info:
            clirunner(["-v", "product", "add", str(path)])
        assert str(exc_info.value).startswith(expected_msg)


@pytest.mark.usefixtures("default_metadata_type")
def test_indexing(clirunner, index, product_def):
    """Test indexing features for 2D and 3D products.

    A few no-op indexing commands are tested as well as a simple load with shape
    check only.
    """
    product_id = GEDI_PRODUCT_IDS[0]
    measurement = product_def.measurements[0]
    index_yaml = str(product_def.index_yaml).format(
        product_id=product_id.pid,
        measurement=measurement,
    )

    # Add the GEDI Dataset Types
    clirunner(["-v", "product", "add", str(GEDI_PRODUCT.dataset_types)])

    # Index the Datasets
    #  - do test run first to increase test coverage
    clirunner(["-v", "dataset", "add", "--dry-run", str(index_yaml)])

    #  - do actual indexing
    clirunner(
        [
            "-v",
            "dataset",
            "add",
            "--confirm-ignore-lineage",
            str(index_yaml),
        ]
    )

    # Test no-op update
    for policy in ["archive", "forget", "keep"]:
        clirunner(
            [
                "-v",
                "dataset",
                "update",
                "--dry-run",
                "--location-policy",
                policy,
                str(index_yaml),
            ]
        )

        # Test no changes needed update
        clirunner(
            [
                "-v",
                "dataset",
                "update",
                "--location-policy",
                policy,
                str(index_yaml),
            ]
        )

    dc = Datacube(index=index)
    check_open_with_dc_simple(dc, product_def, [product_id], measurement)


@pytest.mark.usefixtures("default_metadata_type")
def test_indexing_with_spectral_map(clirunner, index, dataset_types):
    """Test indexing features with spectral map."""
    product_id = GEDI_PRODUCT_IDS[0]
    product_def = GEDI_PRODUCTS["3D"]
    measurement = product_def.measurements[0]
    index_yaml = str(product_def.index_yaml).format(
        product_id=product_id.pid,
        measurement=measurement,
    )

    # Add the GEDI Dataset Types
    clirunner(["-v", "product", "add", str(dataset_types)])

    # Index the Dataset
    clirunner(["-v", "dataset", "add", '--confirm-ignore-lineage', str(index_yaml)])
    dc = Datacube(index=index)
    check_open_with_dc_simple(dc, product_def, [product_id], measurement)


@pytest.mark.usefixtures("default_metadata_type")
def test_end_to_end_multitime(clirunner, index, product_def, original_data):
    """Test simple indexing but for multiple measurements and wavelengths."""
    dc = Datacube(index=index)

    # Add the GEDI Dataset Types
    clirunner(["-v", "product", "add", str(GEDI_PRODUCT.dataset_types)])

    for idx, measurement in enumerate(product_def.measurements):
        for product_id in GEDI_PRODUCT_IDS:
            index_yaml = str(product_def.index_yaml).format(
                product_id=product_id.pid,
                measurement=measurement,
            )
            # Index the Datasets
            clirunner(["-v", "dataset", "add", '--confirm-ignore-lineage', str(index_yaml)])

        if idx == 0:  # Full check for the first measurement only
            # Check data for all product IDs
            check_open_with_dc_contents(
                dc, product_def, GEDI_PRODUCT_IDS, measurement, original_data
            )
            # check_open_with_grid_workflow(index)
            # Only test first product ID with dss
            check_load_via_dss(
                dc, product_def, GEDI_PRODUCT_IDS[:1], measurement, original_data
            )
        else:
            check_open_with_dc_simple(dc, product_def, GEDI_PRODUCT_IDS, measurement)


def check_loaded_vs_original(data, orig, product_def):
    """Check that the `data` against the original data from disk."""
    data_t = data.isel(time=0)  # Only 1 time slice for now
    if product_def.wavelengths:
        # 3D: Compare values for all loaded wavelengths
        wavelengths = (
            (product_def.wavelengths,) * 2
            if isinstance(product_def.wavelengths, int)
            else product_def.wavelengths
        )
        for wavelength in range(wavelengths[0], wavelengths[1] + 1, 5):
            data_w = data_t.sel(z=wavelength)
            orig_w = orig.sel(z=wavelength)
            assert numpy.array_equal(data_w.values, orig_w.values)
    else:
        # 2D: Compare values
        # TODO: When the bug preventing geobox for 2D is fixed, uncomment the
        # following line
        # assert numpy.array_equal(data_t.values, orig.values)
        _LOG.info(f"\n{str(numpy.array_equal(data_t.values, orig.values)):~^80}\n")


def load_with_dc(
    dc, product_def, product_id, measurement, time=None, datasets=None, dask_chunks=None
):
    """Load data with dc, with settable params.

    If `datasets` is specified, the no `product` is used in the load command.
    `dask_chunks` get passed as-is.
    """
    params = SimpleNamespace(
        measurements=[measurement],
        like=geometry.GeoBox(
            *product_id.size,
            product_id.affine,
            GEDI_PRODUCT.crs,
        ),
        dask_chunks=dask_chunks,
    )
    if time:
        params.time = time
    if product_def.wavelengths:
        params.z = product_def.wavelengths

    if datasets:
        params.datasets = datasets
    else:
        params.product = product_def.name.format(measurement=measurement)
    _LOG.info(f"DC Loading {params}")
    data = dc.load(**params.__dict__)
    _LOG.info(f"DC Loaded\n{data}\n{'-'*80}")
    return data


def check_open_with_dc_simple(dc, product_def, product_ids, measurement):
    """Check data can be loaded and has the right shape.

    Only the first of `product_ids` is tested. The actual contents of the loaded
    data are not checked, only their shape.
    """
    product_id = product_ids[0]
    data = load_with_dc(dc, product_def, product_id, measurement)
    expected = [len(product_ids), *product_id.size[::-1]]
    if product_def.wavelengths:
        wlen = (
            1
            if isinstance(product_def.wavelengths, int)
            else len(range(*product_def.wavelengths, 5)) + 1
        )
        expected.insert(1, wlen)
    assert list(data[measurement].shape) == expected


def check_open_with_dc_contents(
    dc, product_def, product_ids, measurement, original_data
):
    """Check simple and dask loads.

    Simple load is checked against the original file, loaded from disk. Lazy
    loading with `dask_chunks` is then compared to the simple load.
    """
    for product_id in product_ids:
        orig = original_data[f"{product_id.pid}_{measurement}"]
        data = load_with_dc(
            dc, product_def, product_id, measurement, time=product_id.time
        )
        check_loaded_vs_original(data[measurement], orig, product_def)

    # Use the last data from the for loop above, i.e., time slice is the last
    # one, assuming product_ids are in chronological order
    data_array = data[measurement]

    # Simpler checks, but using dask
    with rasterio.Env():
        dask_chunks = SimpleNamespace(time=1, latitude=200, longitude=200)
        # We will compare data at the intersection of 4 chunks
        target = SimpleNamespace(
            time=0, latitude=slice(150, 250), longitude=slice(150, 250)
        )
        if product_def.wavelengths:
            dask_chunks.z = 1
            target.z = 0
        lazy_data_array = load_with_dc(
            dc,
            product_def,
            product_id,
            measurement,
            time=product_id.time,
            dask_chunks=dask_chunks.__dict__,
        )[measurement]
        assert lazy_data_array.data.dask
        assert lazy_data_array.ndim == data_array.ndim
        # Checking the target area is not full of nodata
        assert dataarray_has_valid_data(data_array.isel(target.__dict__))
        assert lazy_data_array.isel(target.__dict__).equals(
            data_array.isel(target.__dict__)
        )


def dataarray_has_valid_data(da):
    """Check xarray has valid data."""
    return da.size and not (da.values == da.nodata).all()


def check_open_with_grid_workflow(index):
    """Not implemented"""
    pass


def check_load_via_dss(dc, product_def, product_ids, measurement, original_data):
    """Check dataset can be searched and loaded, and has the right shape.

    Only the first of `product_ids` is tested. The actual contents of the loaded
    data are not checked, only their shape.
    """
    product_id = product_ids[0]
    product = product_def.name.format(measurement=measurement)
    datasets = dc.find_datasets(product=product, time=product_id.time)
    assert len(datasets) > 0
    data = load_with_dc(
        dc,
        product_def,
        product_id,
        measurement,
        datasets=datasets,
    )
    expected = [len(product_ids), *product_id.size[::-1]]
    if product_def.wavelengths:
        wlen = (
            1
            if isinstance(product_def.wavelengths, int)
            else len(range(*product_def.wavelengths, 5)) + 1
        )
        expected.insert(1, wlen)
    assert list(data[measurement].shape) == expected
