# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
import uuid
from unittest.mock import MagicMock, patch

import numpy
import pytest
from odc.geo import CRS, Resolution
from odc.geo.gridspec import GridSpec

from datacube.api.grid_workflow import GridWorkflow, GridWorkflowException
from datacube.testutils import mk_sample_product


class PickleableMock(MagicMock):
    def __reduce__(self):
        return (MagicMock, ())


def mk_fake_index(products, datasets):
    fakeindex = PickleableMock()
    fakeindex._db = None

    fakeindex.products.get_by_name = lambda name: products.get(name, None)

    fakeindex.datasets.get_field_names.return_value = ["time"]  # permit query on time
    fakeindex.datasets.search_eager.return_value = datasets

    return fakeindex


@pytest.fixture
def fake_index():
    return mk_fake_index(
        products={
            "with_gs": mk_sample_product("with_gs", with_grid_spec=True),
            "without_gs": mk_sample_product("without_gs", with_grid_spec=False),
        },
        datasets=[],
    )


def test_create_gridworkflow_creation_with_product(fake_index):
    index = fake_index

    # need product or grispec
    with pytest.raises(GridWorkflowException):
        GridWorkflow(index)

    # Can't specify a product that doesn't exist.
    with pytest.raises(GridWorkflowException):
        GridWorkflow(index, product="no-such-product")

    # Error out if a production without a GridSpec is specified
    assert fake_index.products.get_by_name("without_gs") is not None
    assert fake_index.products.get_by_name("without_gs").grid_spec is None

    with pytest.raises(GridWorkflowException):
        GridWorkflow(index, product="without_gs")

    # Able to create a Grid Workflow with a Product that has a GridSpec
    product = fake_index.products.get_by_name("with_gs")
    assert product is not None
    assert product.grid_spec is not None
    gw = GridWorkflow(index, product="with_gs")
    assert gw.grid_spec is product.grid_spec


@pytest.fixture
def sample_grid_workflow():
    crs = CRS("EPSG:3577")
    pixel = 10  # square pixel linear dimension in crs units
    grid = 10  # size of a single tile in pixels
    gridspec = GridSpec(crs=crs, tile_shape=(grid, grid), resolution=Resolution(pixel))
    fakedataset = MagicMock()
    fakedataset.extent = gridspec.tile_geobox((1, -2)).extent
    fakedataset.center_time = datetime.datetime(2001, 2, 15)
    fakedataset.id = uuid.uuid4()
    fakeindex = PickleableMock()
    fakeindex._db = None
    fakeindex.datasets.get_field_names.return_value = ["time"]
    fakeindex.products.get_field_names.return_value = ["time"]
    fakeindex.datasets.search_eager.return_value = [fakedataset]
    gw = GridWorkflow(fakeindex, gridspec)
    gw.index = fakeindex  # Need to force the fake index
    return gw, gridspec, fakedataset, fakeindex


def test_gridworkflow_str_repr(sample_grid_workflow):
    gw, _, _, _ = sample_grid_workflow
    assert len(str(gw)) > 0
    assert len(repr(gw)) > 0


def test_gridworkflow_cell_observations(sample_grid_workflow):
    gw, gridspec, _, _ = sample_grid_workflow
    query = {
        "product": "fake_product_name",
        "time": ("2001-1-1 00:00:00", "2001-3-31 23:59:59"),
    }
    assert list(gw.cell_observations(**query).keys()) == [(1, -2)]
    assert list(
        gw.cell_observations(
            **query, geopolygon=gridspec.tile_geobox((1, -2)).extent
        ).keys()
    ) == [(1, -2)]


def test_gridworkflow_cell_observations_errors(sample_grid_workflow):
    gw, gridspec, _, _ = sample_grid_workflow
    query = {
        "product": "fake_product_name",
        "time": ("2001-1-1 00:00:00", "2001-3-31 23:59:59"),
    }
    # It's invalid to supply tile_buffer and geopolygon at the same time
    with pytest.raises(GridWorkflowException) as e:
        list(
            gw.cell_observations(
                **query,
                tile_buffer=(1, 1),
                geopolygon=gridspec.tile_geobox((1, -2)).extent,
            ).keys()
        )
    assert str(e.value) == "Cannot process tile_buffering and geopolygon together."


def test_gridworkflow_list_tiles_unpadded(sample_grid_workflow):
    gw, _, _, _ = sample_grid_workflow
    query = {
        "product": "fake_product_name",
        "time": ("2001-1-1 00:00:00", "2001-3-31 23:59:59"),
    }
    assert len(gw.list_tiles(**query)) == 1


def test_gridworkflow_list_tiles_padded(sample_grid_workflow):
    gw, _, _, _ = sample_grid_workflow
    query = {
        "product": "fake_product_name",
        "time": ("2001-1-1 00:00:00", "2001-3-31 23:59:59"),
    }
    assert len(gw.list_tiles(tile_buffer=(20, 20), **query)) == 9


def test_gridworkflow_list_tiles_multiple_datasets(sample_grid_workflow):
    gw, gridspec, fakedataset, fakeindex = sample_grid_workflow
    query = {
        "product": "fake_product_name",
        "time": ("2001-1-1 00:00:00", "2001-3-31 23:59:59"),
    }

    # Add dataset to cell (2,-2)
    fakedataset2 = MagicMock()
    fakedataset2.extent = gridspec.tile_geobox((2, -2)).extent
    fakedataset2.center_time = fakedataset.center_time
    fakedataset2.id = uuid.uuid4()
    fakeindex.datasets.search_eager.return_value = [fakedataset, fakedataset2]

    # unpadded
    assert len(gw.list_tiles(**query)) == 2
    np_time = numpy.datetime64(fakedataset.center_time, "ns")
    assert set(gw.list_tiles(**query).keys()) == {(1, -2, np_time), (2, -2, np_time)}

    # padded
    assert len(gw.list_tiles(tile_buffer=(20, 20), **query)) == 12


def test_gridworkflow_returned_tile_properties(sample_grid_workflow):
    gw, gridspec, fakedataset, fakeindex = sample_grid_workflow
    query = {
        "product": "fake_product_name",
        "time": ("2001-1-1 00:00:00", "2001-3-31 23:59:59"),
    }
    np_time = numpy.datetime64(fakedataset.center_time, "ns")

    # Add dataset to cell (2,-2)
    fakedataset2 = MagicMock()
    fakedataset2.extent = gridspec.tile_geobox((2, -2)).extent
    fakedataset2.center_time = fakedataset.center_time
    fakedataset2.id = uuid.uuid4()
    fakeindex.datasets.search_eager.return_value = [fakedataset, fakedataset2]

    tile = gw.list_tiles(**query)[1, -2, np_time]
    assert tile.shape == (1, 10, 10)
    assert len(str(tile)) > 0
    assert len(repr(tile)) > 0

    padded_tile = gw.list_tiles(tile_buffer=(20, 20), **query)[1, -2, np_time]
    assert padded_tile.shape == (1, 14, 14)

    assert len(tile.sources.isel(time=0).item()) == 1
    assert len(padded_tile.sources.isel(time=0).item()) == 2

    assert tile.geobox.alignment == padded_tile.geobox.alignment
    assert tile.geobox.affine * (0, 0) == padded_tile.geobox.affine * (2, 2)
    assert tile.geobox.affine * (10, 10) == padded_tile.geobox.affine * (10 + 2, 10 + 2)


def test_gridworkflow_loading(sample_grid_workflow):
    gw, _, fakedataset, _ = sample_grid_workflow
    query = {
        "product": "fake_product_name",
        "time": ("2001-1-1 00:00:00", "2001-3-31 23:59:59"),
    }
    np_time = numpy.datetime64(fakedataset.center_time, "ns")
    tile = gw.list_tiles(**query)[1, -2, np_time]
    padded_tile = gw.list_tiles(tile_buffer=(20, 20), **query)[1, -2, np_time]

    measurement = {"nodata": 0, "dtype": numpy.int32}
    fakedataset.product.lookup_measurements.return_value = {"dummy": measurement}
    fakedataset2 = MagicMock()
    fakedataset2.product = fakedataset.product

    with patch("datacube.api.core.Datacube.load_data") as loader:
        _ = GridWorkflow.load(tile)
        _ = GridWorkflow.load(padded_tile)

    assert loader.call_count == 2

    for (args, kwargs), loadable in zip(loader.call_args_list, [tile, padded_tile]):
        args = list(args)
        assert args[0] is loadable.sources
        assert args[1] is loadable.geobox
        assert next(iter(args[2].values())) is measurement
        assert "resampling" in kwargs


def test_gridworkflow_cell_index_extract(sample_grid_workflow):
    gw, gridspec, fakedataset, fakeindex = sample_grid_workflow
    query = {
        "product": "fake_product_name",
        "time": ("2001-1-1 00:00:00", "2001-3-31 23:59:59"),
    }
    np_time = numpy.datetime64(fakedataset.center_time, "ns")

    # Add dataset to cell (2,-2)
    fakedataset2 = MagicMock()
    fakedataset2.extent = gridspec.tile_geobox((2, -2)).extent
    fakedataset2.center_time = fakedataset.center_time
    fakedataset2.id = uuid.uuid4()
    fakeindex.datasets.search_eager.return_value = [fakedataset, fakedataset2]

    tile = gw.list_tiles(cell_index=(1, -2), **query)
    assert len(tile) == 1
    assert tile[1, -2, np_time].shape == (1, 10, 10)
    assert len(tile[1, -2, np_time].sources.values[0]) == 1

    padded_tile = gw.list_tiles(cell_index=(1, -2), tile_buffer=(20, 20), **query)
    assert len(padded_tile) == 1
    assert padded_tile[1, -2, np_time].shape == (1, 14, 14)
    assert len(padded_tile[1, -2, np_time].sources.values[0]) == 2

    # query without product is not allowed
    with pytest.raises(RuntimeError):
        gw.list_cells(cell_index=(1, -2), time=query["time"])


def test_gridworkflow_with_time_depth():
    """Test GridWorkflow with time series.
    Also test `Tile` methods `split` and `split_by_time`
    """
    crs = CRS("EPSG:4326")

    pixel = 10  # square pixel linear dimension in crs units
    gridspec = GridSpec(crs=crs, tile_shape=(10, 10), resolution=Resolution(pixel))

    def make_fake_datasets(num_datasets: int):
        start_time = datetime.datetime(2001, 2, 15)
        delta = datetime.timedelta(days=16)
        for i in range(num_datasets):
            fakedataset = MagicMock()
            fakedataset.extent = gridspec.tile_geobox((1, -2)).extent
            fakedataset.center_time = start_time + (delta * i)
            yield fakedataset

    fakeindex = PickleableMock()
    fakeindex.datasets.get_field_names.return_value = ["time"]  # permit query on time
    fakeindex.datasets.search_eager.return_value = list(make_fake_datasets(100))

    # ------ test with time dimension ----

    gw = GridWorkflow(fakeindex, gridspec)
    query = {"product": "fake_product_name"}

    cells = gw.list_cells(**query)
    for _, cell in cells.items():
        #  test Tile.split()
        for _, tile in cell.split("time"):
            assert tile.shape == (1, 10, 10)

        #  test Tile.split_by_time()
        for year, year_cell in cell.split_by_time(freq="A"):
            for t in year_cell.sources.time.values:
                assert str(t)[:4] == year
