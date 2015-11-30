import pytest

from datacube.storage import ingester


def test_expand_bounds_unit_grid():
    bounds = (148.5, -35.5, 151.5, -30.5)
    tile_size = {'x': 1, 'y': -1}

    expanded = ingester.expand_bounds(bounds, tile_size)

    expected = (148.0, -36, 152, -30)

    assert expanded == expected


def test_expand_bounds_fractional_grid():
    bounds = (4, -4, 7, 1)  # left, bottom, right, top
    tile_size = {'x': 1.5, 'y': -1.5}
    expected = (3, -4.5, 7.5, 1.5)

    expanded = ingester.expand_bounds(bounds, tile_size)

    assert expanded == expected


def test_negative_x():
    bounds = (148.5, -35.5, 151.5, -30.5)
    tile_size = {'x': -1, 'y': -1}

    with pytest.raises(ValueError):
        ingester.expand_bounds(bounds, tile_size)


def test_positive_y():
    bounds = (148.5, -35.5, 151.5, -30.5)
    tile_size = {'x': 1, 'y': 1}

    with pytest.raises(ValueError):
        ingester.expand_bounds(bounds, tile_size)
