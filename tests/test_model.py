# coding=utf-8
import os

import pytest

from datacube.model import _uri_to_local_path, Dataset, DatasetMatcher, GeoPolygon, GeoBox


def test_uri_to_local_path():
    if os.name == 'nt':
        assert 'C:\\tmp\\test.tmp' == str(_uri_to_local_path('file:///C:/tmp/test.tmp'))

    else:
        assert '/tmp/something.txt' == str(_uri_to_local_path('file:///tmp/something.txt'))

    assert _uri_to_local_path(None) is None

    with pytest.raises(ValueError):
        _uri_to_local_path('ftp://example.com/tmp/something.txt')


def test_doctest_local_path():
    if os.name == 'nt':
        dataset = Dataset(None, None, 'file:///C:/tmp/test.tmp')

        assert str(dataset.local_path) == 'C:\\tmp\\test.tmp'

    else:
        dataset = Dataset(None, None, 'file:///tmp/something.txt')

        assert str(dataset.local_path) == '/tmp/something.txt'

    dataset = Dataset(None, None, None).local_path is None

    with pytest.raises(ValueError):
        Dataset(None, None, 'ftp://example.com/tmp/something.txt').local_path


def test_dataset_matcher_repr():
    ds_matcher = DatasetMatcher(metadata={'flim': 'flam'})

    rep = repr(ds_matcher)
    assert 'flim' in rep
    assert 'flam' in rep


def test_geobox():
    points_list = [
        [(148.2697, -35.20111), (149.31254, -35.20111), (149.31254, -36.331431), (148.2697, -36.331431)],
        [(148.2697, 35.20111), (149.31254, 35.20111), (149.31254, 36.331431), (148.2697, 36.331431)],
        [(-148.2697, 35.20111), (-149.31254, 35.20111), (-149.31254, 36.331431), (-148.2697, 36.331431)],
        [(-148.2697, -35.20111), (-149.31254, -35.20111), (-149.31254, -36.331431), (-148.2697, -36.331431)],
        ]
    for points in points_list:
        polygon = GeoPolygon(points, 'EPSG:3577')
        resolution = (25, -25)
        geobox = GeoBox.from_geopolygon(polygon, resolution)

        assert abs(resolution[0]) > abs(geobox.extent.boundingbox.left - polygon.boundingbox.left)
        assert abs(resolution[0]) > abs(geobox.extent.boundingbox.right - polygon.boundingbox.right)
        assert abs(resolution[1]) > abs(geobox.extent.boundingbox.top - polygon.boundingbox.top)
        assert abs(resolution[1]) > abs(geobox.extent.boundingbox.bottom - polygon.boundingbox.bottom)
