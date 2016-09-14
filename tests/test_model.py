# coding=utf-8

import numpy
from datacube.model import GeoPolygon, GeoBox, CRS, GridSpec


def test_geobox():
    points_list = [
        [(148.2697, -35.20111), (149.31254, -35.20111), (149.31254, -36.331431), (148.2697, -36.331431)],
        [(148.2697, 35.20111), (149.31254, 35.20111), (149.31254, 36.331431), (148.2697, 36.331431)],
        [(-148.2697, 35.20111), (-149.31254, 35.20111), (-149.31254, 36.331431), (-148.2697, 36.331431)],
        [(-148.2697, -35.20111), (-149.31254, -35.20111), (-149.31254, -36.331431), (-148.2697, -36.331431)],
        ]
    for points in points_list:
        polygon = GeoPolygon(points, CRS('EPSG:3577'))
        resolution = (-25, 25)
        geobox = GeoBox.from_geopolygon(polygon, resolution)

        assert abs(resolution[0]) > abs(geobox.extent.boundingbox.left - polygon.boundingbox.left)
        assert abs(resolution[0]) > abs(geobox.extent.boundingbox.right - polygon.boundingbox.right)
        assert abs(resolution[1]) > abs(geobox.extent.boundingbox.top - polygon.boundingbox.top)
        assert abs(resolution[1]) > abs(geobox.extent.boundingbox.bottom - polygon.boundingbox.bottom)


def test_grispec():
    gs = GridSpec(crs=CRS('EPSG:4326'), tile_size=(1, 1), resolution=(-0.1, 0.1), origin=(10, 10))
    poly = GeoPolygon([(10, 12.2), (10.8, 13), (13, 10.8), (12.2, 10)], CRS('EPSG:4326'))
    cells = {index: geobox for index, geobox in list(gs.tiles2(poly))}
    assert set(cells.keys()) == {(0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (2, 0), (2, 1)}
    assert numpy.isclose(cells[(2, 0)].coordinates['longitude'].values, numpy.linspace(12.05, 12.95, num=10)).all()
    assert numpy.isclose(cells[(2, 0)].coordinates['latitude'].values, numpy.linspace(10.95, 10.05, num=10)).all()
