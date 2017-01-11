
from __future__ import absolute_import

try:
    import cPickle as pickle
except ImportError:
    import pickle

from datacube.utils import geometry


def test_pickleable():
    poly = geometry.polygon([(10, 20), (20, 20), (20, 10), (10, 20)], crs=geometry.CRS('EPSG:4326'))
    pickled = pickle.dumps(poly, pickle.HIGHEST_PROTOCOL)
    unpickled = pickle.loads(pickled)
    assert poly == unpickled


def test_union_cascaded():
    box1 = geometry.box(10, 10, 30, 30, crs=geometry.CRS('EPSG:4326'))
    box2 = geometry.box(20, 10, 40, 30, crs=geometry.CRS('EPSG:4326'))
    box3 = geometry.box(30, 10, 50, 30, crs=geometry.CRS('EPSG:4326'))
    box4 = geometry.box(40, 10, 60, 30, crs=geometry.CRS('EPSG:4326'))

    union1 = geometry.unary_union([box1, box4])
    assert union1.type == 'MultiPolygon'
    assert union1.area == 2.0*box1.area

    union2 = geometry.unary_union([box1, box2])
    assert union2.type == 'Polygon'
    assert union2.area == 1.5*box1.area

    union3 = geometry.unary_union([box1, box2, box3, box4])
    assert union3.type == 'Polygon'
    assert union3.area == 2.5*box1.area

    union4 = geometry.unary_union([union1, box2, box3])
    assert union4.type == 'Polygon'
    assert union4.area == 2.5*box1.area

    x = 1
