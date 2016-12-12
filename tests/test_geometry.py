
from __future__ import absolute_import

try:
    import cPickle as pickle
except ImportError:
    import pickle

from datacube.model import geometry


def test_pickleable():
    poly = geometry.polygon([(10, 20), (20, 20), (20, 10), (10, 20)], crs=geometry.CRS('EPSG:4326'))
    pickled = pickle.dumps(poly, pickle.HIGHEST_PROTOCOL)
    unpickled = pickle.loads(pickled)

