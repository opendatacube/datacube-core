
from __future__ import absolute_import, print_function, division

from itertools import groupby

from affine import Affine
from rasterio.coords import BoundingBox
import numpy

from datacube.index import index_connect
from datacube.storage.storage import fuse_sources, _dataset_time, DatasetSource, RESAMPLING


# pylint: disable=too-many-locals
def get_data(datasets,
             measurement_id,
             bounds,
             resolution,
             crs,
             nodata,
             group_func,
             fuse_func=None):
    """
    :type bounds: BoundingBox
    """
    groups = [(key, [DatasetSource(dataset, measurement_id) for dataset in group])
              for key, group in groupby(datasets, group_func)]

    shape = (len(groups),
             (bounds.top - bounds.bottom) / abs(resolution[1]),
             (bounds.right - bounds.left) / abs(resolution[0]))
    transform = Affine(resolution[0], 0.0, bounds.right if resolution[0] < 0 else bounds.left,
                       0.0, resolution[1], bounds.top if resolution[1] < 0 else bounds.bottom)

    result = numpy.empty(shape, dtype=numpy.int16)
    for index, (key, sources) in enumerate(groups):
        fuse_sources(sources,
                     result[index],
                     transform,
                     crs,
                     nodata,
                     resampling=RESAMPLING.nearest,
                     fuse_func=fuse_func)
    return result


def main():
    index = index_connect()
    datasets = index.datasets.search_eager(product='EODS_NBAR')

    data = get_data(datasets,
                    '10',
                    bounds=BoundingBox(149, -39, 152, -35),
                    resolution=(0.0025, -0.0025),
                    crs={'init': 'EPSG:4326'},
                    nodata=-999,
                    group_func=_dataset_time)

    print(data)

if __name__ == '__main__':
    main()
