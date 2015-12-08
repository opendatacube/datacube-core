
from __future__ import absolute_import, print_function, division

from affine import Affine
import numpy

from datacube.index import index_connect
from datacube.storage.storage import reproject_datasets, _dataset_time, DatasetSource


def main():

    my_projection = {'init': 'EPSG:4326'}
    my_tile_size = (1.5, 1.5)
    my_tile_res = (-0.0025, 0.0025)
    my_tile_offset = (-35.0, 149.0)
    my_nodata = -999

    my_transform = Affine(my_tile_res[1], 0.0, my_tile_offset[1], 0.0, my_tile_res[0], my_tile_offset[0])

    width = my_tile_size[1]/abs(my_tile_res[1])
    height = my_tile_size[0]/abs(my_tile_res[0])

    index = index_connect()
    datasets = index.datasets.search_eager(product='EODS_NBAR')
    datasets.sort(key=_dataset_time)

    sources = [DatasetSource(dataset, '10') for dataset in datasets]
    dest = numpy.full((len(datasets), height, width), my_nodata, dtype=numpy.int16)
    reproject_datasets(sources, dest, my_transform, my_projection, my_nodata)

    print(dest)

if __name__ == '__main__':
    main()
