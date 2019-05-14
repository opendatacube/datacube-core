""" This modules provides interim solution for supporting S3AIO read driver
during IO driver refactoring.

This module will be removed once S3AIO driver migrates to new style.

"""
import logging
import numpy as np
import uuid
from math import ceil
from typing import Union, Optional, Callable, List, Any

from concurrent.futures import ThreadPoolExecutor, wait
from multiprocessing import cpu_count

from datacube.utils import ignore_exceptions_if
from datacube.utils.geometry import GeoBox, roi_is_empty
from datacube.storage import BandInfo, DataSource
from datacube.model import Measurement, Dataset
from datacube.api.core import Datacube
from datacube.storage._read import read_time_slice

_LOG = logging.getLogger(__name__)

FuserFunction = Callable[[np.ndarray, np.ndarray], Any]  # pylint: disable=invalid-name


def _init_s3_aio_driver():
    try:
        from datacube.drivers.s3.driver import reader_driver_init, reader_test_driver_init
        return reader_driver_init(), reader_test_driver_init()
    except Exception:  # pylint: disable=broad-except
        return None, None


S3AIO_DRIVER, S3AIO_FILE_DRIVER = _init_s3_aio_driver()


def reproject_and_fuse(datasources: List[DataSource],
                       destination: np.ndarray,
                       dst_gbox: GeoBox,
                       dst_nodata: Optional[Union[int, float]],
                       resampling: str = 'nearest',
                       fuse_func: Optional[FuserFunction] = None,
                       skip_broken_datasets: bool = False):
    """
    Reproject and fuse `sources` into a 2D numpy array `destination`.

    :param datasources: Data sources to open and read from
    :param destination: ndarray of appropriate size to read data into
    :param dst_gbox: GeoBox defining destination region
    :param skip_broken_datasets: Carry on in the face of adversity and failing reads.
    """
    # pylint: disable=too-many-locals
    assert len(destination.shape) == 2

    def copyto_fuser(dest: np.ndarray, src: np.ndarray) -> None:
        where_nodata = (dest == dst_nodata) if not np.isnan(dst_nodata) else np.isnan(dest)
        np.copyto(dest, src, where=where_nodata)

    fuse_func = fuse_func or copyto_fuser

    destination.fill(dst_nodata)
    if len(datasources) == 0:
        return destination
    elif len(datasources) == 1:
        with ignore_exceptions_if(skip_broken_datasets):
            with datasources[0].open() as rdr:
                read_time_slice(rdr, destination, dst_gbox, resampling, dst_nodata)

        return destination
    else:
        # Multiple sources, we need to fuse them together into a single array
        buffer_ = np.full(destination.shape, dst_nodata, dtype=destination.dtype)
        for source in datasources:
            with ignore_exceptions_if(skip_broken_datasets):
                with source.open() as rdr:
                    roi = read_time_slice(rdr, buffer_, dst_gbox, resampling, dst_nodata)

                if not roi_is_empty(roi):
                    fuse_func(destination[roi], buffer_[roi])
                    buffer_[roi] = dst_nodata  # clean up for next read

        return destination


def fuse_measurement(dest: np.ndarray,
                     datasets: List[Dataset],
                     geobox: GeoBox,
                     measurement: Measurement,
                     mk_new: Callable[[BandInfo], DataSource],
                     skip_broken_datasets: bool = False):
    reproject_and_fuse([mk_new(BandInfo(dataset, measurement.name)) for dataset in datasets],
                       dest,
                       geobox,
                       dest.dtype.type(measurement.nodata),
                       resampling=measurement.get('resampling_method', 'nearest'),
                       fuse_func=measurement.get('fuser', None),
                       skip_broken_datasets=skip_broken_datasets)


def fuse_lazy(datasets,
              geobox: GeoBox,
              measurement: Measurement,
              mk_new: Callable[[BandInfo], DataSource],
              skip_broken_datasets: bool = False,
              prepend_dims: int = 0):
    prepend_shape = (1,) * prepend_dims
    data = np.full(geobox.shape, measurement.nodata, dtype=measurement.dtype)
    fuse_measurement(data, datasets, geobox, measurement, mk_new,
                     skip_broken_datasets=skip_broken_datasets)
    return data.reshape(prepend_shape + geobox.shape)


def get_loader(sources):
    if S3AIO_DRIVER is None:
        raise RuntimeError("S3AIO driver failed to load")

    ds = sources.values[0][0]
    if ds.uri_scheme == 'file':
        return S3AIO_FILE_DRIVER.new_datasource
    else:
        return S3AIO_DRIVER.new_datasource


def _chunk_geobox(geobox, chunk_size):
    num_grid_chunks = [int(ceil(s / float(c))) for s, c in zip(geobox.shape, chunk_size)]
    geobox_subsets = {}
    for grid_index in np.ndindex(*num_grid_chunks):
        slices = [slice(min(d * c, stop), min((d + 1) * c, stop))
                  for d, c, stop in zip(grid_index, chunk_size, geobox.shape)]
        geobox_subsets[grid_index] = geobox[slices]
    return geobox_subsets


# pylint: disable=too-many-locals
def make_dask_array(sources,
                    geobox,
                    measurement,
                    skip_broken_datasets=False,
                    dask_chunks=None):
    from ..core import (_tokenize_dataset,
                        select_datasets_inside_polygon,
                        _calculate_chunk_sizes)
    from dask import array as da

    dsk_name = 'datacube_load_{name}-{token}'.format(name=measurement['name'], token=uuid.uuid4().hex)

    irr_chunks, grid_chunks = _calculate_chunk_sizes(sources, geobox, dask_chunks)
    sliced_irr_chunks = (1,) * sources.ndim

    dsk = {}
    geobox_subsets = _chunk_geobox(geobox, grid_chunks)
    mk_new = get_loader(sources)

    for irr_index, datasets in np.ndenumerate(sources.values):
        for dataset in datasets:
            ds_token = _tokenize_dataset(dataset)
            dsk[ds_token] = dataset

        for grid_index, subset_geobox in geobox_subsets.items():
            dataset_keys = [_tokenize_dataset(d) for d in
                            select_datasets_inside_polygon(datasets, subset_geobox.extent)]
            dsk[(dsk_name,) + irr_index + grid_index] = (fuse_lazy,
                                                         dataset_keys,
                                                         subset_geobox,
                                                         measurement,
                                                         mk_new,
                                                         skip_broken_datasets,
                                                         sources.ndim)

    data = da.Array(dsk, dsk_name,
                    chunks=(sliced_irr_chunks + grid_chunks),
                    dtype=measurement['dtype'],
                    shape=(sources.shape + geobox.shape))

    if irr_chunks != sliced_irr_chunks:
        data = data.rechunk(chunks=(irr_chunks + grid_chunks))
    return data


def dask_load(sources, geobox, measurements, dask_chunks,
              skip_broken_datasets=False):
    def data_func(measurement):
        return make_dask_array(sources, geobox, measurement,
                               skip_broken_datasets=skip_broken_datasets,
                               dask_chunks=dask_chunks)

    return Datacube.create_storage(sources.coords, geobox, measurements, data_func)


def xr_load(sources, geobox, measurements,
            skip_broken_datasets=False,
            use_threads=False):
    mk_new = get_loader(sources)

    data = Datacube.create_storage(sources.coords, geobox, measurements)

    if use_threads:
        def work_load_data(index, datasets, m):
            t_slice = data[m.name].values[index]
            fuse_measurement(t_slice, datasets, geobox, m,
                             mk_new=mk_new,
                             skip_broken_datasets=skip_broken_datasets)

        futures = []
        pool = ThreadPoolExecutor(cpu_count()*2)
        for index, datasets in np.ndenumerate(sources.values):
            for m in measurements:
                futures.append(pool.submit(work_load_data, index, datasets, m))

        wait(futures)
    else:
        for index, datasets in np.ndenumerate(sources.values):
            for m in measurements:
                t_slice = data[m.name].values[index]

                fuse_measurement(t_slice, datasets, geobox, m,
                                 mk_new=mk_new,
                                 skip_broken_datasets=skip_broken_datasets)

    return data
