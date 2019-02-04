""" This modules provides interim solution for supporting S3AIO read driver
during IO driver refactoring.

This module will be removed once S3AIO driver migrates to new style.
"""


def load_data(sources,
              geobox,
              measurements,
              dask_chunks,
              skip_broken_datasets=False,
              use_threads=False):
    from .load import xr_load, dask_load

    if dask_chunks is not None:
        return dask_load(sources, geobox, measurements, dask_chunks,
                         skip_broken_datasets=skip_broken_datasets)
    else:
        return xr_load(sources, geobox, measurements,
                       skip_broken_datasets=skip_broken_datasets,
                       use_threads=use_threads)
