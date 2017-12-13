"""S3 storage driver module."""
from __future__ import absolute_import

import logging
from pathlib import Path

import numpy as np

from datacube.drivers.driver import Driver
from datacube.drivers.s3.datasource import S3DataSource
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO
from datacube.drivers.utils import DriverUtils
from datacube.utils import DatacubeException


class S3Driver(Driver):
    """S3 storage driver."""

    def __init__(self, driver_manager, name, index=None, *index_args, **index_kargs):
        """Initialise the s3 storage."""
        super(S3Driver, self).__init__(driver_manager, name, index, *index_args, **index_kargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.storage = S3LIO()

    @property
    def uri_scheme(self):
        """URI scheme used by this driver."""
        return 's3'

    def _get_chunksizes(self, chunksizes):
        """Return the chunk sizes as an int tuple, if valid.

        We expect a list/tuple of 3 integers.

        :param dict chunksizes: the raw chunksizes parameter, to be
          validated.
        :return tuple chunksizes: the validated chunksizes as a an
          integers tuple.
        """
        if not isinstance(chunksizes, (list, tuple, set)):
            raise DatacubeException('Dataset contains invalid chunking values, cannot write to storage.')
        try:
            chunksizes = tuple(map(int, chunksizes))
        except ValueError:
            raise DatacubeException('Dataset contains invalid chunking values, cannot write to storage.')
        if not chunksizes:
            raise DatacubeException('Dataset contains invalid chunking values, cannot write to storage.')
        return chunksizes

    def get_reg_irreg_index(self, coord, data):
        """Returns the regular/irregular information for a single dataset
        coordinate.

        Data is considered regular if it is equally spaced, give or
        take a predefined error magin defined per coord type.
        :param str coord: Coordinate name, e.g. 'x' or 'time'.
          data(ndarray): The coordinates values.
        :return: Returns a tuple `(regular_dimension, regular_index,
          irregular_index)` where `regular_dimension` is a boolean
          indicating whether `coord` is regular. If so,
          `irregular_index` is `None` and `regular_index` is the tuple
          `(min_val, max_val, step)` representing the minimum and
          maximum values of that coordinate range and the step
          used. Otherwise, `regular_index` is `None` and
          `irregular_index` is the list of the `coord` values.
        """
        epsilon = DriverUtils.epsilon(coord)
        regular = False  # Default for single element
        delta = None
        previous = None
        for datum in data.tolist():
            if previous:
                if not delta:
                    regular = True
                    delta = datum - previous
                elif abs(datum - previous - delta) > epsilon:
                    regular = False
                    break
            previous = datum
        if regular:
            return True, (np.min(data), np.max(data), np.float64(delta)), None
        return False, None, data

    def get_reg_irreg_indices(self, coords):
        """Returns the regular/irregular information for all the dataset
        coordinates.

        :param xarray.Coordinates coords: The dict-like dataset
          coordinates.
        :return: Returns a tuple of lists `(regular_dimension,
          regular_index, irregular_index)` with each list compiling
          the results of :meth:`get_reg_irreg_index` for each coord in
          `coords`.
        """
        return zip(*[self.get_reg_irreg_index(coord, coords[coord].values)
                     for coord in coords])

    def _get_index(self, chunk, coords, dims, index_type='min'):
        """Return the min or max index of chunk in coord space.

        It basically returns the nth coord value with `n`
        corresponding to the chunk's `start` or `stop - 1` value.

        :param slice chunk: The chunk providing the n index.
        :param xarray.Coordinates coords: The dict-like dataset
          coordinates.
        :param list dims: The dimension names for each axis.
        :param str index_type: Whether to fetch the `min` or `max`
          index. Default: `min`.
        :return: List of coord values corresponding to the chunk's
          min/max index.
        """
        if index_type == 'min':
            idx = lambda x: x.start
        else:
            idx = lambda x: x.stop - 1
        return [coords[dim].values[idx(chunk_dim)] for dim, chunk_dim in zip(dims, chunk)]

    def write_dataset_to_storage(self, dataset, *args, **kargs):
        """See :meth:`datacube.drivers.driver.write_dataset_to_storage`

        :param args: At least 3 arguments are required: filename,
          global_attributes and variable_params.
        :return: Dictionary of metadata consigning the s3 storage
          information. This is required for indexing in particular.
        """
        if len(args) < 3:
            raise DatacubeException('Missing configuration parameters, cannot write to storage.')
        filename = Path(args[0])
        global_attributes = args[1] or {}
        variable_params = args[2] or {}
        if not dataset.data_vars.keys():
            raise DatacubeException('Cannot save empty dataset to storage.')

        if not hasattr(dataset, 'crs'):
            raise DatacubeException('Dataset does not contain CRS, cannot write to storage.')

        outputs = {}
        for band, param in variable_params.items():
            output = {}
            if 'chunksizes' not in param:
                raise DatacubeException('Missing `chunksizes` parameter, cannot write to storage.')
            output['chunk_size'] = self._get_chunksizes(param['chunksizes'])
            if 'container' not in param:
                raise DatacubeException('Missing `container` parameter, cannot write to storage.')
            output['bucket'] = param['container']
            self.storage.filepath = output['bucket']  # For the s3_test driver only
            output['base_name'] = '%s_%s' % (filename.stem, band)
            key_maps = self.storage.put_array_in_s3(dataset[band].values,
                                                    output['chunk_size'],
                                                    output['base_name'],
                                                    output['bucket'],
                                                    True)
            output['key_maps'] = [{
                's3_key': s3_key,
                'chunk': chunk,
                'chunk_id': chunk_id,
                'compression': None,
                'index_min': self._get_index(chunk, dataset[band].coords, dataset[band].dims, 'min'),
                'index_max': self._get_index(chunk, dataset[band].coords, dataset[band].dims, 'max')
            } for (s3_key, chunk, chunk_id) in key_maps]
            output['dimensions'] = dataset[band].dims
            output['macro_shape'] = dataset[band].shape
            output['numpy_type'] = dataset[band].dtype.str
            (output['regular_dims'],
             output['regular_index'],
             output['irregular_index']) = self.get_reg_irreg_indices(dataset[band].coords)

            self.logger.info('Wrote %d chunks of size %s to s3 bucket: %s, base_name: %s',
                             len(output['key_maps']), output['chunk_size'],
                             output['bucket'], output['base_name'])
            outputs[band] = output
        return outputs

    def get_datasource(self, dataset, measurement_id):
        """See :meth:`datacube.drivers.driver.get_datasource`"""
        return S3DataSource(dataset, measurement_id, self.storage)
