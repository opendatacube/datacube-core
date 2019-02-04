"""S3 storage driver module."""

import logging
from pathlib import Path

import numpy as np

from datacube.drivers.s3.datasource import S3DataSource
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO
from datacube.storage import BandInfo
from .utils import DriverUtils
from datacube.utils import DatacubeException

PROTOCOL = 's3'
FORMAT = 'aio'


class S3WriterDriver(object):
    """S3 storage driver."""

    def __init__(self, **kwargs):
        """Initialise the s3 storage."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.storage = S3LIO(**kwargs)

        self._protocol = PROTOCOL if kwargs.get('enable_s3', True) else 'file'

    @property
    def format(self):
        return FORMAT

    @property
    def uri_scheme(self):
        """URI scheme used by this driver."""
        return self._protocol

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

    def write_dataset_to_storage(self, dataset, filename,
                                 global_attributes=None,
                                 variable_params=None,
                                 storage_config=None,
                                 **kwargs):
        """See :meth:`datacube.drivers.driver.write_dataset_to_storage`

        :param `xarray.Dataset` dataset:
        :param filename: Output filename
        :param global_attributes: Global file attributes. dict of attr_name: attr_value
        :param variable_params: dict of variable_name: {param_name: param_value, [...]}

        :return: Dictionary of metadata consigning the s3 storage information.
        This is required for indexing in particular.

        """
        if storage_config is None:
            storage_config = {}

        # TODO: handle missing variable params
        if variable_params is None:
            raise DatacubeException('Missing configuration parameters, cannot write to storage.')
        filename = Path(filename)
        if not dataset.data_vars.keys():
            raise DatacubeException('Cannot save empty dataset to storage.')

        if not hasattr(dataset, 'crs'):
            raise DatacubeException('Dataset does not contain CRS, cannot write to storage.')

        if 'bucket' not in storage_config:
            raise DatacubeException('Expect `bucket` to be set in the storage config')

        bucket = storage_config['bucket']

        # TODO: Should write all data variables to disk, not just configured variables
        outputs = {}
        for band, param in variable_params.items():
            output = {}
            # TODO: Should not assume presence of any kind of parameter
            if 'chunksizes' not in param:
                raise DatacubeException('Missing `chunksizes` parameter, cannot write to storage.')
            output['chunk_size'] = self._get_chunksizes(param['chunksizes'])
            output['bucket'] = bucket
            self.storage.filepath = bucket  # For the s3_test driver only TODO: is this still needed?
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


def writer_driver_init():
    return S3WriterDriver()


def writer_test_driver_init():
    return S3WriterDriver(enable_s3=False, file_path='/')


class S3ReaderDriver(object):

    def __init__(self, **kwargs):
        self.name = 's3aio'
        self.formats = [FORMAT]
        self.protocols = [PROTOCOL] if kwargs.get('enable_s3', True) else ['file']
        self._storage = S3LIO(**kwargs)

    def supports(self, protocol, fmt):
        return (protocol in self.protocols and
                fmt in self.formats)

    def new_datasource(self, band: BandInfo):
        return S3DataSource(band, self._storage)


def reader_driver_init():
    return S3ReaderDriver()


def reader_test_driver_init():
    return S3ReaderDriver(enable_s3=False, file_path='/')
