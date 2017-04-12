'''S3 Test storage driver class. To avoid credential issues with
travis accessing s3, we use a filesystem based s3 driver for testing
purposes.

CAUTION: if run as root, this may write anywhere in the filesystem.
'''
from __future__ import absolute_import

import logging
from pathlib import Path

from datacube.utils import DatacubeException
from datacube.drivers.driver import Driver
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO
from datacube.drivers.s3.index import Index

class S3TestDriver(Driver):
    '''S3 Test storage driver, using filesystem rather than actual s3, for
    testing purposes only.
    '''

    def __init__(self, name, local_config=None, application_name=None, validate_connection=True):
        '''Initialise the s3 test driver.

        CAUTION: if run as root, this may write anywhere in the
        filesystem.
        '''
        super(self.__class__, self).__init__(name, local_config, application_name, validate_connection)
        self.logger = logging.getLogger(self.__class__.__name__)
        # Initialise with the root at the top of the filesystem, so
        # that the `container` path can be absolute.
        self.storage = S3LIO(False, '/')


    @property
    def format(self):
        '''Output format for this driver for use in metadata.'''
        return 's3'


    def _get_chunksizes(self, chunksizes):
        '''Return the chunk sizes as an int tuple, if valid.

        We expect a list/tuple of 3 integers.

        :param dict param: the raw chunksizes parameter, to be
        validated.
        :return tuple chunksizes: the validated chunksizes as a an
        integers tuple.
        '''
        if not isinstance(chunksizes, (list, tuple, set)):
            raise DatacubeException('Dataset contains invalid chunking values, cannot write to storage.')
        try:
            chunksizes = tuple(map(int, chunksizes))
        except ValueError:
            raise DatacubeException('Dataset contains invalid chunking values, cannot write to storage.')
        if len(chunksizes) == 0:
            raise DatacubeException('Dataset contains invalid chunking values, cannot write to storage.')
        return chunksizes


    def write_dataset_to_storage(self, dataset, *args, **kargs):
        '''See :meth:`datacube.drivers.driver.write_dataset_to_storage`

        :return: dict: list of s3 key maps (tuples) indexed by band.
        '''
        if len(args) < 3:
            raise DatacubeException('Missing configuration paramters, cannot write to storage.')
        filename = Path(args[0])
        global_attributes = args[1] or {}
        variable_params = args[2] or {}
        if not dataset.data_vars.keys():
            raise DatacubeException('Cannot save empty dataset to storage.')

        if not hasattr(dataset, 'crs'):
            raise DatacubeException('Dataset does not contain CRS, cannot write to storage.')

        key_maps = {}
        for band, param in variable_params.items():
            if 'chunksizes' not in param:
                raise DatacubeException('Missing `chunksizes` parameter, cannot write to storage.')
            chunk = self._get_chunksizes(param['chunksizes'])
            if 'container' not in param:
                raise DatacubeException('Missing `container` parameter, cannot write to storage.')
            bucket = param['container']
            self.storage.filepath = bucket
            basename = '%s_%s' % (filename.stem, band)
            data = dataset.data_vars[band].values
            key_maps[band] = self.storage.put_array_in_s3(data, chunk, basename, bucket, True)
            self.logger.debug('Wrote %d chunks %s to s3 bucket: %s, object: %s',
                              len(key_maps[band]), chunk, bucket, basename)
        return key_maps


    def _init_index(self, local_config=None, application_name=None, validate_connection=True):
        '''See :meth:`datacube.drivers.driver.init_index`'''
        return Index(local_config, application_name, validate_connection)
