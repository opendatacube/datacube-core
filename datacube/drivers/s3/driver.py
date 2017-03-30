'''S3 storage driver class
'''
from __future__ import absolute_import

import logging
from datacube.utils import DatacubeException
from datacube.drivers.driver import Driver
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO
from pathlib import Path
from collections import abc

class S3Driver(Driver):
    '''S3 storage driver. A placeholder for now.
    '''

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.storage = S3LIO()


    @property
    def name(self):
        '''See :meth:`datacube.drivers.driver.name`
        '''
        return 's3'


    def _get_chunksizes(self, variable_param):
        '''Return the chunk sizes if defined and valid.

        We expect a list/tuple of 3 floats.

        :param dict variable_param: the variable parameter expected to
        contain the `chunksizes`.
        :return tuple chunksizes: the validated chunksizes as a
        3-floats tuple.
        '''
        if not 'chunksizes' in variable_param:
            raise DatacubeException('Dataset does not contain chunking values, ' \
                                    +'cannot write to storage.')
        chunksizes = variable_param['chunksizes']
        if not isinstance(chunksizes, abc.Iterable):
            raise DatacubeException('Dataset contains invalid chunking values, ' \
                                    +'cannot write to storage.')
        try:
            chunksizes = tuple(map(float, chunksizes))
        except ValueError:
            raise DatacubeException('Dataset contains invalid chunking values, ' \
                                    +'cannot write to storage.')
        if not len(chunksizes) == 3:
            raise DatacubeException('Dataset contains invalid chunking values, ' \
                                    +'cannot write to storage.')
        return chunksizes


    def write_dataset_to_storage(self, dataset, *args, **kargs):
        '''See :meth:`datacube.drivers.driver.write_dataset_to_storage`
        '''
        if len(args) < 3:
            raise DatacubeException('Missing configuration paramters, cannot write to storage.')
        filename = Path(args[0])
        global_attributes = args[1] or {}
        variable_params = args[2] or {}
        if not dataset.data_vars.keys():
            raise DatacubeException('Cannot save empty dataset to storage.')

        if not hasattr(dataset, 'crs'):
            raise DatacubeException('Dataset does not contain CRS, ' \
                                    +'cannot write to storage.')

        # TODO(csiro): Complete the proper logic below
        # for name, variable in dataset.data_vars.items():
        #     if not name in variable_params:
        #         raise DatacubeException('Dataset does not contain chunking values, ' \
        #                                 +'cannot write to storage. ' +name)
        #     chunksizes = self._get_chunksizes(variable_params[name])

        #     self.logger.debug('For s3 storage: name: %s, chunksizes: %s',
        #                       name, chunksizes)

        #     # TODO(csiro): determine what goes into which parameter
        #     # In test_full_ingestion, `name` = {`blue`, `green`, `dataset`}
        #     # I am not sure what `dataset` corresponds to, and sure enough
        #     # it doesn't have an entry in variable_params

        #     # Write to s3. Something along those lines?
        #     # key_map.append(self.storage.put_array_in_s3(x,
        #     #                                            (2,2,2),
        #     #                                            'base_name',
        #     #                                            's3aio-test',
        #     #                                            True)
        # TODO(csiro): Return the key map(s)
        return None
