'''A module offering an S3 datasource that mimicks the existing NetCDF
datasource behaviour.
'''

from __future__ import absolute_import

import logging
from contextlib import contextmanager

from affine import Affine
from numpy import dtype

from datacube.drivers.datasource import DataSource
from datacube.storage.storage import OverrideBandDataSource
from datacube.utils import clamp, data_resolution_and_offset, datetime_to_seconds_since_1970, DatacubeException


class S3Source(object):
    '''Proxy class to allow usage in OverrideBandDataSource without modifications.'''

    class S3DS(object):
        '''Proxy class to allow usage in OverrideBandDataSource without
        modifications, because it requires a source.ds.read method.
        '''
        def __init__(self, parent):
            self.parent = parent


        def read(self, indexes, window, out_shape):
            return self.parent.read(window, out_shape)


    def __init__(self, dataset, band_name):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.dataset = dataset
        self.band_name = band_name
        self.ds = self.S3DS(self)
        self.bidx = 1 # Called but unused in s3


    def read(self, window, write_shape):
        s3_dataset = self.dataset.s3_metadata[self.band_name]['s3_dataset']
        if isinstance(window, S3Source):
            slices = tuple([slice(0, a) for a in s3_dataset.macro_shape[1:]])
        else:
            slices = tuple([slice(a[0], a[1]) for a in window])

        # TODO(csiro): do not recreate storage. Instead, make call
        # through driver, somehow
        import datacube.drivers.s3.storage.s3aio as s3lio
        s = s3lio.S3LIO(False, '/')

        self.logger.debug('s.get_data_unlabeled(%s, %s, %s, %s, %s, %s, %s)',
                          s3_dataset.base_name,
                          s3_dataset.macro_shape[1:],
                          s3_dataset.chunk_size[1:],
                          dtype(s3_dataset.numpy_type),
                          slices,
                          s3_dataset.bucket,
                          True)
        data = s.get_data_unlabeled(s3_dataset.base_name,
                                    s3_dataset.macro_shape[1:],
                                    s3_dataset.chunk_size[1:],
                                    dtype(s3_dataset.numpy_type),
                                    slices,
                                    s3_dataset.bucket,
                                    True)
        return data


class S3DataSource(DataSource):
    '''Data source for reading from a Datacube Dataset'''
    def __init__(self, dataset, band_name):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._dataset = dataset
        self.source = S3Source(dataset, band_name)
        self.nodata = dataset.type.measurements[band_name].get('nodata')
        self.macro_shape = dataset.s3_metadata[band_name]['s3_dataset'].macro_shape[1:] # Do NOT use time here


    @contextmanager
    def open(self):
        '''Context manager which returns a `BandDataSource`'''
        yield OverrideBandDataSource(self.source,
                                     nodata=self.nodata,
                                     crs=self.get_crs(),
                                     transform=self.get_transform(self.macro_shape))


    # Unused in s3. (untested)
    # def get_bandnumber(self, src):
    #     time = self._dataset.center_time
    #     sec_since_1970 = datetime_to_seconds_since_1970(time)
    #     s3_dataset = dataset.s3_metadata[band_name]['s3_dataset']
    #     if s3_dataset.regular_dims[0]: # If time is regular
    #         return int((sec_since_1970 - s3_dataset.regular_indices[0]) / s3_dataset.regular_indices[2])
    #     else:
    #         for idx, timestamp in enumerate(s3_dataset.irregular_indices):
    #             if abs(sec_since_1970 - timestamp) < S3Driver.EPSILON['time']:
    #                 return idx


    def get_transform(self, shape):
        return self._dataset.transform * Affine.scale(1.0/shape[1], 1.0/shape[0])


    def get_crs(self):
        return self._dataset.crs
