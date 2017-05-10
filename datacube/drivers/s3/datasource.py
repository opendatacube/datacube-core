'''A module offering an S3 datasource that mimicks the existing NetCDF
datasource behaviour.

******* UNDER DEVELOPMENT - NOT WORKING *********

'''

from __future__ import absolute_import

import logging
from contextlib import contextmanager

from affine import Affine

from datacube.drivers.datasource import DataSource
from datacube.storage.storage import OverrideBandDataSource


class S3Source(object):
    '''Proxy class to allow usage in OverrideBandDataSource without modifications.'''

    class S3DS(object):
        '''Proxy class to allow usage in OverrideBandDataSource without
        modifications, because it requires a source.ds.read method.
        '''
        def __init__(self, parent):
            self.parent = parent

        def read(self, indexes, window, out_shape):
            #: pylint: disable=superfluous-parens
            print('######### Calling from S3DS')
            self.parent.read(window, out_shape)

    def __init__(self, dataset, band_name):
        self.dataset = dataset
        self.band_name = band_name
        self.ds = self.S3DS(self)
        print('BIDX=', band_name)
        self.bidx = band_name # TODO(csiro) find what to put here: band index presumably? *****


    def read(self, window, write_shape):
        #: pylint: disable=superfluous-parens
        print('######### Calling Peter\'s read method with window=%s write_shape=%s' % (window, write_shape))

        # Sanity check that write_shape is the dleta between each dimension in window?

        # TODO(csiro) This will come from the index:
        #self.bucket = '/tmp/pytest-of-tai031/pytest-1/test_full_ingestion_s3_test_UT0/ls5_nbar_ingest_test'
        #self.basename = 'LS5_TM_NBAR_3577_16_-38_19900302231116000000_v1493621666_green'
        #self.nodata = dataset.type.measurements[band_name].get('nodata')
        #self.macro_shape = (40, 40) # Do NOT use time here

        # Return a ndarray from Peter's read method
        # Requires from S3_DATASET:
        #base_name=base_name,
        #band=band,
        #macro_shape=macro_shape,
        #chunk_size=chunk_size,
        #numpy_type=numpy_type,
        # Nothing from S3_DATASET_CHUNK because it can be recomputed
        # To be input to:
        # return DRIVER.s3lio.get_data_unlabeled('base_name_4', # base_name +'_'+ band_name
        #                                        (4, 4, 4), # macro_shape
        #                                        (3, 3, 3), # chunk_size
        #                                        np.int8, # numpy_type
        #                                        (slice(3, 4), slice(1, 3), slice(1, 3)),
        #                                               # window converted to tuple of slices
        #                                        'arrayio', # bucket
        #                                        False) # use md5 or not
        import sys
        sys.exit(0)
        import numpy as np
        return np.ones((40, 40), np.uint8)


class S3DataSource(DataSource):
    '''Data source for reading from a Datacube Dataset'''
    def __init__(self, dataset, band_name):
        import pprint
        pp = pprint.PrettyPrinter(indent=2)
        pp.pprint(dataset.__dict__)
        import sys
        sys.exit(0)

        self._dataset = dataset
        self.band_name = band_name
        self._measurement = dataset.measurements[band_name]
        self.source = S3Source(dataset, band_name)
        # Hardcoded for testing only:
        self.nodata = dataset.type.measurements[band_name].get('nodata')
        self.macro_shape = (40, 40) # Do NOT use time here

    @contextmanager
    def open(self):
        '''Context manager which returns a `BandDataSource`'''

        # TODO(csiro) Determine when/why to use OverrideBandDataSource or BandDataSource
        # Then, overload them as required above
        yield OverrideBandDataSource(self.source,
                                     nodata=self.nodata,
                                     crs=self.get_crs(),
                                     transform=self.get_transform(self.macro_shape))


    def get_bandnumber(self, src):
        return 1 ## TODO(csiro)) Return the time index matching the dataset.center_time (either regular or not)


    def get_transform(self, shape):
        return self._dataset.transform * Affine.scale(1/shape[1], 1/shape[0])


    def get_crs(self):
        return self._dataset.crs
