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

    def __init__(self, dataset):
        self.dataset = dataset
        self.ds = self.S3DS(self)
        self.bidx = 1 # TODO(csiro) find what to put here: band index presumably? *****


    def read(self, window, write_shape):
        #: pylint: disable=superfluous-parens
        print('######### Calling Peter\'s read method with window=%s write_shape=%s' % (window, write_shape))
        return None



class S3DataSource(DataSource):
    '''Data source for reading from a Datacube Dataset'''
    def __init__(self, dataset, band_name):
        self._dataset = dataset
        self._measurement = dataset.measurements[band_name]
        self.source = S3Source(dataset)

        # TODO(csiro) This will come from the index:
        self.bucket = '/tmp/pytest-of-tai031/pytest-1/test_full_ingestion_s3_test_UT0/ls5_nbar_ingest_test'
        self.basename = 'LS5_TM_NBAR_3577_16_-38_19900302231116000000_v1493621666_green'
        self.nodata = dataset.type.measurements[band_name].get('nodata')
        self.macro_shape = (40, 40) # Do NOT use time here


    @contextmanager
    def open(self):
        '''Context manager which returns a `BandDataSource`'''
        yield OverrideBandDataSource(self.source,
                                     nodata=self.nodata,
                                     crs=self.get_crs(),
                                     transform=self.get_transform(self.macro_shape))

    #     try:
    #         _LOG.debug("opening %s", self.filename)
    #         with rasterio.open(self.filename) as src:
    #             override = False

    #             transform = _rasterio_transform(src)
    #             if transform.is_identity:
    #                 override = True
    #                 transform = self.get_transform(src.shape)

    #             try:
    #                 crs = geometry.CRS(_rasterio_crs_wkt(src))
    #             except ValueError:
    #                 override = True
    #                 crs = self.get_crs()

    #             bandnumber = self.get_bandnumber(src)
    #             band = rasterio.band(src, bandnumber)
    #             nodata = numpy.dtype(band.dtype).type(src.nodatavals[0] if src.nodatavals[0] is not None
    #                                                   else self.nodata)

    #             if override:

    #             else:
    #                 yield BandDataSource(band, nodata=nodata)

    #     except Exception as e:
    #         _LOG.error("Error opening source dataset: %s", self.filename)
    #         raise e

    def get_bandnumber(self, src):
        return 1 ## TODO(csiro))

    def get_transform(self, shape):
        return self._dataset.transform * Affine.scale(1/shape[1], 1/shape[0])

    def get_crs(self):
        return self._dataset.crs
