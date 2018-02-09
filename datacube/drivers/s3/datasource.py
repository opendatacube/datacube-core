"""A module offering an S3 datasource that mimicks the existing NetCDF
datasource behaviour.
"""

from __future__ import absolute_import, division

import logging
from contextlib import contextmanager

from affine import Affine
from numpy import dtype

from datacube.drivers.datasource import DataSource
from datacube.storage.storage import OverrideBandDataSource
from datacube.utils import datetime_to_seconds_since_1970
from .utils import DriverUtils


class S3Source(object):
    """A data reader class, with an API similar to rasterio so it can be
    used without modification as a source in
    :class:`datacube.storage.storage.OverrideBandDataSource`.
    """

    class S3DS(object):
        """An inner reader class, mimicking the `source.ds` within
        :class:`datacube.storage.storage.OverrideBandDataSource`.
        """

        def __init__(self, parent):
            """Initialise the inner reader, which will simply call its parent read
            method.

            :param S3Source parent: The parent data reader.
            """
            self.parent = parent

        def read(self, indexes, window, out_shape):
            """Read a dataset slice from the storage.

            :return: The data returned by the parent
              :meth:`S3Source.read` method.
            """
            return self.parent.read(indexes, window, out_shape)

    def __init__(self, dataset, band_name, storage):
        """Initialise the data reader.

        :param datacube.model.Dataset dataset: The dataset to be read.
        :param str band_name: The name of the band to read in the
          dataset, e.g. 'blue'.
        :param datacube.drivers.s3.storage.s3aio.s3lio storage: The s3
          storage used by the s3 driver.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.storage = storage
        self.dataset = dataset
        self.band_name = band_name
        self.ds = self.S3DS(self)
        self.bidx = 1  # Called but unused in s3
        self.shape = dataset.s3_metadata[band_name]['s3_dataset'].macro_shape[-2:]

    def read(self, indexes, window, write_shape):
        """Read a dataset slice from the storage.

        :param window: If an :class:`S3Source` then the macro shape of
          the dataset will define the slices to obtain from
          storage. Otherwise, if a tuple, then the `window` itself
          defined the slices to obtain.

        :param write_shape: Ignored: the output shape is given by the
          slices. Only used for compliance with the
          :class:`datacube.storage.storage.OverrideBandDataSource`
          calls.
        """
        s3_dataset = self.dataset.s3_metadata[self.band_name]['s3_dataset']
        if isinstance(window, S3Source) or window is None:
            slices = tuple([slice(0, a) for a in s3_dataset.macro_shape[-2:]])
        else:
            slices = tuple([slice(a[0], a[1]) for a in window])

        # emulate a nd slice (time + 2D) -> (3D)
        slices = (slice(indexes, indexes + 1),) + slices

        self.logger.debug('Retrieving data from s3 (%s, slices: %s)', s3_dataset.base_name, slices)
        return self.storage.get_data_unlabeled_mp(s3_dataset.base_name,
                                                  s3_dataset.macro_shape,
                                                  s3_dataset.chunk_size,
                                                  dtype(s3_dataset.numpy_type),
                                                  slices,
                                                  s3_dataset.bucket,
                                                  True)[0]


class S3DataSource(DataSource):
    """Data source for reading from a Datacube Dataset."""

    def __init__(self, dataset, band_name, storage):
        """Prepare to read from the data source.

        :param datacube.model.Dataset dataset: The dataset to be read.
        :param str band_name: The name of the band to read in the
          dataset, e.g. 'blue'.
        :param datacube.drivers.s3.storage.s3aio.s3lio storage: The s3
          storage used by the s3 driver.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self._dataset = dataset
        self.source = S3Source(dataset, band_name, storage)
        self.nodata = dataset.type.measurements[band_name].get('nodata')
        self.macro_shape = dataset.s3_metadata[band_name]['s3_dataset'].macro_shape[1:]  # Do NOT use time here

    @contextmanager
    def open(self):
        """Context manager yielding a band datasource.

        :yields: A
          :class:`datacube.storage.storage.OverrideBandDataSource`
          which uses an :class:`S3Source` as a source.
        """
        self.source.bidx = self.get_bandnumber()

        yield OverrideBandDataSource(self.source,
                                     nodata=self.nodata,
                                     crs=self.get_crs(),
                                     transform=self.get_transform(self.macro_shape))

    def get_bandnumber(self):
        time = self._dataset.center_time
        sec_since_1970 = datetime_to_seconds_since_1970(time)
        s3_dataset = self._dataset.s3_metadata[self.source.band_name]['s3_dataset']

        if s3_dataset.regular_dims[0]:  # If time is regular
            return int((sec_since_1970 - s3_dataset.regular_index[0]) / s3_dataset.regular_index[2])
        else:
            epsilon = DriverUtils.epsilon('time')
            for idx, timestamp in enumerate(s3_dataset.irregular_index[0]):
                if abs(sec_since_1970 - timestamp / 1000000000.0) < epsilon:
                    return idx
        raise ValueError('Cannot find band number for centre time %s' % time)

    def get_transform(self, shape):
        """Return the transform scaled by a given factor.

        :param shape: The factor to rescale the transform by.
        :return: The scaled dataset.
        """
        return self._dataset.transform * Affine.scale(1.0 / shape[1], 1.0 / shape[0])

    def get_crs(self):
        """The dataset CRS.

        :return: The CRS of the dataset.
        """
        return self._dataset.crs
