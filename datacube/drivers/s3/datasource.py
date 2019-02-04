"""A module offering an S3 datasource that mimicks the existing NetCDF
datasource behaviour.
"""
import logging
from contextlib import contextmanager
from typing import Dict, Any

from affine import Affine
from numpy import dtype

from datacube.drivers.datasource import DataSource
from datacube.storage._rio import OverrideBandDataSource
from datacube.storage import BandInfo
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

    def __init__(self, band: BandInfo, storage):
        """Initialise the data reader.

        :param band: The band from dataset to be read.
        :param datacube.drivers.s3.storage.s3aio.s3lio storage: The s3
          storage used by the s3 driver.
        """
        if band.driver_data is None:
            raise ValueError("Missing driver data")

        self.logger = logging.getLogger(self.__class__.__name__)
        self.storage = storage
        self.band = band
        self.s3_metadata = band.driver_data  # type: Dict[str, Any]
        self.ds = self.S3DS(self)
        self.bidx = 1  # Called but unused in s3
        self.shape = self.s3_metadata[band.name]['s3_dataset'].macro_shape[-2:]
        self.dtype = dtype(self.s3_metadata[band.name]['s3_dataset'].numpy_type)

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
        s3_dataset = self.s3_metadata[self.band.name]['s3_dataset']
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

    def __init__(self, band: BandInfo, storage):
        """Prepare to read from the data source.

        :param band: The band of a dataset to be read.
        :param datacube.drivers.s3.storage.s3aio.s3lio storage: The s3
          storage used by the s3 driver.
        """
        if band.driver_data is None:
            raise ValueError("Missing driver data")

        self.logger = logging.getLogger(self.__class__.__name__)
        self._s3_metadata = band.driver_data  # type: Dict[str, Any]
        self._band = band
        self.source = S3Source(band, storage)
        self.nodata = band.nodata
        self.macro_shape = self._s3_metadata[band.name]['s3_dataset'].macro_shape[1:]  # Do NOT use time here

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
        time = self._band.center_time
        sec_since_1970 = datetime_to_seconds_since_1970(time)
        s3_dataset = self._s3_metadata[self._band.name]['s3_dataset']

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
        return self._band.transform * Affine.scale(1.0 / shape[1], 1.0 / shape[0])

    def get_crs(self):
        """The dataset CRS.

        :return: The CRS of the dataset.
        """
        return self._band.crs
