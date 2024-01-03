# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
""" xarray 3D driver plugin for 3D support testing
"""
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Optional, Tuple, Union
from urllib.parse import urlparse

import numpy as np
import xarray as xr
from affine import Affine
from datacube.storage import BandInfo
from datacube.utils.math import num2numpy
from odc.geo import CRS

PROTOCOL = ["file", "xarray_3d"]
FORMAT = "xarray_3d"


def uri_split(uri: str) -> Tuple[str, str, str]:
    """
    Splits uri into protocol, root, and group

    Example:
        uri_split('file:///path/to/my_dataset.xarray_3d#group/subgroup/etc')
        returns ('file', '/path/to/my_dataset.xarray_3d', 'group/subgroup/etc')

    If the URI contains no '#' extension, the root group "" is returned.

    :param str uri: The URI to be parsed
    :return: (protocol, root, group)
    """
    components = urlparse(uri)
    scheme = components.scheme
    path = components.netloc + components.path
    if not scheme:
        raise ValueError(f"uri scheme not found: {uri}")
    group = components.fragment
    return scheme, path, group


RasterShape = Tuple[int, ...]
RasterWindow = Tuple[Union[int, Tuple[int, int]], ...]

load_no = 0


class XArrayDataSource3D(object):
    class BandDataSource(object):
        def __init__(
            self,
            dataset: xr.Dataset,
            var_name: str,
            no_data: Optional[float],
        ) -> None:
            """
            Initialises the BandDataSource class.

            The BandDataSource class to read array slices out of the xr.Dataset.

            :param xr.Dataset dataset: The xr.Dataset
            :param str var_name: The variable name of the xr.DataArray
            :param float no_data: The no data value if known
            """
            self.ds = dataset
            self._var_name = var_name
            self.da = dataset.data_vars[var_name]

            self._is_2d = len(self.da.dims) == 2
            self._nbands = 1 if self._is_2d else self.da[self.da.dims[0]].size
            if self._nbands == 0:
                raise ValueError("Dataset has 0 bands.")

            # Set nodata value
            if "nodata" in self.da.attrs and self.da.nodata:
                if isinstance(self.da.nodata, list):
                    self._nodata = self.da.nodata[0]
                else:
                    self._nodata = self.da.nodata
            else:
                self._nodata = no_data

            if not self._nodata:
                raise ValueError("nodata not found in dataset and product definition")

            self._nodata = num2numpy(self._nodata, self.dtype)

        @property
        def nodata(self) -> Optional[float]:
            return self._nodata  # type: ignore

        @property
        def crs(self) -> CRS:
            return self.da.crs

        @property
        def transform(self) -> Affine:
            return self.da.affine

        @property
        def dtype(self) -> np.dtype:
            return self.da.dtype

        @property
        def shape(self) -> RasterShape:
            return self.da.shape if self._is_2d else self.da.shape[1:]

        def read(
            self,
            window: Optional[RasterWindow] = None,
            out_shape: Optional[RasterShape] = None,
        ) -> np.ndarray:
            """
            Reads a slice into the xr.DataArray.

            :param RasterWindow window: The slice to read
            :param RasterShape out_shape: The desired output shape
            :return: Requested data in a :class:`numpy.ndarray`
            """

            if window is None:
                ix: Tuple = (...,)
            else:
                ix = tuple(slice(*w) if isinstance(w, tuple) else w for w in window)

            def fn() -> Any:
                return self.da[ix].values

            data = fn()

            if out_shape and data.shape != out_shape:
                raise ValueError(
                    f"Data shape does not match 'out_shape': {data.shape} != {out_shape}"
                )

            return data

    def __init__(self, band: BandInfo) -> None:
        """
        Initialises the XarrayDataSource3D class.

        :param BandInfo band: BandInfo containing the dataset metadata.
        """
        self._band_info = band
        if band.band == 0:
            raise ValueError("BandInfo.band must be > 0")

        # convert band.uri -> protocol, root and group
        protocol, filepath, _ = uri_split(band.uri)
        if protocol not in PROTOCOL:
            valid = " or ".join([f"{protocol}://" for protocol in PROTOCOL])
            raise ValueError(f"Expected {valid} url")
        self.filepath = Path(filepath)
        if not self.filepath.exists():
            raise ValueError(f"xarray_3d data file does not exist: {self.filepath}")

    @contextmanager
    def open(self) -> Generator[BandDataSource, None, None]:
        """Load xarray_3d dataset."""
        global load_no

        dataset = xr.open_dataset(self.filepath)

        var_name = self._band_info.layer or self._band_info.name
        load_no += 1
        yield XArrayDataSource3D.BandDataSource(
            dataset=dataset,
            var_name=var_name,
            no_data=self._band_info.nodata,
        )


class XArrayReaderDriver3D(object):
    def __init__(self) -> None:
        self.name = "XArrayReader3D"
        self.protocols = PROTOCOL
        self.formats = [FORMAT]

    def supports(self, protocol: str, fmt: str) -> bool:
        return protocol in self.protocols and fmt in self.formats

    def new_datasource(self, band: BandInfo) -> XArrayDataSource3D:
        return XArrayDataSource3D(band)


def reader_driver_init() -> XArrayReaderDriver3D:
    return XArrayReaderDriver3D()
