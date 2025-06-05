# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from urllib.parse import urlsplit

from datacube.storage._rio import RasterDatasetDataSource
from datacube.utils.uris import normalise_path

from ._write import write_dataset_to_netcdf

PROTOCOL = "file"
FORMAT = "NetCDF"


class NetcdfReaderDriver:
    def __init__(self) -> None:
        self.name = "NetcdfReader"
        self.protocols = [PROTOCOL]
        self.formats = [FORMAT]

    def supports(self, protocol, fmt) -> bool:
        return protocol in self.protocols and fmt in self.formats

    def new_datasource(self, band) -> RasterDatasetDataSource:
        return RasterDatasetDataSource(band)


def reader_driver_init() -> NetcdfReaderDriver:
    return NetcdfReaderDriver()


class NetcdfWriterDriver:
    def __init__(self) -> None:
        pass

    @property
    def aliases(self) -> list[str]:
        return ["NetCDF CF"]

    @property
    def format(self) -> str:
        return FORMAT

    @property
    def uri_scheme(self) -> str:
        return PROTOCOL

    def mk_uri(self, file_path: Path | str) -> str:
        """
        Constructs a URI from the file_path and storage config.

        A typical implementation should return f'{scheme}://{file_path}'

        Example:
            file_path = '/path/to/my_file.nc'

            mk_uri(file_path) should return 'file:///path/to/my_file.nc'

        :param file_path: The file path of the file to be converted into a URI.
        :return: file_path as a URI that the Driver understands.
        """
        return normalise_path(file_path).as_uri()

    def write_dataset_to_storage(
        self, dataset, file_uri, global_attributes=None, variable_params=None, **kwargs
    ) -> dict:
        write_dataset_to_netcdf(
            dataset,
            urlsplit(file_uri).path,
            global_attributes=global_attributes,
            variable_params=variable_params,
            **kwargs,
        )

        return {}


def writer_driver_init() -> NetcdfWriterDriver:
    return NetcdfWriterDriver()
