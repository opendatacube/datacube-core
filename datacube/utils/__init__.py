# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Utility functions
"""

from ._misc import DatacubeException, gen_password, report_to_user
from .dates import parse_time
from .documents import (
    DocReader,
    InvalidDocException,
    NoDatesSafeLoader,
    SimpleDocNav,
    _readable_offset,
    get_doc_offset,
    is_supported_document_type,
    netcdf_extract_string,
    read_documents,
    read_strings_from_netcdf,
    schema_validated,
    validate_document,
    without_lineage_sources,
)
from .io import check_write_path, slurp, write_user_secret_file
from .math import (
    iter_slices,
    spatial_dims,
    unsqueeze_data_array,
    unsqueeze_dataset,
)
from .py import cached_property, ignore_exceptions_if, import_function
from .serialise import jsonify_document
from .uris import get_part_from_uri, is_url, is_vsipath, mk_part_uri, uri_to_local_path

__all__ = [
    "DatacubeException",
    "DocReader",
    "InvalidDocException",
    "NoDatesSafeLoader",
    "SimpleDocNav",
    "_readable_offset",
    "cached_property",
    "check_write_path",
    "gen_password",
    "get_doc_offset",
    "get_part_from_uri",
    "ignore_exceptions_if",
    "import_function",
    "is_supported_document_type",
    "is_url",
    "is_vsipath",
    "iter_slices",
    "jsonify_document",
    "mk_part_uri",
    "netcdf_extract_string",
    "parse_time",
    "read_documents",
    "read_strings_from_netcdf",
    "report_to_user",
    "schema_validated",
    "slurp",
    "spatial_dims",
    "unsqueeze_data_array",
    "unsqueeze_dataset",
    "uri_to_local_path",
    "validate_document",
    "without_lineage_sources",
    "write_user_secret_file",
]
