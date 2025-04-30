# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Utility functions
"""

from .dates import parse_time
from .py import cached_property, ignore_exceptions_if, import_function
from .serialise import jsonify_document
from .uris import is_url, uri_to_local_path, get_part_from_uri, mk_part_uri, is_vsipath
from .io import slurp, check_write_path, write_user_secret_file
from .documents import (
    InvalidDocException,
    SimpleDocNav,
    DocReader,
    is_supported_document_type,
    read_strings_from_netcdf,
    read_documents,
    validate_document,
    NoDatesSafeLoader,
    get_doc_offset,
    netcdf_extract_string,
    without_lineage_sources,
    schema_validated,
    _readable_offset,
)
from .math import (
    unsqueeze_dataset,
    unsqueeze_data_array,
    spatial_dims,
    iter_slices,
)
from ._misc import (
    DatacubeException,
    gen_password,
    report_to_user
)


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
    "write_user_secret_file"
]
