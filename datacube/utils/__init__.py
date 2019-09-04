"""
Utility functions
"""

from .dates import datetime_to_seconds_since_1970, parse_time
from .py import cached_property, ignore_exceptions_if, import_function
from .serialise import jsonify_document
from .uris import is_url, uri_to_local_path, get_part_from_uri, mk_part_uri
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
    get_doc_offset_safe,
    netcdf_extract_string,
    without_lineage_sources,
)
from .math import (
    unsqueeze_data_array,
    iter_slices,
    unsqueeze_dataset,
    data_resolution_and_offset,
)
from ._misc import (
    DatacubeException,
    schema_validated,
    gen_password,
    _readable_offset,
)


__all__ = (
    "datetime_to_seconds_since_1970",
    "parse_time",
    "cached_property",
    "ignore_exceptions_if",
    "import_function",
    "jsonify_document",
    "is_url",
    "uri_to_local_path",
    "get_part_from_uri",
    "mk_part_uri",
    "InvalidDocException",
    "SimpleDocNav",
    "DocReader",
    "is_supported_document_type",
    "read_strings_from_netcdf",
    "read_documents",
    "validate_document",
    "NoDatesSafeLoader",
    "get_doc_offset",
    "get_doc_offset_safe",
    "netcdf_extract_string",
    "without_lineage_sources",
    "unsqueeze_data_array",
    "iter_slices",
    "unsqueeze_dataset",
    "data_resolution_and_offset",
    "DatacubeException",
    "schema_validated",
    "write_user_secret_file",
    "slurp",
    "check_write_path",
    "gen_password",
    "_readable_offset",
)
