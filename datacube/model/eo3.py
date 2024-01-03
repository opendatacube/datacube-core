# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from datacube.utils.documents import InvalidDocException

required_sys_field_values = [
    "id", "label", "creation_dt", "sources",
    # Add format here?
    # Drop sources as it appears to be ignored?
]

expected_sys_field_values = {
    # Enforce exactly
    "id": lambda x: x == ["id"],
    "measurements": lambda x: x == ["measurements"],
    "label": lambda x: x == ["label"],
    "creation_dt": lambda x: x == ["properties", "odc:processing_datetime"],
    "format": lambda x: x == ["properties", "odc:file_format"],
    # EO3 mdt has sources at [lineage,source_datasets], but it is actually expected at [lineage],
    # so accept anything vaguely right
    "sources": lambda x: x[0] == "lineage",
    # Actually kept under geometry and grids.  Ignore - why is this even here?
    "grid_spatial": lambda x: True,
    # Placeholder for search fields section
    "search_fields": lambda x: True,
}


def validate_eo3_offset(field_name, mdt_name, offset):
    if not all(isinstance(element, str) for element in offset):
        # Not a simple offset, assume a compound offset
        for element in offset:
            validate_eo3_offset(field_name, mdt_name, element)
        return
    # Simple offset validation
    # Special EO3 offsets
    if offset in [
        ["crs"],
        # Others??
    ]:
        return
    # Everything else should stored flat in properties
    if offset[0] != "properties" or len(offset) != 2:
        raise InvalidDocException(
            f"Search_field {field_name} in metadata type {mdt_name} "
            f"is not stored in an EO3-compliant location: {offset!r}")


def validate_eo3_offsets(field_name, mdt_name, defn):
    if defn.get("type", "string").endswith("-range"):
        # Range Type
        if "min_offset" in defn:
            validate_eo3_offset(field_name, mdt_name, defn["min_offset"])
        else:
            raise InvalidDocException(f"No min_offset supplied for field {field_name} in metadata type {mdt_name}")
        if "max_offset" in defn:
            validate_eo3_offset(field_name, mdt_name, defn["max_offset"])
        else:
            raise InvalidDocException(f"No max_offset supplied for field {field_name} in metadata type {mdt_name}")
    else:
        # Scalar Type
        if "offset" in defn:
            validate_eo3_offset(field_name, mdt_name, defn["offset"])
        else:
            raise InvalidDocException(f"No offset supplied for field {field_name} in metadata type {mdt_name}")


def validate_eo3_compatible_type(doc):
    """
    Valdate that a metadata type document is EO3 compatible.

    N.B. Assumes the document has already been validated as a valid ODC metadata type document.

    :param doc: A metadata type document

    Raises InvalidDocException if not EO3 compatible
    """
    # Validate that a metadata type doc is EO3 compatible.
    name = doc["name"]
    # Validate system field offsets
    for k, v in doc["dataset"].items():
        try:
            if not expected_sys_field_values[k](v):
                raise InvalidDocException(
                    f"Offset for system field {k} ({v!r}) in metadata type {name} does not match EO3 standard"
                )
        except KeyError as e:
            raise InvalidDocException(f"Unexpected system field in metadata type {name}: {k}")
    # Validate search field offsets
    for k, v in doc["dataset"]["search_fields"].items():
        if k in ["lat", "lon"]:
            continue
        validate_eo3_offsets(k, name, v)
