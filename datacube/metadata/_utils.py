# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Mirrored helper functions for STAC/EO3 conversion
"""

from datetime import datetime
from typing import Any

from pystac.utils import datetime_to_str

from datacube.index.abstract import default_metadata_type_docs
from datacube.model import Dataset, MetadataType, metadata_from_doc
from datacube.utils import parse_time

# Mapping between EO3 field names and STAC properties object field names
# EO3 metadata was defined before STAC 1.0, so we used some extensions
# that are now part of the standard instead
STAC_TO_EO3_RENAMES = {
    "end_datetime": "dtr:end_datetime",
    "start_datetime": "dtr:start_datetime",
    "gsd": "eo:gsd",
    "instruments": "eo:instrument",
    "platform": "eo:platform",
    "constellation": "eo:constellation",
    "view:off_nadir": "eo:off_nadir",
    "view:azimuth": "eo:azimuth",
    "view:sun_azimuth": "eo:sun_azimuth",
    "view:sun_elevation": "eo:sun_elevation",
    "created": "odc:processing_datetime",
}

EO3_TO_STAC_RENAMES: dict[str, str] = {v: k for k, v in STAC_TO_EO3_RENAMES.items()}

_default_md_types = {
    d.get("name"): metadata_from_doc(d) for d in default_metadata_type_docs()
}
EO3_MD_TYPE: MetadataType = _default_md_types["eo3"]
EO_MD_TYPE: MetadataType = _default_md_types["eo"]


def _as_stac_instruments(value: str) -> list[str]:
    """
    >>> _as_stac_instruments("TM")
    ['tm']
    >>> _as_stac_instruments("OLI")
    ['oli']
    >>> _as_stac_instruments("ETM+")
    ['etm']
    >>> _as_stac_instruments("OLI_TIRS")
    ['oli', 'tirs']
    """
    return [i.strip("+-").lower() for i in value.split("_")]


def _value_to_stac_type(key: str, value):
    """
    Convert return type as per STAC specification
    """
    # In STAC spec, "instruments" have [String] type
    if key == "eo:instrument":
        return _as_stac_instruments(value)
    # Convert the non-default datetimes to a string
    if isinstance(value, datetime) and key != "datetime":
        return datetime_to_str(value)
    if isinstance(value, str) and "datetime" in key:
        try:
            return datetime_to_str(datetime.fromisoformat(value))
        except ValueError:
            # fromisoformat doesn't play nice with "Z" at the end of the timestamp in 3.10
            return datetime_to_str(datetime.fromisoformat(value[:-1]))
    return value


def _value_to_eo3_type(key: str, value) -> str | datetime | None:
    # TODO: remove once list field types are supported
    if key == "instruments":
        if len(value) > 0:
            return "_".join([i.upper() for i in value])
        return None
    if key == "created" or "datetime" in key:
        if isinstance(value, str):
            return parse_time(value)
        return value
    return value


def eo3_to_stac_properties(dataset: Dataset, title: str | None = None) -> dict:
    """
    Convert EO3 properties dictionary to the Stac equivalent.
    """
    # Put the title at the top for document readability.
    properties = {"title": title} if title else {}

    properties.update(
        {
            EO3_TO_STAC_RENAMES.get(key, key): _value_to_stac_type(key, val)
            for key, val in dataset.properties.items()
        },
    )

    return properties


def stac_to_eo3_properties(properties: dict[str, Any]) -> dict[str, Any]:
    """
    Convert STAC properties dictionary to the EO3 equivalent.
    """
    prop = {
        STAC_TO_EO3_RENAMES.get(key, key): _value_to_eo3_type(key, val)
        for key, val in properties.items()
        if not key.startswith("proj:")
    }

    if prop.get("odc:processing_datetime") is None:
        # default to datetime value if no created
        prop["odc:processing_datetime"] = properties.get("datetime")

    if prop.get("odc:file_format") is None:
        prop["odc:file_format"] = "GeoTIFF"

    return prop
