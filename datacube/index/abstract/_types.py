# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Iterable, Sequence
from typing import NamedTuple
from uuid import UUID

from deprecat import deprecat

from datacube.migration import ODC2DeprecationWarning
from datacube.model import Dataset, Product
from datacube.utils import cached_property
from datacube.utils.documents import JsonDict


class BatchStatus(NamedTuple):
    """
    A named tuple representing the results of a batch add operation:

      - completed: Number of objects added to theMay be None for internal functions and for datasets.
      - skipped: Number of objects skipped, either because they already exist
        or the documents are invalid for this driver.
      - seconds_elapsed: seconds elapsed during the bulk add operation;
      - safe: an optional list of names of bulk added objects that are safe to be
        used for lower level bulk adds. Includes objects added, and objects skipped
        because they already exist in the index and are identical to the version
        being added.  May be None for internal functions and for datasets.
    """

    completed: int
    skipped: int
    seconds_elapsed: float
    safe: Iterable[str] | None = None


# Non-strict Dataset ID representation

DSID = str | UUID


def dsid_to_uuid(dsid: DSID) -> UUID:
    """
    Convert non-strict dataset ID representation to strict UUID
    """
    if isinstance(dsid, UUID):
        return dsid
    else:
        return UUID(dsid)


class DatasetTuple(NamedTuple):
    """
    A named tuple representing a complete dataset:
    - product: A Product model.
    - metadata: The dataset metadata document
    - uri_: The dataset location or list of locations
    """

    product: Product
    metadata: JsonDict
    uri_: str | list[str]

    @property
    def uri_is_string(self) -> bool:
        if isinstance(self.uri_, str):
            return True
        return False

    @property
    def is_legacy(self) -> bool:
        return not isinstance(self.uri_, str) and len(self.uri_) > 1

    @property
    def uri(self) -> str:
        if isinstance(self.uri_, str):
            return self.uri_
        return self.uri_[0]

    @property
    @deprecat(
        reason="Multiple uris are deprecated. Please use the uri field and ensure that datasets only have one location",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def uris(self) -> Sequence[str]:
        if isinstance(self.uri_, str):
            return [self.uri_]
        return self.uri_


# The special handling of grid_spatial, etc. appears to NOT apply to EO3.
# Does EO3 handle it in metadata?
class DatasetSpatialMixin:
    __slots__ = ()

    @property
    def _gs(self):
        return self.grid_spatial

    @property
    def crs(self):
        return Dataset.crs.__get__(self)

    @cached_property
    def extent(self):
        return Dataset.extent.func(self)

    @property
    def transform(self):
        return Dataset.transform.__get__(self)

    @property
    def bounds(self):
        return Dataset.bounds.__get__(self)
