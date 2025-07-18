# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Iterable

from typing_extensions import override

from datacube.index.abstract import AbstractMetadataTypeResource
from datacube.model import MetadataType
from datacube.utils.changes import Change
from datacube.utils.documents import JsonDict


class MetadataTypeResource(AbstractMetadataTypeResource):
    @override
    def from_doc(self, definition: JsonDict) -> MetadataType:
        raise NotImplementedError

    @override
    def add(
        self, metadata_type: MetadataType, allow_table_lock: bool = False
    ) -> MetadataType:
        raise NotImplementedError

    @override
    def can_update(
        self, metadata_type: MetadataType, allow_unsafe_updates: bool = False
    ) -> tuple[bool, Iterable[Change], Iterable[Change]]:
        raise NotImplementedError

    @override
    def update(
        self,
        metadata_type: MetadataType,
        allow_unsafe_updates: bool = False,
        allow_table_lock: bool = False,
    ) -> MetadataType:
        raise NotImplementedError

    @override
    def get_unsafe(self, id_: int) -> MetadataType:
        raise KeyError(id_)

    @override
    def get_by_name_unsafe(self, name: str) -> MetadataType:
        raise KeyError(name)

    @override
    def check_field_indexes(
        self,
        allow_table_lock: bool = False,
        rebuild_views: bool = False,
        rebuild_indexes: bool = False,
    ) -> None:
        raise NotImplementedError

    @override
    def get_all(self) -> Iterable[MetadataType]:
        return []
