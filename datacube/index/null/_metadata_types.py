# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from typing_extensions import override

from datacube.index.abstract import AbstractMetadataTypeResource
from datacube.model import MetadataType


class MetadataTypeResource(AbstractMetadataTypeResource):
    @override
    def from_doc(self, definition):
        raise NotImplementedError

    @override
    def add(self, metadata_type, allow_table_lock=False):
        raise NotImplementedError

    @override
    def can_update(self, metadata_type, allow_unsafe_updates=False):
        raise NotImplementedError

    @override
    def update(self, metadata_type: MetadataType, allow_unsafe_updates=False, allow_table_lock=False):
        raise NotImplementedError

    @override
    def get_unsafe(self, id_):
        raise KeyError(id_)

    @override
    def get_by_name_unsafe(self, name):
        raise KeyError(name)

    @override
    def check_field_indexes(self, allow_table_lock=False,
                            rebuild_views=False, rebuild_indexes=False):
        raise NotImplementedError

    @override
    def get_all(self):
        return []
