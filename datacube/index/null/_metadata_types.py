# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

from datacube.index.abstract import AbstractMetadataTypeResource
from datacube.model import MetadataType


class MetadataTypeResource(AbstractMetadataTypeResource):
    def from_doc(self, definition):
        raise NotImplementedError

    def add(self, metadata_type, allow_table_lock=False):
        raise NotImplementedError

    def can_update(self, metadata_type, allow_unsafe_updates=False):
        raise NotImplementedError

    def update(self, metadata_type: MetadataType, allow_unsafe_updates=False, allow_table_lock=False):
        raise NotImplementedError

    def get_unsafe(self, id_):
        raise KeyError(id_)

    def get_by_name_unsafe(self, name):
        raise KeyError(name)

    def check_field_indexes(self, allow_table_lock=False,
                            rebuild_views=False, rebuild_indexes=False):
        raise NotImplementedError

    def get_all(self):
        return []
