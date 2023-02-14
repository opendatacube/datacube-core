# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from typing import Mapping, Optional
from uuid import UUID

from datacube.index.abstract import AbstractIndex, AbstractLineageResource, DSID, dsid_to_uuid
from datacube.index.postgis._transaction import IndexResourceAddIn
from datacube.drivers.postgis._api import PostgisDbAPI
from datacube.model import LineageTree, LineageDirection


class LineageResource(AbstractLineageResource, IndexResourceAddIn):
    def __init__(self, db: PostgisDbAPI, index: AbstractIndex) -> None:
        """
        :type db: datacube.drivers.postgis._connections.PostgresDb
        :type metadata_type_resource: datacube.index._metadata_types.MetadataTypeResource
        """
        self._db = db
        super().__init__(index)

    def get_derived_tree(self, id: DSID, max_depth: int = 0) -> LineageTree:
        raise NotImplementedError("TODO")

    def get_source_tree(self, id: DSID, max_depth: int = 0) -> LineageTree:
        raise NotImplementedError("TODO")

    def add(self, tree: LineageTree, max_depth: int = 0, allow_updates: bool = False) -> None:
        raise NotImplementedError("TODO")

    def remove(self, id_: DSID, direction: LineageDirection, max_depth: int = 0) -> None:
        raise NotImplementedError("TODO")

    def set_home(self, home: str, *args: DSID, allow_updates: bool = False) -> int:
        with self._db_connection() as connection:
            ids = (dsid_to_uuid(id_) for id_ in args)
            return connection.insert_home(home, ids, allow_updates)

    def clear_home(self, *args: DSID, home: Optional[str] = None) -> int:
        ids = [dsid_to_uuid(id_) for id_ in args]
        with self._db_connection() as connection:
            return connection.delete_home(ids)

    def get_homes(self, *args: DSID) -> Mapping[UUID, str]:
        ids = [dsid_to_uuid(id_) for id_ in args]
        with self._db_connection() as connection:
            return connection.select_homes(ids)
