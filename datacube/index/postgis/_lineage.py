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
from datacube.model.lineage import LineageRelations, LineageRelation


class LineageResource(AbstractLineageResource, IndexResourceAddIn):
    def __init__(self, db: PostgisDbAPI, index: AbstractIndex) -> None:
        """
        :type db: datacube.drivers.postgis._connections.PostgresDb
        :type metadata_type_resource: datacube.index._metadata_types.MetadataTypeResource
        """
        self._db = db
        super().__init__(index)

    def get_derived_tree(self, id_: DSID, max_depth: int = 0) -> LineageTree:
        with self._db_connection() as connection:
            relations = connection.load_lineage_relations([id_],
                                                          LineageDirection.DERIVED,
                                                          max_depth)
        rels = LineageRelations(relations=relations)
        return rels.extract_tree(id_, LineageDirection.DERIVED)

    def get_source_tree(self, id_: DSID, max_depth: int = 0) -> LineageTree:
        with self._db_connection() as connection:
            relations = connection.load_lineage_relations([id_],
                                                          LineageDirection.SOURCES,
                                                          max_depth)
        rels = LineageRelations(relations=relations)
        return rels.extract_tree(id_, LineageDirection.SOURCES)

    def add(self, tree: LineageTree, max_depth: int = 0, allow_updates: bool = False) -> None:
        # Convert to a relations collection
        relations = LineageRelations(tree=tree, max_depth=max_depth)
        with self._db_connection() as connection:
            # Get all current relations one step forwards and backwards from all dataset ids in the tree.
            db_relations = LineageRelations(
                relations=connection.get_all_relations(relations.dataset_ids),
                homes=connection.select_homes(relations.dataset_ids)
            )
            # Check for consistency:
            new_rels, update_rels, new_homes, update_homes = relations.relations_diff(
                existing_relations=db_relations,
                allow_updates=allow_updates
            )
            # Merge homes data
            if new_homes:
                homes_new = {}
                for id_, home in new_homes.items():
                    if id_ in homes_new:
                        homes_new[home].append(id_)
                    else:
                        homes_new[home] = [id_]
                for home, ids in homes_new.items():
                    connection.insert_home(home, ids, allow_updates=False)
            if update_homes:
                homes_update = {}
                for id_, home in update_homes.items():
                    if id_ in homes_update:
                        homes_update[home].append(id_)
                    else:
                        homes_update[home] = [id_]
                for home, ids in homes_update.items():
                    connection.insert_home(home, ids, allow_updates=allow_updates)
            # Merge Relations data
            rels_new = [
                LineageRelation(classifier=classifier, derived_id=derived, source_id=src)
                for (derived, src), classifier in new_rels.items()
            ]
            rels_update = [
                LineageRelation(classifier=classifier, derived_id=derived, source_id=src)
                for (derived, src), classifier in update_rels.items()
            ]
            connection.write_relations(rels_new, allow_updates=False)
            connection.write_relations(rels_update, allow_updates=True)

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
