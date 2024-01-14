# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from time import monotonic
from typing import Mapping, Optional, Iterable
from uuid import UUID

from datacube.index.abstract import AbstractIndex, AbstractLineageResource, DSID, dsid_to_uuid, BatchStatus
from datacube.index.postgis._transaction import IndexResourceAddIn
from datacube.drivers.postgis._api import PostgisDbAPI
from datacube.model import LineageTree, LineageDirection, LineageRelation
from datacube.model.lineage import LineageRelations


class LineageResource(AbstractLineageResource, IndexResourceAddIn):
    def __init__(self, db: PostgisDbAPI, index: AbstractIndex) -> None:
        """
        :type db: datacube.drivers.postgis._connections.PostgresDb
        :type metadata_type_resource: datacube.index._metadata_types.MetadataTypeResource
        """
        self._db = db
        super().__init__(index)

    def get_derived_tree(self, id_: DSID, max_depth: int = 0) -> LineageTree:
        return self.get_lineage_tree(id_, LineageDirection.DERIVED, max_depth)

    def get_source_tree(self, id_: DSID, max_depth: int = 0) -> LineageTree:
        return self.get_lineage_tree(id_, LineageDirection.SOURCES, max_depth)

    def get_lineage_tree(self, id_: DSID, direction: LineageDirection, max_depth: int):
        with self._db_connection() as connection:
            # Extract lineage relations into a collection
            relations = connection.load_lineage_relations([id_],
                                                          direction,
                                                          max_depth)
            rels = LineageRelations(relations=relations)
            # Extract home information into the collection
            homes = connection.select_homes(rels.dataset_ids)
        for dsid, home in homes.items():
            rels.merge_new_home(dsid, home)
        # Extract tree from collection
        return rels.extract_tree(id_, direction)

    def add(self, tree: LineageTree, max_depth: int = 0, allow_updates: bool = False) -> None:
        # Convert to a relations collection
        relations = LineageRelations(tree=tree, max_depth=max_depth)
        # and merge into index.
        self.merge(relations, allow_updates=allow_updates)

    def merge(self, rels: LineageRelations, allow_updates: bool = False, validate_only: bool = False) -> None:
        if allow_updates and validate_only:
            raise ValueError("Cannot validate-only AND allow updates")
        with self._db_connection() as connection:
            # Get all current relations one step forwards and backwards from all dataset ids in the tree.
            db_relations = LineageRelations(
                relations=connection.get_all_relations(rels.dataset_ids),
                homes=connection.select_homes(rels.dataset_ids)
            )
            # Check for consistency:
            new_rels, update_rels, new_homes, update_homes = rels.relations_diff(
                existing_relations=db_relations,
                allow_updates=allow_updates
            )
            if validate_only:
                # If we get to here, data is safe to add
                return
            # Merge homes data
            if new_homes:
                homes_new = {}
                for id_, home in new_homes.items():
                    if home in homes_new:
                        homes_new[home].append(id_)
                    else:
                        homes_new[home] = [id_]
                for home, ids in homes_new.items():
                    connection.insert_home(home, ids, allow_updates=False)
            if update_homes:
                homes_update = {}
                for id_, home in update_homes.items():
                    if home in homes_update:
                        homes_update[home].append(id_)
                    else:
                        homes_update[home] = [id_]
                for home, ids in homes_update.items():
                    connection.insert_home(home, ids, allow_updates=allow_updates)
            # Merge Relations data
            rels_new = [
                LineageRelation(classifier=classifier, derived_id=ids.derived_id, source_id=ids.source_id)
                for ids, classifier in new_rels.items()
            ]
            rels_update = [
                LineageRelation(classifier=classifier, derived_id=ids.derived_id, source_id=ids.source_id)
                for ids, classifier in update_rels.items()
            ]
            connection.write_relations(rels_new, allow_updates=False)
            connection.write_relations(rels_update, allow_updates=True)

    def remove(self, id_: DSID, direction: LineageDirection, max_depth: int = 0) -> None:
        with self._db_connection() as connection:
            # Convert tree to desired deoth to lineage relations collection
            relations = connection.load_lineage_relations([id_],
                                                          direction,
                                                          max_depth)
            rels = LineageRelations(relations=relations)
            if direction == LineageDirection.SOURCES:
                ids = list(rels.by_derived.keys())
            else:
                ids = list(rels.by_source.keys())
            # Delete individual relations.
            connection.remove_lineage_relations(ids, direction)

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

    def get_all_lineage(self, batch_size: int = 1000) -> Iterable[LineageRelation]:
        with self._db_connection(transaction=True) as connection:
            for row in connection.get_all_lineage(batch_size=batch_size):
                yield LineageRelation(
                    derived_id=row.derived_dataset_ref,
                    classifier=row.classifier,
                    source_id=row.source_dataset_ref
                )

    def _add_batch(self, batch: Iterable[LineageRelation]) -> BatchStatus:
        b_started = monotonic()
        with self._db_connection(transaction=True) as connection:
            b_added, b_skipped = connection.insert_lineage_bulk(
                [
                    {
                        "derived_dataset_ref": rel.derived_id,
                        "classifier": rel.classifier,
                        "source_dataset_ref": rel.source_id,
                    }
                    for rel in batch
                ]
            )
        return BatchStatus(b_added, b_skipped, monotonic() - b_started)
