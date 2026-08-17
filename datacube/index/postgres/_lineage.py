# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from time import monotonic
from typing import override

from datacube.index.abstract import BatchStatus, NoLineageResource
from datacube.index.postgres._transaction import IndexResourceAddIn
from datacube.model import LineageRelation, dsid_to_uuid

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Iterable
    from uuid import UUID

    from datacube.model import DSID
    from datacube.model.lineage import Eo3LineageDict


class LineageResource(NoLineageResource, IndexResourceAddIn):
    def __init__(self, db, index) -> None:
        self._db = db
        super().__init__(index)

    @override
    def get_all_lineage(self, batch_size: int = 1000) -> Iterable[LineageRelation]:
        with self._db_connection(transaction=True) as connection:
            for row in connection.get_all_lineage(batch_size=batch_size):
                yield LineageRelation(
                    derived_id=row.dataset_ref,
                    classifier=row.classifier,
                    source_id=row.source_dataset_ref,
                )

    @override
    def _add_batch(self, batch_rels: Iterable[LineageRelation]) -> BatchStatus:
        b_started = monotonic()
        with self._db_connection(transaction=True) as connection:
            b_added, b_skipped = connection.insert_lineage_bulk(
                [
                    (str(rel.derived_id), rel.classifier, str(rel.source_id))
                    for rel in batch_rels
                ]
            )
        return BatchStatus(b_added, b_skipped, monotonic() - b_started)

    @override
    def get_derived_ids(self, id_: DSID) -> Eo3LineageDict:
        lineage: dict[str, list[UUID]] = {}
        with self._db_connection() as connection:
            for rel in connection.get_derived_relations(dsid_to_uuid(id_)):
                if deriveds := lineage.get(rel.classifier):
                    deriveds.append(rel.derived_id)
                else:
                    lineage[rel.classifier] = [rel.derived_id]
        return lineage

    @override
    def get_source_ids(self, id_: DSID) -> Eo3LineageDict:
        lineage: dict[str, list[UUID]] = {}
        with self._db_connection() as connection:
            for rel in connection.get_source_relations(dsid_to_uuid(id_)):
                # Lineage rewriting forces single source per classifier in postgres driver
                assert rel.classifier not in lineage, "Lineage rewrite error"
                lineage[rel.classifier] = [rel.source_id]
        return lineage
