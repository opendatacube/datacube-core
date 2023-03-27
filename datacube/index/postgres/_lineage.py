# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from time import monotonic
from typing import Iterable

from datacube.index.abstract import NoLineageResource, BatchStatus
from datacube.index.postgres._transaction import IndexResourceAddIn
from datacube.model import LineageRelation


class LineageResource(NoLineageResource, IndexResourceAddIn):
    def __init__(self, db, index):
        self._db = db
        super().__init__(index)

    def get_all_lineage(self, batch_size: int = 1000) -> Iterable[LineageRelation]:
        with self._db_connection(transaction=True) as connection:
            for row in connection.get_all_lineage(batch_size=batch_size):
                yield LineageRelation(
                    derived_id=row.dataset_ref,
                    classifier=row.classifier,
                    source_id=row.dataset_source_ref
                )

    def _add_batch(self, batch_rels: Iterable[LineageRelation]) -> BatchStatus:
        b_started = monotonic()
        return BatchStatus(0, 0, monotonic()-b_started)
