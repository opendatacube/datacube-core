# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from enum import Enum
from uuid import UUID
from typing import Mapping, Optional, Sequence


class LineageDirection(Enum):
    """
    Enumeration specifying the direction sense of a LineageTree (source-ward or derived-ward)

     - SOURCES indicates a lineage tree that contains the source datasets of the root node
     - DERIVED indicates a lineage tree that contains the derived datasets of the root node
    """
    SOURCES = 1
    DERIVED = 2


@dataclass
class LineageTree:
    """
    A node in a Dataset Lineage tree.

     - direction (LineageDirection): Whether this is a node in a source tree or a derived tree
     - dataset_id (UUID): The dataset id associated with this node
     - children (Optional[Mapping[str, Sequence[LineageTree]]]):
          An optional mapping of lineage nodes of the same direction as this node.
          The keys of the mapping are classifier strings.
          children=None means that there may be children in the database.
          children={} means there are no children in the database.
          children represent source datasets or derived datasets depending on the direction.
    home (Optional[str]):
          The home index associated with this node's dataset.
          Optional. Index drivers may not implement a home table, in which case this value
          will always be None.
    """
    direction: LineageDirection
    dataset_id: UUID
    children: Optional[Mapping[str, Sequence["LineageTree"]]] = None
    home: Optional[str] = None

    @classmethod
    def sources(cls, dsid: UUID, sources: Mapping[str, Sequence[UUID]], home=None) -> "LineageTree":
        return cls(
            direction=LineageDirection.SOURCES,
            dataset_id=dsid,
            children={
                classifier: [
                    cls(direction=LineageDirection.SOURCES, dataset_id=src, home=home)
                    for src in srcs
                ]
                for classifier, srcs in sources.items()
            },
        )
