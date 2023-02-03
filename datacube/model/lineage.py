# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from enum import Enum
from uuid import UUID
from typing import Mapping, Optional, Sequence, MutableMapping, Set, Tuple

from datacube.utils import cached_property



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

    @classmethod
    def deriveds(cls, dsid: UUID, sources: Mapping[str, Sequence[UUID]], home=None) -> "LineageTree":
        return cls(
            direction=LineageDirection.DERIVED,
            dataset_id=dsid,
            children={
                classifier: [
                    cls(direction=LineageDirection.DERIVED, dataset_id=der, home=home)
                    for der in ders
                ]
                for classifier, ders in sources.items()
            },
        )


class InconsistentLineageException(Exception):
    pass


@dataclass
class LineageRelation:
    """
    LineageRelation
    """
    classifier: str
    source_id: UUID
    derived_id: UUID


class LineageRelations:
    def __init__(self,
                 tree: Optional[LineageTree] = None,
                 max_depth: int = 0,
                 merge_with: Optional["LineageRelations"] = None) -> None:
        self._homes: MutableMapping[UUID, str] = {}
        self._relations_idx: MutableMapping[Tuple[UUID, UUID], str] = {}
        self._trees_idx: MutableMapping[Tuple[UUID, UUID], Sequence[LineageTree]] = {}
        self.relations: Sequence[LineageRelation] = []
        self.source_ids: Set[UUID] = set()
        self.derived_ids: Set[UUID] = set()
        if merge_with is not None:
            self.merge(merge_with)
        if tree is not None:
            self.merge_tree(tree, max_depth=max_depth)

    def _merge_new_home(self, id_: UUID, home: str) -> None:
        if id_ in self._homes:
            if self._homes[id_] and self._homes[id_] != home:
                raise InconsistentLineageException(f"Tree contains inconsistent homes for dataset {id_}")
        else:
            self._homes[id_] = home

    def _merge_new_relation(self, ids: Tuple[UUID, UUID], classifier: str) -> None:
        self._merge_new_lineage_relation(LineageRelation(classifier=classifier, source_id=ids[0], derived_id=ids[1]))

    def merge_new_lineage_relation(self, rel: LineageRelation) -> None:
        ids = (rel.source_id, rel.derived_id)
        if ids in self._relations_idx:
            if self._relations_idx[ids] != rel.classifier:
                raise InconsistentLineageException(
                    f"Dataset {ids[0]} depends on {ids[1]} with inconsistent classifiers."
                )
        else:
            self._relations_idx[ids] = rel.classifier
            self.relations.append(rel)
            self.source_ids.add(rel.source_id)
            self.derived_ids.add(rel.derived_id)

    def _merge_new_node(self, ids: Tuple[UUID, UUID], node):
        if ids in self._trees_idx:
            got_grandchildren = bool(node.children)
            for prev_tree in self._trees_idx[ids]:
                if prev_tree.children and got_grandchildren:
                    raise InconsistentLineageException(
                        f"Self-reference or duplicate subtrees detected: risk of infinite recursion."
                    )
            self._trees_idx[ids].append(node)
        else:
            self._trees_idx[ids] = [node]

    def merge(self, pool: "LineageRelations") -> None:
        for id_, home in pool._homes.items():
            self._merge_new_home(id_, home)
        for ids, classifier in pool._relations_idx.items():
            self._merge_new_relation(ids, classifier)
        for ids, node in pool._trees_idx.items():
            self._merge_new_node(ids, node)

    def merge_tree(self, tree: LineageTree,
                                     parent_id: Optional[UUID] = None,
                                     max_depth: int = 0) -> None:
        self._merge_new_home(tree.dataset_id.home)
        recurse = True
        next_max_depth = max_depth - 1
        if not parent_id:
            next_max_depth = max_depth
        elif max_depth == 0:
            next_max_depth = 0
        elif max_depth == 1:
            recurse = False
        for classifier, children in tree.children.items():
            for child in children:
                if child.direction != tree.direction:
                    raise InconsistentLineageException("Tree contains both derived and source nodes")
                if parent_id:
                    if tree.direction == LineageDirection.SOURCES:
                        ids = [parent_id, child.dataset_id]
                    else:
                        ids = [child.dataset_id, parent_id]
                    self._merge_new_relation(ids, classifier)
                    self._merge_new_node(ids, child)
                if recurse:
                    self.merge_tree(
                        child,
                        parent_id = tree.dataset_id,
                        max_depth=next_max_depth
                    )
        return

    def relations_diff(self,
                  existing_relations: Optional["LineageRelations"] = None,
                  allow_updates: bool = False
                 ) -> Tuple[
                      Mapping[Tuple[UUID, UUID], str],
                      Mapping[Tuple[UUID, UUID], str],
                      Mapping[UUID, str],
                      Mapping[UUID, str]
                      ]:
        if not existing_relations:
            return (
                self.relations, {},
                self._homes, {}
            )
        relations_to_add = []
        relations_to_update = []
        homes_to_add = {}
        homes_to_update = {}

        if not allow_updates:
            # Ensure no inconsistencies
            merged = LineageRelations(merge_with=self)
            merged.merge(existing_relations)
            for id_, home in self._homes:
                if id_ not in existing_relations._homes:
                    homes_to_add[id_] = home
            for ids, classifier in self._relations_idx:
                if ids not in existing_relations._relations_idx:
                    relations_to_add[ids] = classifier
        else:
            for id_, home in self._homes:
                if id_ not in existing_relations._homes:
                    homes_to_add[id_] = home
                elif home != existing_relations._homes[id_]:
                    homes_to_update[id_] = home
            for ids, classifier in self._relations_idx:
                if ids not in existing_relations._relations_idx:
                    relations_to_add[ids] = classifier
                elif classifier != existing_relations._relations_idx[ids]:
                    relations_to_update[ids] = classifier
        return (
            relations_to_add, relations_to_update,
            homes_to_add, homes_to_update
        )
