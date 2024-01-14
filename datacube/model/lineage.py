# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from enum import Enum
from uuid import UUID
from typing import Mapping, Optional, Sequence, MutableMapping, Set, Tuple, Iterable, Any


class LineageDirection(Enum):
    """
    Enumeration specifying the direction sense of a LineageTree (source-ward or derived-ward)

     - SOURCES indicates a lineage tree that contains the source datasets of the root node
     - DERIVED indicates a lineage tree that contains the derived datasets of the root node
    """
    SOURCES = 1
    DERIVED = 2

    def opposite(self):
        if self == self.SOURCES:
            return self.DERIVED
        else:
            return self.SOURCES

    @property
    def label(self):
        if self == self.SOURCES:
            return "sources"
        else:
            return "derivations"


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

    def __eq__(self, other):
        if not self.children and not other.children:
            children_equal = True
        else:
            children_equal = (self.children == other.children)

        return (
            self.dataset_id == other.dataset_id
            and self.home == other.home
            and children_equal
        )

    def serialise(self, specify_direction_if_empty=True) -> Mapping[str, Any]:
        serial = {"id": str(self.dataset_id)}
        if self.home:
            serial["home"] = self.home
        if self.children:
            serial[self.direction.label] = {
                classifier: [
                    child.serialise(False)
                    for child in children
                ]
                for classifier, children in self.children.items()
            }
        elif specify_direction_if_empty:
            serial[self.direction.label] = {}
        return serial

    @classmethod
    def deserialise(cls, serialised: Mapping[str, Any], direction: Optional[LineageDirection] = None) -> "LineageTree":
        if "id" not in serialised:
            raise ValueError("Serialised Lineage tree node must have an id")
        id_ = UUID(serialised["id"])
        home = serialised.get("home")
        if direction is None:
            if LineageDirection.SOURCES.label in serialised:
                direction = LineageDirection.SOURCES
            elif LineageDirection.DERIVED.label in serialised:
                direction = LineageDirection.DERIVED
            else:
                raise ValueError(f"Ambiguous direction for serialised Lineage Tree at {id_}")
        if direction.opposite().label in serialised:
            raise ValueError(f"Ambiguous direction for serialised Lineage Tree at {id_}")
        if serialised.get(direction.label):
            children = {
                classifier: [
                    LineageTree.deserialise(child_tree, direction)
                    for child_tree in child_trees
                ]
                for classifier, child_trees in serialised[direction.label].items()
            }
        else:
            children = {}
        return LineageTree(dataset_id=id_, home=home, direction=direction, children=children)

    def find_subtree(self, dsid: UUID, _state: Optional[Sequence["LineageTree"]] = None) -> Optional["LineageTree"]:
        """
        Finds subtree with root at dsid, if there is one.

        Immediately retunrs the first match found with not-None children, but keeps track of first
        children=None match to return if no not-None matches.


        :param dsid: The desired dataset id
        :param _state: For shared state through recursion
        :return: None, or the subtree with root at dsid.
        """
        root_call = _state is None
        if root_call:
            _state = list()
        # root node will always be first match.
        if dsid == self.dataset_id:
            if self.children is not None:
                # Best match - shortcut
                return self
            if not _state:
                # Partial match - cache if first
                _state.append(self)
        # Recurse
        if self.children is not None:
            for classifier, children in self.children.items():
                for child in children:
                    subtree = child.find_subtree(dsid, _state=_state)
                    if subtree:
                        # Catch and return shortcut best match
                        return subtree
        if root_call and _state:
            # Cached first partial match
            return _state[0]
        # No match
        return None

    @classmethod
    def from_eo3_doc(cls, doc: Mapping[str, Any],
                     home=None,
                     home_derived=None) -> "LineageTree":
        """
        Generate a shallow (depth=1) LineageTree from an EO3 dataset document

        :param dsid: The (derived) dataset id
        :param sources: A dictionary of classifiers to list of source IDs
        :param direction: Tree direction (default SOURCEwards, as per an EO3 dataset)
        :param home: Home database for source datasets (defaults to None).
        :param home_derived: Home database for the derived dataset (defaults to None).
        :return: A depth==1 LineageTree

        :param doc_in:
        :return:
        """
        lineage = doc.get("lineage", {})
        return cls.from_data(doc["id"], lineage, home=home, home_derived=home_derived)

    @classmethod
    def from_data(cls, dsid: UUID,
                  sources: Optional[Mapping[str, Sequence[UUID]]] = None,
                  direction: LineageDirection = LineageDirection.SOURCES,
                  home=None,
                  home_derived=None) -> "LineageTree":
        """
        Generate a shallow (depth=1) LineageTree from the sort of data found in an EO3 dataset

        :param dsid: The (derived) dataset id
        :param sources: A dictionary of classifiers to list of source IDs
        :param direction: Tree direction (default SOURCEwards, as per an EO3 dataset)
        :param home: Home database for source datasets (defaults to None).
        :param home_derived: Home database for the derived dataset (defaults to None).
        :return: A depth==1 LineageTree
        """
        if sources is None:
            children = None
        else:
            children = {
                classifier: [
                    cls(direction=direction, dataset_id=der, home=home)
                    for der in ders
                ]
                for classifier, ders in sources.items()
            }
        return cls(
            direction=direction,
            dataset_id=dsid,
            children=children,
            home=home_derived
        )

    def child_datasets(self) -> Set[UUID]:
        child_dsids = set()
        if self.children is None:
            return child_dsids
        for classifier, children in self.children.items():
            for child in children:
                subchildren = child.child_datasets()
                subchildren.add(child.dataset_id)
                if self.dataset_id in subchildren:
                    raise InconsistentLineageException("LineageTrees must be acyclic")
                child_dsids.update(subchildren)
        return child_dsids


class InconsistentLineageException(Exception):
    """
    Raised when a method would result in an inconsistent/invalid LineageTree or LineageRelations collection.
    """


@dataclass(frozen=True)
class LineageIDPair:
    """
    LineagePair

    A lineage relationship between a source UUID, a derived UUID
    (no classifier)
    """
    derived_id: UUID
    source_id: UUID


@dataclass(frozen=True)
class LineageRelation:
    """
    LineageRelation

    A lineage relationship between a source UUID, a derived UUID, with a classifier string
    """
    classifier: str
    source_id: UUID
    derived_id: UUID

    def ids(self):
        return LineageIDPair(derived_id=self.derived_id, source_id=self.source_id)


class LineageRelations:
    """
    An indexed collection of LineageRelations.

    For converting between iterables of LineageRelations and LineageTrees.
    Enforces all lineage chains are acyclic.
    """
    def __init__(self,
                 tree: Optional[LineageTree] = None,
                 max_depth: int = 0,
                 relations: Optional[Iterable[LineageRelation]] = None,
                 homes: Optional[Mapping[UUID, str]] = None,
                 clone: Optional["LineageRelations"] = None) -> None:
        """
        All arguments are optional.  Default gives an empty LineageRelations, and:

             rels = LineageRelations(tree, max_depth=max_depth, relations=lrels, clone=clone)

        is equivalent to:

             rels = LineageRelations()
             rels.merge_tree(tree, max_depth=max_depth)
             rels.merge(clone)
             for rel in lrels:
                rels.merge_new_lineage_relation(rel)

        :param tree: Initially merge a LineageTree
        :param max_depth: The maximum depth to read the LineageTree.
                          Default/0: no limit.  Not used if tree is None.

        :param clone: Initially clone this other LineageRelations object
        """
        # Internal index of id/home relations
        self._homes: MutableMapping[UUID, str] = {}
        # Tuple[UUID, UUID]'s are always (derived, source)
        # Mapping  (derived, source): classifier - Allow search by source, derived pair.
        self._relations_idx: MutableMapping[LineageIDPair, str] = {}
        # Sequence of the distinct LineageRelation objects this object represents.
        self.relations: Sequence[LineageRelation] = []
        # Mapping source to mapping derived to classifier.  Allow search by source
        self.by_source: MutableMapping[UUID, Mapping[UUID, str]] = {}
        # Mapping source to mapping derived to classifier.  Allow search by derived
        self.by_derived: MutableMapping[UUID] = {}
        # Dataset ids known to this object
        self.dataset_ids: Set[UUID] = set()

        # Merge initial arguments
        if clone is not None:
            self.merge(clone)
        if tree is not None:
            self.merge_tree(tree, max_depth=max_depth)
        if relations is not None:
            for rel in relations:
                self.merge_new_lineage_relation(rel)
        if homes is not None:
            for id_, home in homes.items():
                self.merge_new_home(id_, home)

    def merge_new_home(self, id_: UUID, home: str) -> None:
        """
        Merge a new home relation

        Raises InconsistentLineageException if we already have this id with a different home

        :param id_: The dataet id
        :param home: The home string
        """
        if id_ in self._homes:
            if self._homes[id_] and self._homes[id_] != home:
                raise InconsistentLineageException(f"Tree contains inconsistent homes for dataset {id_}")
        else:
            self._homes[id_] = home

    def _merge_new_relation(self, ids: Tuple[UUID, UUID], classifier: str) -> None:
        """
        Internal convenience wrapper to merge_new_lineage_relation
        """
        self.merge_new_lineage_relation(
            LineageRelation(
                classifier=classifier,
                derived_id=ids.derived_id,
                source_id=ids.source_id))

    def merge_new_lineage_relation(self, rel: LineageRelation) -> None:
        """
        Merge a new LineageRelation object

        Raises InconsistentLineageException if we already have this relation with a different classifier, or
        this relation would result in a cyclic relation.
        """
        ids = rel.ids()
        if ids in self._relations_idx:
            if self._relations_idx[ids] != rel.classifier:
                raise InconsistentLineageException(
                    f"Dataset {ids.derived_id} is derived from {ids.source_id} with inconsistent classifiers."
                )
        else:
            self._relations_idx[ids] = rel.classifier
            self.relations.append(rel)
            if rel.source_id not in self.by_source:
                self.by_source[rel.source_id] = {}
            if rel.derived_id not in self.by_derived:
                self.by_derived[rel.derived_id] = {}
            self.by_source[rel.source_id][rel.derived_id] = rel.classifier
            self.by_derived[rel.derived_id][rel.source_id] = rel.classifier
            # Check for cyclic dependencies:
            new_ids = set([ids.derived_id, ids.source_id])
            if new_ids & self.dataset_ids:
                # We already know about these ids so need to confirm we are still acyclic
                # Extract sourcewards from derived and vice versa for full tree coverage
                self.extract_tree(rel.derived_id, direction=LineageDirection.SOURCES)
                self.extract_tree(rel.source_id, direction=LineageDirection.DERIVED)
            self.dataset_ids.update(new_ids)

    def merge(self, pool: "LineageRelations") -> None:
        """
        Merge in another LineageRelations collection, ensuring it is consistent with this one.

        :param pool: The other LineageRelations object
        """
        for id_, home in pool._homes.items():
            self.merge_new_home(id_, home)
        for ids, classifier in pool._relations_idx.items():
            self._merge_new_relation(ids, classifier)

    def merge_tree(self, tree: LineageTree,
                   nodes: Optional[Mapping[UUID, LineageTree]] = None,
                   max_depth: int = 0) -> None:
        """
        Merge in a LineageTree, ensuring it is consistent with the collection so far.

        Raises InconsistentLineageException if tree contains cyclic depenedencies or inconsistent direction

        :param tree: The LineageTree to merge
        :param parent_node: The parent node (used to mark recursive traversal - should be None on first call)
        :param max_depth: The depth to traverse the tree to.  default/zero = unlimited
        """
        # Check new tree is acyclic within itself
        tree.child_datasets()
        if tree.home is not None:
            self.merge_new_home(tree.dataset_id, tree.home)
        # Determine recursion behaviour
        recurse = True
        next_max_depth = max_depth - 1
        if nodes is None:
            nodes = {}
        if max_depth == 0:
            next_max_depth = 0
        elif max_depth == 1:
            recurse = False
        if tree.children:
            if tree.dataset_id in nodes:
                raise InconsistentLineageException("Duplicate nodes in LineageTree")
            nodes[tree.dataset_id] = tree
        else:
            # tree.children is {} or None (i.e. leaf node of original input tree).
            # Try to extract a reverse-direction tree to check for cyclic dependencies
            self.extract_tree(tree.dataset_id, direction=tree.direction.opposite())
        if tree.children is None:
            return
        # Perform recursion, as determined above
        for classifier, children in tree.children.items():
            for child in children:
                if child.direction != tree.direction:
                    raise InconsistentLineageException("Tree contains both derived and source nodes")
                if tree.direction == LineageDirection.SOURCES:
                    ids = LineageIDPair(derived_id=tree.dataset_id, source_id=child.dataset_id)
                else:
                    ids = LineageIDPair(derived_id=child.dataset_id, source_id=tree.dataset_id)
                self._merge_new_relation(ids, classifier)
                if recurse:
                    self.merge_tree(
                        child,
                        nodes=nodes,
                        max_depth=next_max_depth
                    )
        return

    def relations_diff(self,
                       existing_relations: Optional["LineageRelations"] = None,
                       allow_updates: bool = False) -> Tuple[Mapping[LineageIDPair, str],
                                                             Mapping[LineageIDPair, str],
                                                             Mapping[UUID, str],
                                                             Mapping[UUID, str]]:
        """
        Compare to another LineageRelations object, returning records to be added to or updated in
        the other LinearRelations collection to consistently merge this collection into it.

        Intended to be used by index drivers when adding lineage data to an index.

        Raises InconsistentLineageException if updates are required and allow_updates is False, or if
        merging the two LineageRelations would result in cyclic depenedencies.

        :param existing_relations: The relations currently in an index.
        :param allow_updates: Whether updates to existing records are allowed.
        :return: Tuple containing:
            Relations that need to be added to existing_relations to merge with this collection.
            Relations that need to be updated in existing_relations to merge with this collection.
            Homes that need to be added to existing_relations to merge with this collection.
            Homes that need to up updated in existing_relations to merge with this collection.
        """
        if not existing_relations:
            return (
                self._relations_idx, {},
                self._homes, {}
            )
        relations_to_add = {}
        relations_to_update = {}
        homes_to_add = {}
        homes_to_update = {}

        if not allow_updates:
            # Ensure no inconsistencies
            merged = LineageRelations(clone=self)
            merged.merge(existing_relations)
            # Determine homes and relations to add
            for id_, home in self._homes.items():
                if id_ not in existing_relations._homes:
                    homes_to_add[id_] = home
            for ids, classifier in self._relations_idx.items():
                if ids not in existing_relations._relations_idx:
                    relations_to_add[ids] = classifier
        else:
            # Determine homes to add and update
            for id_, home in self._homes.items():
                if id_ not in existing_relations._homes:
                    homes_to_add[id_] = home
                elif home != existing_relations._homes[id_]:
                    homes_to_update[id_] = home
            # Determine relations to add and update
            for ids, classifier in self._relations_idx.items():
                if ids not in existing_relations._relations_idx:
                    relations_to_add[ids] = classifier
                elif classifier != existing_relations._relations_idx[ids]:
                    relations_to_update[ids] = classifier
        return (
            relations_to_add, relations_to_update,
            homes_to_add, homes_to_update
        )

    def extract_tree(self,
                     root: UUID,
                     direction: LineageDirection = LineageDirection.SOURCES,
                     parents: Optional[Set[UUID]] = None,
                     so_far: Optional[Set[UUID]] = None,
                     ) -> LineageTree:
        """
        Extract a LineageTree from this LineageRelations collection.

        Used to detect cyclic dependencies.

        :param root: The dataset id at the root of the extracted LineageTree
        :param direction: The direction of the extracted tree
        :param parents: Used to detect cyclic dependencies in recursive mode
                        - should be None on initial call.
        :param so_far: Used to detect duplication from diamond dependencies in recursive mode
                       - should be None on initial call.
        :return: the extracted LineageTree.
        """
        # Trees are extracted from the root down, so the leaf-up cycle-detection of tree.child_datasets
        # is insufficient here
        if so_far is None:
            so_far = set()
        if parents is None:
            parents = set()
        if root in parents:
            raise InconsistentLineageException(f"LineageTrees must be acyclic: {root}")
        parents.add(root)
        if root in so_far:
            # Shortcut duplicates for handling diamond-dependencies
            return LineageTree(dataset_id=root, direction=direction, home=self._homes.get(root))
        so_far.add(root)

        children = {}
        if direction == LineageDirection.SOURCES:
            subtrees = self.by_derived.get(root, {})
        else:
            subtrees = self.by_source.get(root, {})
        for dsid, classifier in subtrees.items():
            subtree = self.extract_tree(dsid, direction, set(parents), so_far)
            if classifier in children:
                children[classifier].append(subtree)
            else:
                children[classifier] = [subtree]
        tree = LineageTree(
            dataset_id=root,
            direction=direction,
            children=children,
            home=self._homes.get(root)
        )
        return tree
