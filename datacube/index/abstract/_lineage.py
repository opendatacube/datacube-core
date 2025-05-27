# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from time import monotonic
from uuid import UUID

from typing_extensions import override

from datacube.model import LineageDirection, LineageRelation, LineageTree
from datacube.model.lineage import LineageRelations
from datacube.utils import report_to_user

from ._types import DSID, BatchStatus

_LOG: logging.Logger = logging.getLogger(__name__)


class AbstractLineageResource(ABC):
    """
    Abstract base class for the Lineage portion of an index api.

    All LineageResource implementations should inherit from this base class.

    Note that this is a "new" resource only supported by new index drivers with `supports_external_lineage`
    set to True.  If a driver does NOT support external lineage, it can use extend the NoLineageResource class below,
    which is a minimal implementation of this resource that raises a NotImplementedError for all methods.

    However, any index driver that supports lineage must implement at least the get_all_lineage() and _add_batch()
    methods.
    """

    def __init__(self, index, supports_external_lineage: bool = True) -> None:
        self._index = index
        supports = self._index.supports_external_lineage
        assert supports if supports_external_lineage else not supports

    @abstractmethod
    def get_derived_tree(self, id_: DSID, max_depth: int = 0) -> LineageTree:
        """
        Extract a LineageTree from the index, with:
            - "id" at the root of the tree.
            - "derived" direction (i.e. datasets derived from id, datasets derived from
              datasets derived from id, etc.)
            - maximum depth as requested (default 0 = unlimited depth)

        Tree may be empty (i.e. just the root node) if no lineage for id is stored.

        :param id_: the id of the dataset at the root of the returned tree
        :param max_depth: Maximum recursion depth.  Default/Zero = unlimited depth
        :return: A derived-direction Lineage tree with id at the root.
        """

    @abstractmethod
    def get_source_tree(self, id_: DSID, max_depth: int = 0) -> LineageTree:
        """
        Extract a LineageTree from the index, with:
            - "id" at the root of the tree.
            - "source" direction (i.e. datasets id was derived from, the dataset ids THEY were derived from, etc.)
            - maximum depth as requested (default 0 = unlimited depth)

        Tree may be empty (i.e. just the root node) if no lineage for id is stored.

        :param id_: the id of the dataset at the root of the returned tree
        :param max_depth: Maximum recursion depth.  Default/Zero = unlimited depth
        :return: A source-direction Lineage tree with id at the root.
        """

    @abstractmethod
    def merge(
        self,
        rels: LineageRelations,
        allow_updates: bool = False,
        validate_only: bool = False,
    ) -> None:
        """
        Merge an entire LineageRelations collection into the database.

        :param rels: The LineageRelations collection to merge.
        :param allow_updates: If False and the merging rels would require index updates,
                              then raise an InconsistentLineageException.
        :param validate_only: If True, do not actually merge the LineageRelations, just check for inconsistency.
                              allow_updates and validate_only cannot both be True
        """

    @abstractmethod
    def add(
        self, tree: LineageTree, max_depth: int = 0, allow_updates: bool = False
    ) -> None:
        """
        Add or update a LineageTree into the Index.

        If the provided tree is inconsistent with lineage data already
        recorded in the database, by default a ValueError is raised,
        If replace is True, the provided tree is treated as authoritative
        and the database is updated to match.

        :param tree: The LineageTree to add to the index
        :param max_depth: Maximum recursion depth. Default/Zero = unlimited depth
        :param allow_updates: If False and the tree would require index updates to fully
                              add, then raise an InconsistentLineageException.
        """

    @abstractmethod
    def remove(
        self, id_: DSID, direction: LineageDirection, max_depth: int = 0
    ) -> None:
        """
        Remove lineage information from the Index.

        Removes lineage relation data only. Home values not affected.

        :param id_: The Dataset ID to start removing lineage from.
        :param direction: The direction in which to remove lineage (from id_)
        :param max_depth: The maximum depth to which to remove lineage (0/default = no limit)
        """

    @abstractmethod
    def set_home(self, home: str, *args: DSID, allow_updates: bool = False) -> int:
        """
        Set the home for one or more dataset ids.

        :param home: The home string
        :param args: One or more dataset ids
        :param allow_updates: Allow datasets with existing homes to be updated.
        :returns: The number of records affected.  Between zero and len(args).
        """

    @abstractmethod
    def clear_home(self, *args: DSID, home: str | None = None) -> int:
        """
        Clear the home for one or more dataset ids, or all dataset ids that currently have
        a particular home value.

        :param args: One or more dataset ids
        :param home: The home string.  Supply home or args - not both.
        :returns: The number of home records deleted. Usually len(args).
        """

    @abstractmethod
    def get_homes(self, *args: DSID) -> Mapping[UUID, str]:
        """
        Obtain a dictionary mapping UUIDs to home strings for the passed in DSIDs.

        If a passed in DSID does not have a home set in the database, it will not
        be included in the returned mapping.  i.e. a database index with no homes
        recorded will always return an empty mapping.

        :param args: One or more dataset ids
        :return: Mapping of dataset ids to home strings.
        """

    @abstractmethod
    def get_all_lineage(self, batch_size: int = 1000) -> Iterable[LineageRelation]:
        """
        Perform a batch-read of all lineage relations (as used by index clone operation)
        and return as an iterable stream of LineageRelation objects.

        API Note: This API method is not finalised and may be subject to change.

        :param batch_size: The number of records to read from the database at a time.
        :return: An iterable stream of LineageRelation objects.
        """

    @abstractmethod
    def _add_batch(self, batch_rels: Iterable[LineageRelation]) -> BatchStatus:
        """
        Add a single "batch" of LineageRelation objects.

        No default implementation is provided

        API Note: This API method is not finalised and may be subject to change.

        :param batch_rels: An iterable of one batch's worth of LineageRelation objects to add
        :return: BatchStatus named tuple, with `safe` set to None.
        """

    def bulk_add(
        self, relations: Iterable[LineageRelation], batch_size: int = 1000
    ) -> BatchStatus:
        """
        Add a group of LineageRelation objects in bulk.

        API Note: This API method is not finalised and may be subject to change.

        :param relations: An Iterable of LineageRelation objects (i.e. as returned by get_all_lineage)
        :param batch_size: Number of lineage relations to add per batch (default 1000)
        :return: BatchStatus named tuple, with `safe` set to None.
        """

        def increment_progress() -> None:
            report_to_user(".", progress_indicator=True)

        n_batches = 0
        n_in_batch = 0
        added = 0
        skipped = 0
        batch = []
        job_started = monotonic()
        for rel in relations:
            batch.append(rel)
            n_in_batch += 1
            if n_in_batch >= batch_size:
                batch_result = self._add_batch(batch)
                _LOG.info(
                    "Batch %d/%d datasets added in %.2fs: (%.2fdatasets/min)",
                    batch_result.completed,
                    n_in_batch,
                    batch_result.seconds_elapsed,
                    batch_result.completed * 60 / batch_result.seconds_elapsed,
                )
                added += batch_result.completed
                skipped += batch_result.skipped
                batch = []
                n_in_batch = 0
                n_batches += 1
                increment_progress()
        if n_in_batch > 0:
            batch_result = self._add_batch(batch)
            added += batch_result.completed
            skipped += batch_result.skipped
            increment_progress()

        return BatchStatus(added, skipped, monotonic() - job_started)


class NoLineageResource(AbstractLineageResource):
    """
    Minimal implementation of AbstractLineageResource that raises "not implemented"
       for all methods.

    Index drivers that do not support lineage at all may use this implementation as is.

    Index drivers that support legacy lineage should extend this implementation and provide
    implementations of the get_all_lineage() and _add_batch() methods.
    """

    def __init__(self, index) -> None:
        super().__init__(index, supports_external_lineage=False)

    @override
    def get_derived_tree(self, id_: DSID, max_depth: int = 0) -> LineageTree:
        raise NotImplementedError()

    @override
    def get_source_tree(self, id_: DSID, max_depth: int = 0) -> LineageTree:
        raise NotImplementedError()

    @override
    def add(
        self, tree: LineageTree, max_depth: int = 0, allow_updates: bool = False
    ) -> None:
        raise NotImplementedError()

    @override
    def merge(
        self,
        rels: LineageRelations,
        allow_updates: bool = False,
        validate_only: bool = False,
    ) -> None:
        raise NotImplementedError()

    @override
    def remove(
        self, id_: DSID, direction: LineageDirection, max_depth: int = 0
    ) -> None:
        raise NotImplementedError()

    @override
    def set_home(self, home: str, *args: DSID, allow_updates: bool = False) -> int:
        raise NotImplementedError()

    @override
    def clear_home(self, *args: DSID, home: str | None = None) -> int:
        raise NotImplementedError()

    @override
    def get_homes(self, *args: DSID) -> Mapping[UUID, str]:
        return {}

    @override
    def get_all_lineage(self, batch_size: int = 1000) -> Iterable[LineageRelation]:
        raise NotImplementedError()

    @override
    def _add_batch(self, batch_rels: Iterable[LineageRelation]) -> BatchStatus:
        raise NotImplementedError()
