# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

# We often have one-arg-per column, so these checks aren't so useful.
# pylint: disable=too-many-arguments,too-many-public-methods,too-many-lines

# SQLAlchemy queries require "column == None", not "column is None" due to operator overloading:
# pylint: disable=singleton-comparison

"""
Persistence API implementation for postgres.
"""

import datetime
import logging
import uuid  # noqa: F401
from collections.abc import Iterable, Iterator
from typing import Any
from typing import cast as type_cast

from sqlalchemy import (
    Label,
    Select,
    String,
    and_,
    bindparam,
    cast,
    column,
    delete,
    distinct,
    func,
    literal,
    or_,
    select,
    text,
    values,
)
from sqlalchemy.dialects.postgresql import INTERVAL, JSONB, UUID, insert
from sqlalchemy.engine import Row
from sqlalchemy.exc import IntegrityError
from typing_extensions import override

from datacube.index.abstract import DSID
from datacube.index.exceptions import MissingRecordError
from datacube.index.fields import Expression, Field, OrExpression
from datacube.model import Range
from datacube.utils.uris import split_uri

from . import _core
from . import _dynamic as dynamic
from ._fields import (  # noqa: F401
    DateDocField,
    DateRangeDocField,
    NativeField,
    PgExpression,
    PgField,
    SimpleDocField,
    parse_fields,
)
from ._schema import DATASET, DATASET_LOCATION, DATASET_SOURCE, METADATA_TYPE, PRODUCT
from .sql import escape_pg_identifier

PGCODE_FOREIGN_KEY_VIOLATION = "23503"
_LOG: logging.Logger = logging.getLogger(__name__)


def _dataset_uri_field(table):
    return table.c.uri_scheme + ":" + table.c.uri_body


# Fields for selecting dataset with uris
# Need to alias the table, as queries may join the location table for filtering.
SELECTED_DATASET_LOCATION = DATASET_LOCATION.alias("selected_dataset_location")
# All active URIs, from newest to oldest
_ALL_ACTIVE_URIS = func.array(
    select(_dataset_uri_field(SELECTED_DATASET_LOCATION))
    .where(
        and_(
            SELECTED_DATASET_LOCATION.c.dataset_ref == DATASET.c.id,
            SELECTED_DATASET_LOCATION.c.archived == None,
        )
    )
    .order_by(
        SELECTED_DATASET_LOCATION.c.added.desc(), SELECTED_DATASET_LOCATION.c.id.desc()
    )
    .label("uris")
).label("uris")

_DATASET_SELECT_FIELDS = (
    DATASET,
    _ALL_ACTIVE_URIS,
)

_DATASET_BULK_SELECT_FIELDS = (
    PRODUCT.c.name,
    DATASET.c.metadata,
    _ALL_ACTIVE_URIS,
)


def _base_known_fields() -> dict:
    fields = get_native_fields().copy()
    fields["archived"] = NativeField("archived", "Archived date", DATASET.c.archived)
    fields["uris"] = NativeField(
        "uris",
        "all active uris",
        _ALL_ACTIVE_URIS,
    )
    return fields


def get_native_fields() -> dict[str, NativeField]:
    # Native fields (hard-coded into the schema)
    fields = {
        "id": NativeField("id", "Dataset UUID", DATASET.c.id),
        "indexed_time": NativeField(
            "indexed_time", "When dataset was indexed", DATASET.c.added
        ),
        "indexed_by": NativeField(
            "indexed_by", "User who indexed the dataset", DATASET.c.added_by
        ),
        "product": NativeField("product", "Product name", PRODUCT.c.name),
        "product_id": NativeField(
            "product_id", "ID of a product", DATASET.c.dataset_type_ref
        ),
        "metadata_type": NativeField(
            "metadata_type", "Metadata type name of dataset", METADATA_TYPE.c.name
        ),
        "metadata_type_id": NativeField(
            "metadata_type_id", "ID of a metadata type", DATASET.c.metadata_type_ref
        ),
        "metadata_doc": NativeField(
            "metadata_doc", "Full metadata document", DATASET.c.metadata
        ),
        # Fields that can affect row selection
        # Note that this field is a single uri: selecting it will result in one-result per uri.
        # (ie. duplicate datasets if multiple uris, no dataset if no uris)
        "uri": NativeField(
            "uri",
            "Dataset URI",
            DATASET_LOCATION.c.uri_body,
            alchemy_expression=_dataset_uri_field(DATASET_LOCATION),
            affects_row_selection=True,
        ),
    }
    return fields


def get_dataset_fields(metadata_type_definition):
    dataset_section = metadata_type_definition["dataset"]

    fields = get_native_fields()
    # "Fixed fields" (not dynamic: defined in metadata type schema)
    fields.update(
        {
            "creation_time": DateDocField(
                "creation_time",
                "Time when dataset was created (processed)",
                DATASET.c.metadata,
                False,
                offset=dataset_section.get("creation_dt") or ["creation_dt"],
            ),
            "format": SimpleDocField(
                "format",
                "File format (GeoTiff, NetCDF)",
                DATASET.c.metadata,
                False,
                offset=dataset_section.get("format") or ["format", "name"],
            ),
            "label": SimpleDocField(
                "label",
                "Label",
                DATASET.c.metadata,
                False,
                offset=dataset_section.get("label") or ["label"],
            ),
        }
    )

    # noinspection PyTypeChecker
    fields.update(parse_fields(dataset_section["search_fields"], DATASET.c.metadata))
    return fields


class PostgresDbAPI:
    def __init__(self, connection) -> None:
        self._connection = connection
        self._sqla_txn = None

    @property
    def in_transaction(self):
        return self._connection.in_transaction()

    def begin(self) -> None:
        self._connection.execution_options(isolation_level="REPEATABLE READ")
        self._sqla_txn = self._connection.begin()

    def _end_transaction(self) -> None:
        self._sqla_txn = None
        self._connection.execution_options(isolation_level="AUTOCOMMIT")

    def commit(self) -> None:
        self._sqla_txn.commit()  # type: ignore[attr-defined]
        self._end_transaction()

    def rollback(self) -> None:
        self._sqla_txn.rollback()  # type: ignore[attr-defined]
        self._end_transaction()

    def execute(self, command):
        return self._connection.execute(command)

    def insert_dataset(self, metadata_doc, dataset_id, product_id):
        """
        Insert dataset if not already indexed.
        :return: whether it was inserted
        """
        dataset_type_ref = bindparam("dataset_type_ref")
        ret = self._connection.execute(
            insert(DATASET)
            .from_select(
                ["id", "dataset_type_ref", "metadata_type_ref", "metadata"],
                select(
                    bindparam("id"),
                    dataset_type_ref,
                    select(PRODUCT.c.metadata_type_ref)
                    .where(PRODUCT.c.id == dataset_type_ref)
                    .label("metadata_type_ref"),
                    bindparam("metadata", type_=JSONB),
                ),
            )
            .on_conflict_do_nothing(index_elements=["id"]),
            {
                "id": dataset_id,
                "dataset_type_ref": product_id,
                "metadata": metadata_doc,
            },
        )
        return ret.rowcount > 0

    def insert_dataset_bulk(self, values) -> tuple:
        requested = len(values)
        res = self._connection.execute(insert(DATASET), values)
        return res.rowcount, requested - res.rowcount

    def update_dataset(self, metadata_doc, dataset_id, product_id) -> bool:
        """
        Update dataset
        """
        res = self._connection.execute(
            DATASET.update()
            .returning(DATASET.c.id)
            .where(
                and_(
                    DATASET.c.id == dataset_id, DATASET.c.dataset_type_ref == product_id
                )
            )
            .values(metadata=metadata_doc)
        )
        return res.rowcount > 0

    def insert_dataset_location(self, dataset_id: DSID, uri: str) -> bool:
        """
        Add a location to a dataset if it is not already recorded.

        Returns True if success, False if this location already existed
        """
        scheme, body = split_uri(uri)

        r = self._connection.execute(
            insert(DATASET_LOCATION).on_conflict_do_nothing(
                index_elements=["uri_scheme", "uri_body", "dataset_ref"]
            ),
            {
                "dataset_ref": dataset_id,
                "uri_scheme": scheme,
                "uri_body": body,
            },
        )

        return r.rowcount > 0

    def insert_dataset_location_bulk(self, values) -> tuple:
        requested = len(values)
        res = self._connection.execute(insert(DATASET_LOCATION), values)
        return res.rowcount, requested - res.rowcount

    def contains_dataset(self, dataset_id) -> bool:
        return bool(
            self._connection.execute(
                select(DATASET.c.id).where(DATASET.c.id == dataset_id)
            ).fetchone()
        )

    def datasets_intersection(self, dataset_ids) -> list:
        """Compute set intersection: db_dataset_ids & dataset_ids"""
        return [
            r[0]
            for r in self._connection.execute(
                select(DATASET.c.id).where(DATASET.c.id.in_(dataset_ids))
            ).fetchall()
        ]

    def get_datasets_for_location(self, uri, mode: str | None = None):
        scheme, body = split_uri(uri)

        if mode is None:
            mode = "exact" if body.count("#") > 0 else "prefix"

        if mode == "exact":
            body_query = DATASET_LOCATION.c.uri_body == body
        elif mode == "prefix":
            body_query = DATASET_LOCATION.c.uri_body.startswith(body)
        else:
            raise ValueError(f"Unsupported query mode {mode}")

        return self._connection.execute(
            select(*_DATASET_SELECT_FIELDS)
            .select_from(DATASET_LOCATION.join(DATASET))
            .where(and_(DATASET_LOCATION.c.uri_scheme == scheme, body_query))
        ).fetchall()

    def all_dataset_ids(self, archived: bool | None = False):
        query = select(DATASET.c.id).select_from(DATASET)
        if archived:
            query = query.where(DATASET.c.archived.is_not(None))
        elif archived is not None:
            query = query.where(DATASET.c.archived.is_(None))
        return self._connection.execute(query).fetchall()

    def insert_dataset_source(self, classifier, dataset_id, source_dataset_id):
        try:
            r = self._connection.execute(
                insert(DATASET_SOURCE).on_conflict_do_nothing(
                    index_elements=["classifier", "dataset_ref"]
                ),
                {
                    "classifier": classifier,
                    "dataset_ref": dataset_id,
                    "source_dataset_ref": source_dataset_id,
                },
            )
            return r.rowcount > 0
        except IntegrityError as e:
            if e.orig.pgcode == PGCODE_FOREIGN_KEY_VIOLATION:
                raise MissingRecordError(
                    "Referenced source dataset doesn't exist"
                ) from None
            raise

    def archive_dataset(self, dataset_id) -> None:
        self._connection.execute(
            DATASET.update()
            .where(DATASET.c.id == dataset_id)
            .where(DATASET.c.archived == None)
            .values(archived=func.now())
        )

    def restore_dataset(self, dataset_id) -> None:
        self._connection.execute(
            DATASET.update().where(DATASET.c.id == dataset_id).values(archived=None)
        )

    def delete_dataset(self, dataset_id) -> None:
        self._connection.execute(
            DATASET_LOCATION.delete().where(
                DATASET_LOCATION.c.dataset_ref == dataset_id
            )
        )
        self._connection.execute(
            DATASET_SOURCE.delete().where(DATASET_SOURCE.c.dataset_ref == dataset_id)
        )
        self._connection.execute(DATASET.delete().where(DATASET.c.id == dataset_id))

    def get_dataset(self, dataset_id):
        return self._connection.execute(
            select(*_DATASET_SELECT_FIELDS).where(DATASET.c.id == dataset_id)
        ).first()

    def get_datasets(self, dataset_ids):
        return self._connection.execute(
            select(*_DATASET_SELECT_FIELDS).where(DATASET.c.id.in_(dataset_ids))
        ).fetchall()

    def get_derived_datasets(self, dataset_id):
        return self._connection.execute(
            select(*_DATASET_SELECT_FIELDS)
            .select_from(
                DATASET.join(
                    DATASET_SOURCE, DATASET.c.id == DATASET_SOURCE.c.dataset_ref
                )
            )
            .where(DATASET_SOURCE.c.source_dataset_ref == dataset_id)
        ).fetchall()

    def get_dataset_sources(self, dataset_id):
        # recursively build the list of (dataset_ref, source_dataset_ref) pairs starting from dataset_id
        # include (dataset_ref, NULL) [hence the left join]
        sources = (
            select(
                DATASET.c.id.label("dataset_ref"),
                DATASET_SOURCE.c.source_dataset_ref,
                DATASET_SOURCE.c.classifier,
            )
            .select_from(
                DATASET.join(
                    DATASET_SOURCE,
                    DATASET.c.id == DATASET_SOURCE.c.dataset_ref,
                    isouter=True,
                )
            )
            .where(DATASET.c.id == dataset_id)
            .cte(name="sources", recursive=True)
        )

        sources = sources.union_all(
            select(
                sources.c.source_dataset_ref.label("dataset_ref"),
                DATASET_SOURCE.c.source_dataset_ref,
                DATASET_SOURCE.c.classifier,
            )
            .select_from(
                sources.join(
                    DATASET_SOURCE,
                    sources.c.source_dataset_ref == DATASET_SOURCE.c.dataset_ref,
                    isouter=True,
                )
            )
            .where(sources.c.source_dataset_ref != None)
        )

        # turn the list of pairs into adjacency list (dataset_ref, [source_dataset_ref, ...])
        # some source_dataset_ref's will be NULL
        aggd = (
            select(
                sources.c.dataset_ref,
                func.array_agg(sources.c.source_dataset_ref).label("sources"),
                func.array_agg(sources.c.classifier).label("classes"),
            )
            .group_by(sources.c.dataset_ref)
            .alias("aggd")
        )

        # join the adjacency list with datasets table
        select_fields = _DATASET_SELECT_FIELDS + (aggd.c.sources, aggd.c.classes)
        query = select(*select_fields).select_from(
            aggd.join(DATASET, DATASET.c.id == aggd.c.dataset_ref)
        )

        return self._connection.execute(query).fetchall()

    def search_datasets_by_metadata(
        self, metadata: dict, archived: bool | None = False
    ) -> dict:
        """
        Find any datasets that have the given metadata.
        """
        # Find any storage types whose 'dataset_metadata' document is a subset of the metadata.
        where_clause = DATASET.c.metadata.contains(metadata)
        if archived:
            where_clause = and_(where_clause, DATASET.c.archived.is_not(None))
        elif archived is not None:
            where_clause = and_(where_clause, DATASET.c.archived.is_(None))
        query = select(*_DATASET_SELECT_FIELDS).where(where_clause)
        return self._connection.execute(query).fetchall()

    def search_products_by_metadata(self, metadata: dict) -> dict:
        """
        Find any products that have the given metadata.
        """
        # Find any products types whose metadata document contains the passed in metadata
        return self._connection.execute(
            PRODUCT.select().where(PRODUCT.c.metadata.contains(metadata))
        ).fetchall()

    @staticmethod
    def _alchemify_expressions(expressions) -> list:
        def raw_expr(expression):
            if isinstance(expression, OrExpression):
                return or_(raw_expr(expr) for expr in expression.exprs)
            return expression.alchemy_expression

        return [raw_expr(expression) for expression in expressions]

    @staticmethod
    def search_datasets_query(
        expressions: Iterable[Expression],
        source_exprs: Iterable[Expression] | None = None,
        select_fields: Iterable[PgField] | None = None,
        with_source_ids: bool = False,
        limit: int | None = None,
        archived: bool | None = False,
        order_by=None,
    ) -> Select:
        if select_fields:
            # Expand select fields, inserting placeholder columns selections for fields that aren't defined for
            # this product query.
            select_columns = tuple(
                f.alchemy_expression.label(f.name) if f is not None else None
                for f in select_fields
            )
            known_fields = _base_known_fields() | {f.name: f for f in select_fields}
        else:
            select_columns = _DATASET_SELECT_FIELDS
            known_fields = _base_known_fields()

        def _ob_exprs(o):
            if isinstance(o, str):
                if known_fields.get(o.lower()) is not None:
                    return known_fields[o.lower()].alchemy_expression
                raise ValueError(f"Cannot order by unknown field {o}")
            elif isinstance(o, PgField):
                return o.alchemy_expression
            else:
                # assume func, clause, or other expression, and leave as-is
                return o

        if order_by is not None:
            order_by = [_ob_exprs(o) for o in order_by]
        else:
            order_by = []

        if with_source_ids:
            # Include the IDs of source datasets
            select_columns += (
                select(func.array_agg(DATASET_SOURCE.c.source_dataset_ref))
                .select_from(DATASET_SOURCE)
                .where(DATASET_SOURCE.c.dataset_ref == DATASET.c.id)
                .group_by(DATASET_SOURCE.c.dataset_ref)
                .label("dataset_refs"),
            )

        raw_expressions = PostgresDbAPI._alchemify_expressions(expressions)
        from_expression = PostgresDbAPI._from_expression(
            DATASET, expressions, select_fields
        )
        if archived:
            # True: Archived datasets only:
            where_expr = and_(DATASET.c.archived.is_not(None), *raw_expressions)
        elif archived is not None:
            # False/default:  Active datasets only:
            where_expr = and_(DATASET.c.archived.is_(None), *raw_expressions)
        else:
            # None: both active and archived datasets
            where_expr = and_(*raw_expressions)

        if not source_exprs:
            return (
                select(*select_columns)
                .select_from(from_expression)
                .where(where_expr)
                .order_by(*order_by)
                .limit(limit)
            )
        select_fields = select_columns + (
            DATASET_SOURCE.c.source_dataset_ref,
            literal(1).label("distance"),
            DATASET_SOURCE.c.classifier.label("path"),
        )
        base_query = (
            select(*select_fields)
            .select_from(
                from_expression.join(
                    DATASET_SOURCE, DATASET.c.id == DATASET_SOURCE.c.dataset_ref
                )
            )
            .where(where_expr)
        ).cte(name="base_query", recursive=True)

        rq_select_cols = [
            col
            for col in base_query.columns
            if col.name not in ["source_dataset_ref", "distance", "path"]
        ] + [
            DATASET_SOURCE.c.source_dataset_ref,
            (base_query.c.distance + 1).label("distance"),
            (base_query.c.path + "." + DATASET_SOURCE.c.classifier).label("path"),
        ]
        recursive_query = base_query.union_all(
            select(*rq_select_cols).select_from(
                base_query.join(
                    DATASET_SOURCE,
                    base_query.c.source_dataset_ref == DATASET_SOURCE.c.dataset_ref,
                )
            )
        )
        if archived:
            where_expr = and_(
                DATASET.c.archived.is_not(None),
                *PostgresDbAPI._alchemify_expressions(source_exprs),
            )
        elif archived is not None:
            where_expr = and_(
                DATASET.c.archived.is_(None),
                *PostgresDbAPI._alchemify_expressions(source_exprs),
            )
        else:
            where_expr = and_(*PostgresDbAPI._alchemify_expressions(source_exprs))

        return (
            select(
                distinct(recursive_query.c.id),
                *[
                    col
                    for col in recursive_query.columns
                    if col.name not in ["id", "source_dataset_ref", "distance", "path"]
                ],
            )
            .select_from(
                recursive_query.join(
                    DATASET, DATASET.c.id == recursive_query.c.source_dataset_ref
                )
            )
            .where(where_expr)
            .order_by(*order_by)
            .limit(limit)
        )

    def search_datasets(
        self,
        expressions: Iterable[Expression],
        source_exprs: Iterable[Expression] | None = None,
        select_fields: Iterable[PgField] | None = None,
        with_source_ids: bool = False,
        limit: int | None = None,
        archived: bool | None = False,
        order_by=None,
    ):
        select_query = self.search_datasets_query(
            expressions,
            source_exprs,
            select_fields,
            with_source_ids,
            limit,
            archived=archived,
            order_by=order_by,
        )
        return self._connection.execute(select_query)

    def bulk_simple_dataset_search(self, products=None, batch_size: int = 0) -> list:
        """
        Perform bulk database reads (e.g. for index cloning)

        :param products: Optional iterable of product names.  Only fetch nominated products.
        :param batch_size: Number of streamed rows to fetch from database at once.
                           Defaults to zero, which means no streaming.
                           Note streaming is only supported inside a transaction.
        :return: Iterable of tuples of:
                 * Product name
                 * Dataset metadata document
                 * array of uris
        """
        if batch_size > 0 and not self.in_transaction:
            raise ValueError("Postgresql bulk reads must occur within a transaction.")
        if products:
            query = (
                select(PRODUCT.c.id)
                .select_from(PRODUCT)
                .where(PRODUCT.c.name.in_(products))
            )
            products = [row[0] for row in self._connection.execute(query)]
            if not products:
                return []
        else:
            products = None
        query = (
            select(*_DATASET_BULK_SELECT_FIELDS)
            .select_from(DATASET)
            .join(PRODUCT)
            .where(DATASET.c.archived == None)
        )
        if products:
            query = query.where(DATASET.c.dataset_type_ref.in_(products))
        return self._connection.execution_options(
            stream_results=True, yield_per=batch_size
        ).execute(query)

    def get_all_lineage(self, batch_size: int):
        """
        Stream all lineage data in bulk (e.g. for index cloning)

        :param batch_size: The number of lineage records to return at once.
        :return: Streamable SQLAlchemy result object.
        """
        if batch_size > 0 and not self.in_transaction:
            raise ValueError("Postgresql bulk reads must occur within a transaction.")
        query = select(
            DATASET_SOURCE.c.dataset_ref,
            DATASET_SOURCE.c.classifier,
            DATASET_SOURCE.c.source_dataset_ref,
        )
        return self._connection.execution_options(
            stream_results=True, yield_per=batch_size
        ).execute(query)

    def insert_lineage_bulk(self, vals) -> tuple:
        """
        Insert bulk lineage records (e.g. for index cloning)

        :param vals: An array of values dicts for bulk insert
        :return: tuple[count of rows loaded, count of rows skipped]
        """
        requested = len(vals)
        # Wrap values in SQLAlchemy Values object
        sqla_vals = values(
            column("dataset_ref", UUID),
            column("classifier", String),
            column("source_dataset_ref", UUID),
            name="batch_data",
        ).data(vals)
        # Join Values object against the dataset table, via both FK relations to
        # filter out external lineage that cannot be loaded into a legacy lineage index driver
        derived_ds = DATASET.alias("derived")
        source_ds = DATASET
        sel_query = sqla_vals.select().where(
            derived_ds.select()
            .where(derived_ds.c.id == sqla_vals.c.dataset_ref)
            .exists(),
            source_ds.select()
            .where(source_ds.c.id == sqla_vals.c.source_dataset_ref)
            .exists(),
        )
        query = (
            insert(DATASET_SOURCE)
            .from_select(["dataset_ref", "classifier", "source_dataset_ref"], sel_query)
            .on_conflict_do_nothing(index_elements=["classifier", "dataset_ref"])
        )
        res = self._connection.execute(query)
        return res.rowcount, requested - res.rowcount

    def get_duplicates(
        self, match_fields: Iterable[Field], expressions: Iterable[Expression]
    ) -> Iterable[Row]:
        if "time" in [f.name for f in match_fields]:
            return self.get_duplicates_with_time(match_fields, expressions)

        group_expressions = tuple(
            type_cast(PgField, f).alchemy_expression for f in match_fields
        )

        select_query = (
            select(func.array_agg(DATASET.c.id).label("ids"), *group_expressions)
            .select_from(
                PostgresDbAPI._from_expression(DATASET, expressions, match_fields)
            )
            .where(
                and_(
                    DATASET.c.archived == None,
                    *(PostgresDbAPI._alchemify_expressions(expressions)),
                )
            )
            .group_by(*group_expressions)
            .having(func.count(DATASET.c.id) > 1)
        )
        return self._connection.execute(select_query)

    def get_duplicates_with_time(
        self, match_fields: Iterable[Field], expressions: Iterable[Expression]
    ) -> Iterable[Row]:
        """
        If considering time when searching for duplicates, we need to grant some amount of leniency
        in case timestamps are not exactly the same.
        From the set of datasets that are active and have the correct product (candidates),
        find all those whose extended timestamp range overlap (overlapping),
        then group them by the other fields.
        """
        fields = []
        time_field: Label[Any] | None = None
        for f in match_fields:
            if f.name == "time":
                time_field = type_cast(DateRangeDocField, f).expression_with_leniency
            else:
                fields.append(type_cast(PgField, f).alchemy_expression)

        if time_field is None:
            raise Exception("No timme field in duplicates query")

        candidates_table = (
            select(DATASET.c.id, time_field.label("time"), *fields)
            .select_from(
                PostgresDbAPI._from_expression(DATASET, expressions, match_fields)
            )
            .where(
                and_(
                    DATASET.c.archived == None,
                    *(PostgresDbAPI._alchemify_expressions(expressions)),
                )
            )
        )

        t1 = candidates_table.alias("t1")
        t2 = candidates_table.alias("t2")

        fields = [getattr(t1.c, f.name) for f in fields]
        overlapping = (
            select(t1.c.id, text("t1.time * t2.time as time_intersect"), *fields)
            .select_from(
                t1.join(t2, and_(t1.c.time.overlaps(t2.c.time), t1.c.id != t2.c.id))
            )
            .cte("time_overlap")
        )

        fields = [getattr(overlapping.c, f.name) for f in fields]
        final_query = (
            select(
                func.array_agg(func.distinct(overlapping.c.id)).label("ids"),
                *fields,
                text(
                    "(lower(time_intersect) at time zone 'UTC', upper(time_intersect) at time zone 'UTC') as time"
                ),
            )
            .select_from(overlapping)
            .group_by(*fields, text("time_intersect"))
            .having(func.count(overlapping.c.id) > 1)
        )

        return self._connection.execute(final_query)

    def count_datasets(
        self, expressions: Iterable[Expression], archived: bool | None = False
    ) -> int:
        raw_expressions = self._alchemify_expressions(expressions)
        if archived:
            where_exprs = and_(DATASET.c.archived.is_not(None), *raw_expressions)
        elif archived is not None:
            where_exprs = and_(DATASET.c.archived.is_(None), *raw_expressions)
        else:
            where_exprs = and_(*raw_expressions)

        select_query = (
            select(func.count())
            .select_from(self._from_expression(DATASET, expressions))
            .where(where_exprs)
        )

        return self._connection.scalar(select_query)

    def count_datasets_through_time(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
        period: str,
        time_field,
        expressions: Iterable[Expression],
    ) -> Iterator[tuple[tuple[datetime.datetime, datetime.datetime], int]]:
        results = self._connection.execute(
            self.count_datasets_through_time_query(
                start, end, period, time_field, expressions
            )
        )

        for time_period, dataset_count in results:
            # if not time_period.upper_inf:
            yield Range(time_period.lower, time_period.upper), dataset_count

    def count_datasets_through_time_query(
        self, start, end, period, time_field, expressions
    ):
        raw_expressions = self._alchemify_expressions(expressions)

        start_times = select(
            func.generate_series(start, end, cast(period, INTERVAL)).label(
                "start_time"
            ),
        ).alias("start_times")

        time_range_select = (
            select(
                func.tstzrange(
                    start_times.c.start_time, func.lead(start_times.c.start_time).over()
                ).label("time_period"),
            )
        ).alias("all_time_ranges")

        # Exclude the trailing (end time to infinite) row. Is there a simpler way?
        time_ranges = (
            select(
                time_range_select,
            ).where(~func.upper_inf(time_range_select.c.time_period))
        ).alias("time_ranges")

        count_query = (
            select(func.count("*"))
            .select_from(self._from_expression(DATASET, expressions))
            .where(
                and_(
                    time_field.alchemy_expression.overlaps(time_ranges.c.time_period),
                    DATASET.c.archived == None,
                    *raw_expressions,
                )
            )
        )

        return select(time_ranges.c.time_period, count_query.label("dataset_count"))

    @staticmethod
    def _from_expression(source_table, expressions=None, fields=None):
        join_tables = set()
        if expressions:
            join_tables.update(
                expression.field.required_alchemy_table for expression in expressions
            )
        if fields:
            # Ignore placeholder columns
            join_tables.update(
                field.required_alchemy_table for field in fields if field
            )
        join_tables.discard(source_table)

        table_order_hack = [
            DATASET_SOURCE,
            DATASET_LOCATION,
            DATASET,
            PRODUCT,
            METADATA_TYPE,
        ]

        from_expression = source_table
        for table in table_order_hack:
            if table in join_tables:
                from_expression = from_expression.join(table)
        return from_expression

    def get_product(self, id_):
        return self._connection.execute(
            PRODUCT.select().where(PRODUCT.c.id == id_)
        ).first()

    def get_metadata_type(self, id_):
        return self._connection.execute(
            METADATA_TYPE.select().where(METADATA_TYPE.c.id == id_)
        ).first()

    def get_product_by_name(self, name: str):
        return self._connection.execute(
            PRODUCT.select().where(PRODUCT.c.name == name)
        ).first()

    def get_metadata_type_by_name(self, name: str):
        return self._connection.execute(
            METADATA_TYPE.select().where(METADATA_TYPE.c.name == name)
        ).first()

    def insert_product(
        self,
        name: str,
        metadata,
        metadata_type_id,
        search_fields,
        definition,
        concurrently: bool = True,
    ):
        res = self._connection.execute(
            PRODUCT.insert().values(
                name=name,
                metadata=metadata,
                metadata_type_ref=metadata_type_id,
                definition=definition,
            )
        )

        type_id = res.inserted_primary_key[0]

        # Initialise search fields.
        self._setup_product_fields(
            type_id,
            name,
            search_fields,
            definition["metadata"],
            concurrently=concurrently,
        )
        return type_id

    def update_product(
        self,
        name: str,
        metadata,
        metadata_type_id,
        search_fields,
        definition,
        update_metadata_type: bool = False,
        concurrently: bool = False,
    ):
        res = self._connection.execute(
            PRODUCT.update()
            .returning(PRODUCT.c.id)
            .where(PRODUCT.c.name == name)
            .values(
                metadata=metadata,
                metadata_type_ref=metadata_type_id,
                definition=definition,
            )
        )
        type_id = res.first()[0]

        if update_metadata_type:
            if not self._connection.in_transaction():
                raise RuntimeError("Must update metadata types in transaction")

            self._connection.execute(
                DATASET.update()
                .where(DATASET.c.dataset_type_ref == type_id)
                .values(
                    metadata_type_ref=metadata_type_id,
                )
            )

        # Initialise search fields.
        self._setup_product_fields(
            type_id,
            name,
            search_fields,
            definition["metadata"],
            concurrently=concurrently,
            rebuild_view=True,
        )
        return type_id

    def delete_product(self, name: str, fields, definition):
        res = self._connection.execute(
            PRODUCT.delete().returning(PRODUCT.c.id).where(PRODUCT.c.name == name)
        )
        type_id = res.first()[0]

        # Update dynamic fields to remove deleted product fields
        self._setup_product_fields(
            type_id,
            name,
            fields,
            definition["metadata"],
            concurrently=False,
            delete=True,
        )

        return type_id

    def insert_metadata_type(
        self, name: str, definition, concurrently: bool = False
    ) -> None:
        res = self._connection.execute(
            METADATA_TYPE.insert().values(name=name, definition=definition)
        )
        type_id = res.inserted_primary_key[0]

        self._setup_metadata_type_fields(
            type_id, name, definition, concurrently=concurrently
        )

    def update_metadata_type(self, name: str, definition, concurrently: bool = False):
        res = self._connection.execute(
            METADATA_TYPE.update()
            .returning(METADATA_TYPE.c.id)
            .where(METADATA_TYPE.c.name == name)
            .values(name=name, definition=definition)
        )
        type_id = res.first()[0]

        self._setup_metadata_type_fields(
            type_id,
            name,
            definition,
            concurrently=concurrently,
            rebuild_views=True,
        )

        return type_id

    def check_dynamic_fields(
        self,
        concurrently: bool = False,
        rebuild_views: bool = False,
        rebuild_indexes: bool = False,
    ) -> None:
        _LOG.info(
            "Checking dynamic views/indexes. (rebuild views=%s, indexes=%s)",
            rebuild_views,
            rebuild_indexes,
        )

        for metadata_type in self.get_all_metadata_types():
            self._setup_metadata_type_fields(
                metadata_type.id,
                metadata_type.name,
                metadata_type.definition,
                rebuild_indexes=rebuild_indexes,
                rebuild_views=rebuild_views,
                concurrently=concurrently,
            )

    def _setup_metadata_type_fields(
        self,
        id_,
        name: str,
        definition,
        rebuild_indexes: bool = False,
        rebuild_views: bool = False,
        concurrently: bool = True,
    ) -> None:
        # Metadata fields are no longer used (all queries are per-dataset-type): exclude all.
        # This will have the effect of removing any old indexes that still exist.
        fields = get_dataset_fields(definition)
        exclude_fields = tuple(fields)

        dataset_filter = and_(
            DATASET.c.archived == None, DATASET.c.metadata_type_ref == id_
        )
        dynamic.check_dynamic_fields(
            self._connection,
            concurrently,
            dataset_filter,
            exclude_fields,
            fields,
            name,
            rebuild_indexes=rebuild_indexes,
            rebuild_view=rebuild_views,
        )

        for product in self._get_products_for_metadata_type(id_):
            self._setup_product_fields(
                product.id,
                product.name,
                fields,
                product.definition["metadata"],
                rebuild_view=rebuild_views,
                rebuild_indexes=rebuild_indexes,
                concurrently=concurrently,
            )

    def _setup_product_fields(
        self,
        id_,
        name: str,
        fields,
        metadata_doc,
        rebuild_indexes: bool = False,
        rebuild_view: bool = False,
        concurrently: bool = True,
        delete: bool = False,
    ) -> None:
        dataset_filter = and_(
            DATASET.c.archived == None, DATASET.c.dataset_type_ref == id_
        )
        if delete:
            excluded_field_names = tuple(field.name for field in fields.values())
        else:
            excluded_field_names = tuple(
                self._get_active_field_names(fields, metadata_doc)
            )

        dynamic.check_dynamic_fields(
            self._connection,
            concurrently,
            dataset_filter,
            excluded_field_names,
            fields,
            name,
            rebuild_indexes=rebuild_indexes,
            rebuild_view=rebuild_view,
            delete_view=delete,
        )

    @staticmethod
    def _get_active_field_names(fields, metadata_doc) -> Iterator:
        for field in fields.values():
            if field.can_extract:
                try:
                    value = field.extract(metadata_doc)
                    if value is not None:
                        yield field.name
                except (AttributeError, KeyError, ValueError):
                    continue

    def get_all_products(self):
        return self._connection.execute(
            PRODUCT.select().order_by(PRODUCT.c.name.asc())
        ).fetchall()

    def get_all_product_docs(self):
        return self._connection.execute(select(PRODUCT.c.definition))

    def _get_products_for_metadata_type(self, id_):
        return self._connection.execute(
            PRODUCT.select()
            .where(PRODUCT.c.metadata_type_ref == id_)
            .order_by(PRODUCT.c.name.asc())
        ).fetchall()

    def get_all_metadata_types(self):
        return self._connection.execute(
            METADATA_TYPE.select().order_by(METADATA_TYPE.c.name.asc())
        ).fetchall()

    def get_all_metadata_type_docs(self):
        return self._connection.execute(select(METADATA_TYPE.c.definition))

    def get_all_metadata_defs(self):
        return [
            r[0]
            for r in self._connection.execute(
                METADATA_TYPE.select(METADATA_TYPE.c.definition).order_by(
                    METADATA_TYPE.c.name.asc()
                )
            ).fetchall()
        ]

    def temporal_extent_by_product(
        self, product_id: int, min_time_offset, max_time_offset
    ) -> tuple[datetime.datetime, datetime.datetime]:
        time_min = DateDocField(
            "aquisition_time_min",
            "Min of time when dataset was acquired",
            DATASET.c.metadata,
            False,  # is it indexed
            offset=min_time_offset,
            selection="least",
        )

        time_max = DateDocField(
            "aquisition_time_max",
            "Max of time when dataset was acquired",
            DATASET.c.metadata,
            False,  # is it indexed
            offset=max_time_offset,
            selection="greatest",
        )

        res = self._connection.execute(
            select(
                func.min(time_min.alchemy_expression),
                func.max(time_max.alchemy_expression),
            ).where(DATASET.c.dataset_type_ref == product_id)
        ).first()

        if res is None:
            raise RuntimeError(
                "Product has no datasets and therefore no temporal extent"
            )
        return res

    def get_locations(self, dataset_id) -> list:
        return [
            record[0]
            for record in self._connection.execute(
                select(_dataset_uri_field(DATASET_LOCATION))
                .where(
                    and_(
                        DATASET_LOCATION.c.dataset_ref == dataset_id,
                        DATASET_LOCATION.c.archived == None,
                    )
                )
                .order_by(DATASET_LOCATION.c.added.desc(), DATASET_LOCATION.c.id.desc())
            ).fetchall()
        ]

    def get_archived_locations(self, dataset_id) -> list:
        """
        Return a list of uris and archived_times for a dataset
        """
        return [
            (location_uri, archived_time)
            for location_uri, archived_time in self._connection.execute(
                select(
                    _dataset_uri_field(DATASET_LOCATION), DATASET_LOCATION.c.archived
                )
                .where(
                    and_(
                        DATASET_LOCATION.c.dataset_ref == dataset_id,
                        DATASET_LOCATION.c.archived != None,
                    )
                )
                .order_by(DATASET_LOCATION.c.added.desc())
            ).fetchall()
        ]

    def remove_location(self, dataset_id, uri) -> bool:
        """
        Remove the given location for a dataset

        :returns bool: Was the location deleted?
        """
        scheme, body = split_uri(uri)
        res = self._connection.execute(
            delete(DATASET_LOCATION).where(
                and_(
                    DATASET_LOCATION.c.dataset_ref == dataset_id,
                    DATASET_LOCATION.c.uri_scheme == scheme,
                    DATASET_LOCATION.c.uri_body == body,
                )
            )
        )
        return res.rowcount > 0

    def archive_location(self, dataset_id, uri) -> bool:
        scheme, body = split_uri(uri)
        res = self._connection.execute(
            DATASET_LOCATION.update()
            .where(
                and_(
                    DATASET_LOCATION.c.dataset_ref == dataset_id,
                    DATASET_LOCATION.c.uri_scheme == scheme,
                    DATASET_LOCATION.c.uri_body == body,
                    DATASET_LOCATION.c.archived == None,
                )
            )
            .values(archived=func.now())
        )
        return res.rowcount > 0

    def restore_location(self, dataset_id, uri) -> bool:
        scheme, body = split_uri(uri)
        res = self._connection.execute(
            DATASET_LOCATION.update()
            .where(
                and_(
                    DATASET_LOCATION.c.dataset_ref == dataset_id,
                    DATASET_LOCATION.c.uri_scheme == scheme,
                    DATASET_LOCATION.c.uri_body == body,
                    DATASET_LOCATION.c.archived != None,
                )
            )
            .values(archived=None)
        )
        return res.rowcount > 0

    @override
    def __repr__(self) -> str:
        return f"PostgresDb<connection={self._connection!r}>"

    def list_users(self) -> Iterator:
        result = self._connection.execute(
            text("""
            select
                group_role.rolname as role_name,
                user_role.rolname as user_name,
                pg_catalog.shobj_description(user_role.oid, 'pg_authid') as description
            from pg_roles group_role
            inner join pg_auth_members am on am.roleid = group_role.oid
            inner join pg_roles user_role on am.member = user_role.oid
            where (group_role.rolname like 'agdc_%%') and not (user_role.rolname like 'agdc_%%')
            order by group_role.oid asc, user_role.oid asc;
        """)
        )
        for row in result:
            yield _core.from_pg_role(row.role_name), row.user_name, row.description

    def create_user(
        self, username: str, password: str, role, description: str | None = None
    ) -> None:
        pg_role = _core.to_pg_role(role)
        username = escape_pg_identifier(self._connection, username)
        sql = text(f"create user {username} password :password in role {pg_role}")
        self._connection.execute(sql, {"password": password})
        if description:
            sql = text(f"comment on role {username} is :description")
            self._connection.execute(sql, {"description": description})

    def drop_users(self, users: Iterable[str]) -> None:
        for username in users:
            sql = text(f"drop role {escape_pg_identifier(self._connection, username)}")
            self._connection.execute(sql)

    def grant_role(self, role: str, users: Iterable[str]) -> None:
        """
        Grant a role to a user.
        """
        pg_role = _core.to_pg_role(role)

        for user in users:
            if not _core.has_role(self._connection, user):
                raise ValueError(f"Unknown user {user!r}")

        _core.grant_role(self._connection, pg_role, users)

    def find_most_recent_change(self, product_id: int):
        """
        Find the database-local time of the last dataset that changed for this product.
        """
        return self._connection.execute(
            select(
                func.max(
                    func.greatest(
                        DATASET.c.added,
                        column("updated"),
                    )
                )
            ).where(DATASET.c.dataset_type_ref == product_id)
        ).scalar()
