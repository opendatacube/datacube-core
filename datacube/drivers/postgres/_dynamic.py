# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Methods for managing dynamic dataset field indexes and views.
"""

import logging
from collections.abc import Sequence

from sqlalchemy import Index, select, text

from ._core import schema_qualified
from ._schema import DATASET, METADATA_TYPE, PRODUCT
from .sql import CreateView, pg_exists

_LOG: logging.Logger = logging.getLogger(__name__)


def contains_all(d_: dict, *keys) -> bool:
    """
    Does the dictionary have values for all of the given keys?

    >>> contains_all({'a': 4}, 'a')
    True
    >>> contains_all({'a': 4, 'b': 5}, 'a', 'b')
    True
    >>> contains_all({'b': 5}, 'a')
    False
    """
    return all(d_.get(key) for key in keys)


def _ensure_view(
    conn, fields, name: str, replace_existing, where_expression, delete: bool = False
) -> None:
    """
    Ensure a view exists for the given fields
    """
    # Create a view of search fields (for debugging convenience).
    # 'dv_' prefix: dynamic view. To distinguish from views that are created as part of the schema itself.
    view_name = schema_qualified(f"dv_{name}_dataset")
    exists = pg_exists(conn, view_name)
    # This currently leaves a window of time without the views: it's primarily intended for development.
    if exists and replace_existing:
        _LOG.debug("Dropping view: %s (replace=%r)", view_name, replace_existing)
        conn.execute(text(f"drop view {view_name}"))
        exists = False
    if not exists:
        _LOG.debug("Creating view: %s", view_name)
        select_fields = [
            field.alchemy_expression.label(field.name)
            for field in fields.values()
            if not field.affects_row_selection
        ]
        conn.execute(
            CreateView(
                view_name,
                select(*select_fields)
                .select_from(DATASET.join(PRODUCT).join(METADATA_TYPE))
                .where(where_expression),
            )
        )
    elif exists and delete:
        _LOG.debug(f"Dropping view: {view_name}")
        conn.execute(text(f"drop view {view_name}"))
    else:
        _LOG.debug("View exists: %s (replace=%r)", view_name, replace_existing)
    legacy_name = schema_qualified(f"{name}_dataset")
    if pg_exists(conn, legacy_name):
        _LOG.debug("Dropping legacy view: %s", legacy_name)
        conn.execute(text(f"drop view {legacy_name}"))


def check_dynamic_fields(
    conn,
    concurrently,
    dataset_filter,
    excluded_field_names: Sequence[str],
    fields,
    name: str,
    rebuild_indexes: bool = False,
    rebuild_view: bool = False,
    delete_view: bool = False,
) -> None:
    """
    Check that we have expected indexes and views for the given fields
    """
    # If this type has time/space fields, create composite indexes (as they are often searched together)
    # We will probably move these into product configuration in the future.
    composite_indexes = (
        ("lat", "lon", "time"),
        ("time", "lat", "lon"),
        ("sat_path", "sat_row", "time"),
    )

    all_exclusions = tuple(excluded_field_names)
    for composite_names in composite_indexes:
        # If all of the fields are available in this product, we'll create a composite index
        # for them instead of individual indexes.
        if contains_all(fields, *composite_names):
            all_are_excluded = set(excluded_field_names) >= set(composite_names)
            _check_field_index(
                conn,
                [fields.get(f) for f in composite_names],
                name,
                dataset_filter,
                concurrently=concurrently,
                replace_existing=rebuild_indexes,
                # If all fields were excluded individually it should be removed.
                should_exist=not all_are_excluded,
                index_type="gist",
            )
            all_exclusions += composite_names

    # Create indexes for the individual fields.
    for field in fields.values():
        if not field.postgres_index_type:
            continue
        _check_field_index(
            conn,
            [field],
            name,
            dataset_filter,
            should_exist=field.indexed and (field.name not in all_exclusions),
            concurrently=concurrently,
            replace_existing=rebuild_indexes,
        )
    # A view of all fields
    _ensure_view(conn, fields, name, rebuild_view, dataset_filter, delete_view)


def _check_field_index(
    conn,
    fields,
    name_prefix,
    filter_expression,
    should_exist: bool = True,
    concurrently: bool = False,
    replace_existing: bool = False,
    index_type=None,
) -> None:
    """
    Check the status of a given index: add or remove it as needed
    """
    if index_type is None:
        if len(fields) > 1:
            raise ValueError("Must specify index type for composite indexes.")
        index_type = fields[0].postgres_index_type

    field_name = "_".join([f.name.lower() for f in fields])
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    index_name = f"dix_{name_prefix.lower()}_{field_name}"
    # Previous naming scheme
    legacy_name = f"dix_field_{name_prefix.lower()}_dataset_{field_name}"
    indexed_expressions = [f.alchemy_expression for f in fields]
    index = Index(
        index_name,
        *indexed_expressions,
        postgresql_where=filter_expression,
        postgresql_using=index_type,
        # Don't lock the table (in the future we'll allow indexing new fields...)
        postgresql_concurrently=concurrently,
    )
    exists = pg_exists(conn, schema_qualified(index_name))
    legacy_exists = pg_exists(conn, schema_qualified(legacy_name))

    # This currently leaves a window of time without indexes: it's primarily intended for development.
    if replace_existing or (not should_exist):
        if exists:
            _LOG.debug("Dropping index: %s (replace=%r)", index_name, replace_existing)
            index.drop(conn)
            exists = False
        if legacy_exists:
            _LOG.debug(
                "Dropping legacy index: %s (replace=%r)", legacy_name, replace_existing
            )
            Index(legacy_name, *indexed_expressions).drop(conn)
            legacy_exists = False

    if should_exist:
        if not (exists or legacy_exists):
            _LOG.info("Creating index: %s", index_name)
            index.create(conn)
        else:
            _LOG.debug("Index exists: %s  (replace=%r)", index_name, replace_existing)
