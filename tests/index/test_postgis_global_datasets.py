# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""Compile real PostGIS queries without needing a running database."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import nullcontext
from copy import deepcopy
from unittest.mock import Mock
from uuid import UUID

import pytest
from odc.geo import geom
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.compiler import Compiled

from datacube.drivers.postgis._api import PostgisDbAPI, get_dataset_fields
from datacube.drivers.postgis._schema import Dataset
from datacube.drivers.postgis._spatial import SpatialIndexORMRegistry
from datacube.index.postgis._datasets import DatasetResource
from datacube.model import MetadataType, Product, QueryDict

TYPE_CHECKING = False
if TYPE_CHECKING:
    from sqlalchemy.sql import Select

QueryCapture = Callable[[Sequence[bool | None]], tuple[DatasetResource, list[Compiled]]]


@pytest.fixture
def query_capture(eo3_product: Product) -> QueryCapture:
    registry = SpatialIndexORMRegistry()
    registry.register(4326)
    db = Mock()
    db.spatial_index.side_effect = registry.get
    connection = PostgisDbAPI(db, Mock())
    statements: list[Compiled] = []

    def stream(statement: Select) -> Iterator[tuple]:
        statements.append(statement.compile(dialect=postgresql.dialect()))
        return iter(())

    def scalar(statement: Select) -> int:
        statements.append(statement.compile(dialect=postgresql.dialect()))
        return 3

    connection.stream_query = stream
    connection.run_scalar_query = scalar
    index = Mock()
    index._active_connection.side_effect = lambda **kwargs: nullcontext(connection)
    resource = DatasetResource(db, index)
    metadata = MetadataType(
        eo3_product.metadata_type.definition, search_field_extractor=get_dataset_fields
    )

    def configure(
        flags: Sequence[bool | None],
    ) -> tuple[DatasetResource, list[Compiled]]:
        products = []
        for product_id, flag in enumerate(flags, 1):
            definition = deepcopy(eo3_product.definition)
            definition["name"] = f"product_{product_id}"
            if flag is not None:
                definition["global_datasets"] = flag
            products.append(Product(metadata, definition, id_=product_id))
        index.products.search_robust.side_effect = lambda **query: (
            (product, {k: v for k, v in query.items() if k != "product"})
            for product in products
        )
        return resource, statements

    return configure


@pytest.mark.parametrize("flag", [None, False, True])
@pytest.mark.parametrize("operation", ["search", "search_returning", "count"])
@pytest.mark.parametrize("archived", [False, True, None])
@pytest.mark.parametrize(
    "spatial_query",
    [
        {},
        {"lat": (-5, 10), "lon": (30, 45)},
        {"geopolygon": geom.box(30, -5, 45, 10, crs="EPSG:4326")},
    ],
)
def test_global_dataset_query_sql(
    query_capture: QueryCapture,
    flag: bool | None,
    operation: str,
    archived: bool | None,
    spatial_query: QueryDict,
) -> None:
    resource, statements = query_capture([flag])
    dataset_id = UUID("16e020bf-0668-43e0-81ba-286351a589be")
    query = {"product": "product_1", "id": dataset_id, **spatial_query}
    if operation == "count":
        assert resource.count(archived=archived, **query) == 3
    else:
        options = {"field_names": ("id",)} if operation == "search_returning" else {}
        assert (
            list(
                getattr(resource, operation)(
                    limit=2, archived=archived, order_by=["id"], **options, **query
                )
            )
            == []
        )
    assert len(statements) == 1
    sql = str(statements[0])
    dialect = postgresql.dialect()
    spatial_index = SpatialIndexORMRegistry().get(4326)
    assert spatial_index is not None
    spatial_table = dialect.identifier_preparer.format_table(spatial_index.__table__)
    product_ref = str(Dataset.product_ref.compile(dialect=dialect))
    archived_column = str(Dataset.archived.compile(dialect=dialect))
    spatial_filter = bool(spatial_query) and flag is not True
    assert (f"JOIN {spatial_table}" in sql) is spatial_filter
    assert ("ST_Intersects" in sql) is spatial_filter
    assert f"{product_ref} =" in sql
    assert dataset_id in statements[0].params.values()
    if archived is False:
        assert f"{archived_column} IS NULL" in sql
    elif archived is True:
        assert f"{archived_column} IS NOT NULL" in sql
    else:
        assert f"{archived_column} IS" not in sql
    if operation != "count":
        assert "ORDER BY id" in sql
        assert "LIMIT" in sql


@pytest.mark.parametrize("flags", [[True, False], [False, True]])
@pytest.mark.parametrize("operation", ["search_by_product", "count_by_product"])
def test_mixed_products_keep_their_own_spatial_filter(
    query_capture: QueryCapture, flags: Sequence[bool], operation: str
) -> None:
    resource, statements = query_capture(flags)
    results = list(getattr(resource, operation)(lat=(-5, 10), lon=(30, 45)))
    if operation == "search_by_product":
        for _, rows in results:
            list(rows)
    assert len(statements) == 2
    for statement, flag in zip(statements, flags, strict=True):
        assert ("ST_Intersects" in str(statement)) is (not flag)


@pytest.mark.parametrize("flag", [None, False, True])
def test_product_global_datasets_defaults_false(
    eo3_product: Product, flag: bool | None
) -> None:
    definition = deepcopy(eo3_product.definition)
    if flag is not None:
        definition["global_datasets"] = flag
    product = Product(eo3_product.metadata_type, definition)
    assert product.global_datasets is (flag is True)


@pytest.mark.parametrize("flag", [None, False, True])
@pytest.mark.parametrize("operation", ["search", "search_returning", "count"])
def test_global_datasets_preserve_spatial_query_validation(
    query_capture: QueryCapture, flag: bool | None, operation: str
) -> None:
    resource, statements = query_capture([flag])
    options = {"field_names": ("id",)} if operation == "search_returning" else {}
    with pytest.raises(
        ValueError, match="Cannot specify spatial key lat AND geopolygon"
    ):
        result = getattr(resource, operation)(
            product="product_1",
            lat=(-5, 10),
            geopolygon=geom.box(30, -5, 45, 10, crs="EPSG:4326"),
            **options,
        )
        if operation != "count":
            list(result)
    assert statements == []
