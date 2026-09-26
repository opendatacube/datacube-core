# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""Compile real PostGIS queries without needing a running database."""

from contextlib import nullcontext
from copy import deepcopy
from unittest.mock import Mock
from uuid import UUID

import pytest
from odc.geo import geom
from sqlalchemy.dialects import postgresql

from datacube.drivers.postgis._api import PostgisDbAPI, get_dataset_fields
from datacube.drivers.postgis._spatial import SpatialIndexORMRegistry
from datacube.index.postgis._datasets import DatasetResource
from datacube.model import MetadataType, Product


@pytest.fixture
def query_capture(eo3_product):
    registry = SpatialIndexORMRegistry()
    registry.register(4326)
    db = Mock()
    db.spatial_index.side_effect = registry.get
    connection = PostgisDbAPI(db, Mock())
    statements = []

    def stream(statement):
        statements.append(statement.compile(dialect=postgresql.dialect()))
        return iter(())

    def scalar(statement):
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

    def configure(flags):
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
        {"lat": (-5, 10), "lon": (30, 45)},
        {"geopolygon": geom.box(30, -5, 45, 10, crs="EPSG:4326")},
    ],
)
def test_global_dataset_query_sql(
    query_capture, flag, operation, archived, spatial_query
):
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
    assert ("JOIN odc.spatial_4326" in sql) is (flag is not True)
    assert ("ST_Intersects" in sql) is (flag is not True)
    assert "odc.dataset.product_ref =" in sql
    assert dataset_id in statements[0].params.values()
    if archived is False:
        assert "odc.dataset.archived IS NULL" in sql
    elif archived is True:
        assert "odc.dataset.archived IS NOT NULL" in sql
    else:
        assert "odc.dataset.archived IS" not in sql
    if operation != "count":
        assert "ORDER BY id" in sql
        assert "LIMIT" in sql


@pytest.mark.parametrize("flags", [[True, False], [False, True]])
@pytest.mark.parametrize("operation", ["search_by_product", "count_by_product"])
def test_mixed_products_keep_their_own_spatial_filter(query_capture, flags, operation):
    resource, statements = query_capture(flags)
    results = list(getattr(resource, operation)(lat=(-5, 10), lon=(30, 45)))
    if operation == "search_by_product":
        for _, rows in results:
            list(rows)
    assert len(statements) == 2
    for statement, flag in zip(statements, flags, strict=True):
        assert ("ST_Intersects" in str(statement)) is (not flag)


@pytest.mark.parametrize("flag", [None, False, True])
def test_product_global_datasets_defaults_false(eo3_product, flag):
    definition = deepcopy(eo3_product.definition)
    if flag is not None:
        definition["global_datasets"] = flag
    product = Product(eo3_product.metadata_type, definition)
    assert product.global_datasets is (flag is True)
