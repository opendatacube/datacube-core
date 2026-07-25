# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest
import yaml

from datacube.index.postgis.index import Index as PGISIndex
from datacube.index.postgres.index import Index as PgIndex
from datacube.model import MetadataType

TYPE_CHECKING = False


@pytest.fixture
def index_cls(datacube_env_name) -> type[PgIndex | PGISIndex]:
    if datacube_env_name.startswith("postgis"):
        return PGISIndex
    return PgIndex


@pytest.mark.parametrize(
    "datacube_env_name", ("datacube", "datacube3", "postgis", "postgis3")
)
def test_with_standard_index(uninitialised_postgres_db, cfg_env, index_cls) -> None:
    index = index_cls(uninitialised_postgres_db, cfg_env)
    index.init_db()


@pytest.mark.parametrize("datacube_env_name", ("datacube3", "postgis3"))
def test_metadata_type_with_standard_index(index) -> None:
    metadata_doc = yaml.safe_load("""
    name: minimal
    description: minimal metadata definition
    dataset:
        id: [id]
        sources: [lineage, source_datasets]
        label: [label]
        creation_dt: ["properties", "odc:processing_datetime"]
        search_fields:
            some_custom_field:
                description: some custom field
                offset: [properties, custom]
        """)

    metadata = index.metadata_types.from_doc(metadata_doc)
    assert isinstance(metadata, MetadataType)
    assert metadata.id is None
    assert metadata.name == "minimal"
    assert "some_custom_field" in metadata.dataset_fields


def test_system_init(uninitialised_postgres_db, clirunner) -> None:
    result = clirunner(["system", "init"], catch_exceptions=False)

    # Question: Should the Index be able to be specified on the command line, or should it come from the config file?

    if result.exit_code != 0:
        print(result.output)
    assert result.exit_code == 0, f"Output: {result.output}"
