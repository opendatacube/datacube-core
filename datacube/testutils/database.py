# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import configparser
import os
import time
from pathlib import Path

import docker
import psycopg2
import pytest
import yaml

from datacube import Datacube
from datacube.drivers.postgres import _core as pgres_core
from datacube.index import index_connect
from datacube.model import MetadataType


GET_DB_FROM_ENV = "get-the-db-from-the-environment-variable"


@pytest.fixture(scope="session")
def postgresql_server():
    """
    Provide a temporary PostgreSQL server for the test session using Docker.
    :return: dictionary configuration required to connect to the server
    """

    # If we're running inside docker already, don't attempt to start a container!
    # Hopefully we're using the `with-test-db` script and can use *that* database.
    if Path("/.dockerenv").exists() and os.environ.get("DATACUBE_DB_URL"):
        yield GET_DB_FROM_ENV

    else:

        client = docker.from_env()
        container = client.containers.run(
            "postgis/postgis:14-3.3-alpine",
            auto_remove=True,
            remove=True,
            detach=True,
            environment={
                "POSTGRES_PASSWORD": "badpassword",
                "POSTGRES_USER": "odc_tools_test",
            },
            ports={"5432/tcp": None},
        )
        try:
            while not container.attrs["NetworkSettings"]["Ports"]:
                time.sleep(1)
                container.reload()
            host_port = container.attrs["NetworkSettings"]["Ports"]["5432/tcp"][0][
                "HostPort"
            ]
            # From the documentation for the postgres docker image. The value of POSTGRES_USER
            # is used for both the user and the default database.
            yield {
                "db_hostname": "127.0.0.1",
                "db_username": "odc_tools_test",
                "db_port": host_port,
                "db_database": "odc_tools_test",
                "db_password": "badpassword",
                "index_driver": "default",
            }
            # 'f"postgresql://odc_tools_test:badpassword@localhost:{host_port}/odc_tools_test",
        finally:
            container.remove(v=True, force=True)


@pytest.fixture(scope='module')
def odc_db(postgresql_server, tmp_path_factory):
    if postgresql_server == GET_DB_FROM_ENV:
        return os.environ["DATACUBE_DB_URL"]
    else:
        temp_datacube_config_file = tmp_path_factory.mktemp('odc') / "test_datacube.conf"

        config = configparser.ConfigParser()
        config["default"] = postgresql_server
        with open(temp_datacube_config_file, "w", encoding="utf8") as fout:
            config.write(fout)

        postgres_url = "postgresql://{db_username}:{db_password}@{db_hostname}:{db_port}/{db_database}".format(
            **postgresql_server
        )

        # Wait for PostgreSQL Server to start up
        while True:
            try:
                with psycopg2.connect(postgres_url):
                    break
            except psycopg2.OperationalError:
                print("Waiting for PostgreSQL to become available")
                time.sleep(1)

        # Use pytest.MonkeyPatch instead of the monkeypatch fixture
        # to enable this fixture to not be function scoped
        mp = pytest.MonkeyPatch()

        # This environment variable points to the configuration file, and is used by the odc-tools CLI apps
        # as well as direct ODC API access, eg creating `Datacube()`
        mp.setenv(
            "DATACUBE_CONFIG_PATH",
            str(temp_datacube_config_file.absolute()),
        )
        # This environment is used by the `datacube ...` CLI tools, which don't obey the same environment variables
        # as the API and odc-tools apps.
        # See https://github.com/opendatacube/datacube-core/issues/1258 for more
        # pylint:disable=consider-using-f-string
        mp.setenv("DATACUBE_DB_URL", postgres_url)
        yield postgres_url
        mp.undo()


@pytest.fixture(scope='module')
def odc_test_db(odc_db, request):
    """
    Provide a temporary PostgreSQL server initialised by ODC, usable as
    the default ODC DB by setting environment variables.
    :return: Datacube instance
    """

    index = index_connect(validate_connection=False)
    index.init_db()

    dc = Datacube(index=index)
    # TODO Look into performance improvements, disabling logging

    yield dc

    dc.close()

    # This actually drops the schema, not the DB
    pgres_core.drop_db(index._db._engine)  # pylint:disable=protected-access

    # We need to run this as well, because SQLAlchemy grabs them into it's MetaData,
    # and attempts to recreate them

    _remove_postgres_dynamic_indexes()


def _remove_postgres_dynamic_indexes():
    """
    Clear any dynamically created postgresql indexes from the schema.
    """
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    for table in pgres_core.METADATA.tables.values():
        table.indexes.intersection_update(
            [i for i in table.indexes if not i.name.startswith("dix_")]
        )


@pytest.fixture(scope="module")
def populated_odc_db(odc_test_db, request):
    data_path = request.path.parent().joinpath('data')
    if data_path.exists():
        # Load Metadata Types
        for metadata_file in data_path.joinpath('metadata_types').glob('*.yaml'):
            with metadata_file.open(encoding="utf8") as f:
                meta_doc = yaml.safe_load(f)
                odc_test_db.index.metadata_types.add(MetadataType(meta_doc))

        # Load Products
        for metadata_file in data_path.joinpath('products').glob('*.yaml'):
            with metadata_file.open(encoding="utf8") as f:
                doc = yaml.safe_load(f)
                odc_test_db.index.products.add_document(doc)

        # Load Datasets

    yield odc_test_db
