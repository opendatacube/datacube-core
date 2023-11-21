# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from logging.config import fileConfig

from alembic import context

from datacube.cfg import ODCConfig
from datacube.drivers.postgis._connections import PostGisDb
from datacube.drivers.postgis._schema import MetadataObj
from datacube.drivers.postgis._spatial import is_spindex_table_name
from datacube.drivers.postgis.sql import SCHEMA_NAME

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = MetadataObj

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        dialect_name="postgresql",
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table_schema="odc",
    )

    with context.begin_transaction():
        context.run_migrations()


def include_name(name, type_, parent_names):
    if type_ == "table":
        # Ignore postgis system table
        if name == "spatial_ref_sys" and parent_names["schema_name"] is None:
            return False

        # Ignore dynamically generated spatial index tables
        if is_spindex_table_name(name):
            return False

        # Include other tables
        return True
    elif type_ == "schema":
        if name is None or name == SCHEMA_NAME:
            # Monitor default and odc schema
            return True
        else:
            # Ignore any other schemas
            return False
    elif type_ == "column":
        if name == "updated" and parent_names["schema_name"] == SCHEMA_NAME:
            # Ignore updated columns with triggers - handled manually
            return False
        # Include other columns
        return True
    else:
        # Include any constraints, indexes, etc, that made it this far.
        return True


def get_odc_env():
    # In active Alembic Config?
    cfg = config.attributes.get('cfg')
    env = config.attributes.get('env')
    raw_config = config.attributes.get('raw_config')
    if not (cfg or env or raw_config):
        # No?  How about from alembic CLI -X args?
        x_args = context.get_x_argument(as_dictionary=True)
        cfg = x_args.get('cfg')
        if cfg:
            cfg = cfg.split(':')
        env = x_args.get('env')
        raw_config = x_args.get('raw_config')
    return ODCConfig.get_environment(env=env, config=cfg, raw_config=raw_config)


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # ODC index:
    index = config.attributes.get("index")
    if index:
        connectable = index._db._engine
    else:
        db = PostGisDb.create(
            get_odc_env(),
            application_name="migration"
        )
        connectable = db._engine

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema="odc",
            include_schemas=True,
            include_name=include_name,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
