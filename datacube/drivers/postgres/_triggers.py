# coding=utf-8
"""
Methods for adding triggers to capture row update time-stamps
"""

from .sql import SCHEMA_NAME
from ._schema import (
    METADATA_TYPE, DATASET, DATASET_LOCATION, PRODUCT, DATASET_SOURCE
)

UPDATE_TIMESTAMP_SQL = """
CREATE OR REPLACE FUNCTION {schema}.trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
""".format(schema=SCHEMA_NAME)

UPDATE_COLUMN_MIGRATE_SQL_TEMPLATE = """
ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS updated_at
TIMESTAMPTZ NOT NULL DEFAULT NOW();
"""

INSTALL_TRIGGER_SQL_TEMPLATE = """
CREATE TRIGGER set_timestamp
BEFORE UPDATE ON {schema}.{table}
FOR EACH ROW
EXECUTE PROCEDURE {schema}.trigger_set_timestamp();
"""

TABLE_NAMES = [
    METADATA_TYPE.name,
    PRODUCT.name,
    DATASET_SOURCE.name,
    DATASET.name,
    DATASET_LOCATION.name
]


def install_timestamp_trigger(conn):
    # Create trigger capture function
    conn.execute(UPDATE_TIMESTAMP_SQL)

    for name in TABLE_NAMES:
        # Add update_at columns
        # HACK: Make this more SQLAlchemy with add_column on Table objects
        conn.execute(UPDATE_COLUMN_MIGRATE_SQL_TEMPLATE.format(schema=SCHEMA_NAME, table=name))
        conn.execute(INSTALL_TRIGGER_SQL_TEMPLATE.format(schema=SCHEMA_NAME, table=name))
