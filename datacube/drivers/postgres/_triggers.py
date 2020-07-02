# coding=utf-8
"""
Methods for adding triggers to capture row update time-stamps
"""

from .sql import SCHEMA_NAME
from ._schema import (
    METADATA_TYPE, DATASET, DATASET_LOCATION, PRODUCT, DATASET_SOURCE
)

UPDATE_TIMESTAMP_SQL = """
create or replace function {schema}.set_row_update_time()
returns trigger as $$
begin
  new.updated = now();
  return new;
end;
$$ language plpgsql;
""".format(schema=SCHEMA_NAME)

UPDATE_COLUMN_MIGRATE_SQL_TEMPLATE = """
alter table {schema}.{table} add column if not exists updated
timestamptz not null default now();
"""

INSTALL_TRIGGER_SQL_TEMPLATE = """
drop trigger if exists row_update_time_{table} on {schema}.{table};
create trigger row_update_time_{table}
before update on {schema}.{table}
for each row
execute procedure {schema}.set_row_update_time();
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
