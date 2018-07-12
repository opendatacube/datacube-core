from sqlalchemy import MetaData, create_engine
from sqlalchemy.schema import Table
from sqlalchemy.pool import NullPool
from sqlalchemy.engine import reflection
import warnings

SQL_NAMING_CONVENTIONS = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
    # Other prefixes handled outside of sqlalchemy:
    # dix: dynamic-index, those indexes created automatically based on search field configuration.
    # tix: test-index, created by hand for testing, particularly in dev.
}
SCHEMA_NAME = 'agdc'
ENGINE = create_engine('postgresql://aj9439@agdcdev-db.nci.org.au:6432/datacube', poolclass=NullPool)


def is_table_equal(table, db_url, schema_name):
    # We use db_url instead of engine objects which rely on unreliable connection states
    egn = create_engine(db_url)
    meta = MetaData()
    meta.reflect(bind=egn, schema=schema_name, only=lambda name, _: name in [table.name])
    existing = Table(table.name, meta, autoload=True, autoload_with=egn)
    for column in table.c:
        if column not in existing.c:
            return False
    return True


def check_and_create_table(table, db_url, schema_name):
    # We use db_url instead of engine objects which rely on unreliable connection states
    egn = create_engine(db_url)
    inspector = reflection.Inspector.from_engine(egn)
    if table.name not in inspector.get_table_names(schema=schema_name):
        table.create(bind=egn)
    else:
        # verify whether schema is same
        if not is_table_equal(table, db_url, schema_name):
            # There is no previous versions, therefore
            # Existing table unlikely to be useful for migration
            # ToDo: should this be  a message to log file or an exception
            warnings.warn("Incompatible {} table exists in the database".format(table.name),
                          RuntimeWarning)


if __name__ == '__main__':
    META = MetaData(naming_convention=SQL_NAMING_CONVENTIONS, schema=SCHEMA_NAME)
    META.reflect(bind=ENGINE, schema=SCHEMA_NAME, only=lambda name, _: name in ['extent', 'extent_meta', 'ranges'])
