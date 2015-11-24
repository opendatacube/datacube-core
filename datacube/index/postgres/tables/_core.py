# coding=utf-8
"""
Core SQL schema settings.
"""
from __future__ import absolute_import

from sqlalchemy import MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql.expression import Executable, ClauseElement

SQL_NAMING_CONVENTIONS = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
SCHEMA_NAME = 'agdc'
METADATA = MetaData(naming_convention=SQL_NAMING_CONVENTIONS, schema=SCHEMA_NAME)


def ensure_db(connection, engine):
    is_new = False
    if not engine.dialect.has_schema(connection, SCHEMA_NAME):
        engine.execute(CreateSchema(SCHEMA_NAME))
        is_new = True

    METADATA.create_all(engine)

    return is_new


class View(Executable, ClauseElement):
    def __init__(self, name, select):
        self.name = name
        self.select = select


@compiles(View)
def visit_create_view(element, compiler, **kw):
    return "CREATE VIEW %s.%s AS %s" % (
        SCHEMA_NAME,
        element.name,
        compiler.process(element.select, literal_binds=True)
    )
