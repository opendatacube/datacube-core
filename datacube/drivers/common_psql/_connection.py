# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import contextlib
from collections.abc import Generator

from sqlalchemy import text
from sqlalchemy.engine import Connection


@contextlib.contextmanager
def as_role(conn: Connection, role: str | None) -> Generator[Connection]:
    if role is None:
        yield conn
    else:
        try:
            db_user = conn.execute(text("select quote_ident(current_user)")).scalar()
            conn.execute(text(f"set role {role}"))
            yield conn
        finally:
            conn.execute(text(f"set role {db_user}"))
