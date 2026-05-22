# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Callable, Generator
from functools import wraps
from typing import Any, TypeAlias, cast

from sqlalchemy.exc import OperationalError

from datacube.index.exceptions import QueryTimeout

F: TypeAlias = Callable[..., Any]
G: TypeAlias = Callable[..., Generator[Any]]

try:
    from psycopg2.errors import QueryCanceled as QueryCanceled2
except ImportError:

    class QueryCanceled2(Exception):  # type: ignore[no-redef]  # noqa: N818
        pass


try:
    from psycopg.errors import QueryCanceled as QueryCanceled3
except ImportError:

    class QueryCanceled3(Exception):  # type: ignore[no-redef]  # noqa: N818
        pass


def catch_generator_timeout(gen: G) -> G:
    """
    Decorator to catch postgresql exceptions from query timeouts in a Generator function, and raise a QueryTimeout
    """

    @wraps(gen)
    def timeout_wrapper(*args: Any, **kwargs: Any) -> Generator[Any]:
        try:
            yield from gen(*args, **kwargs)
        except OperationalError as e:
            # Timeouts manifest as SQLA OperationalError, wrapping a psycopg(2) QueryCanceled exception
            if isinstance(e.orig, (QueryCanceled2, QueryCanceled3)):
                raise QueryTimeout(str(e)) from None
            raise

    return cast("G", timeout_wrapper)


def catch_timeout(func: F) -> F:
    """
    Decorator to catch postgresql exceptions from query timeouts in a simple function, and raise a QueryTimeout
    """

    @wraps(func)
    def timeout_wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except OperationalError as e:
            # Timeouts manifest as SQLA OperationalError, wrapping a psycopg(2) QueryCanceled exception
            if isinstance(e.orig, (QueryCanceled2, QueryCanceled3)):
                raise QueryTimeout(str(e)) from None
            raise

    return cast("F", timeout_wrapper)
