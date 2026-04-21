# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing_extensions import override

from datacube.index.abstract import AbstractUserResource
from datacube.index.postgres._transaction import IndexResourceAddIn

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Iterable

    from datacube.drivers.postgres import PostgresDb
    from datacube.index.postgres.index import Index


class UserResource(AbstractUserResource, IndexResourceAddIn):
    def __init__(self, db: PostgresDb, index: Index) -> None:
        self._db = db
        self._index = index

    @override
    def grant_role(self, role: str, usernames: str | Iterable[str]) -> bool:
        """
        Grant a role to users
        """
        with self._db_connection() as connection:
            return connection.grant_role(role, self._to_str_iter(usernames))

    @override
    def create_user(
        self, username: str, password: str, role: str, description: str | None = None
    ) -> bool:
        """
        Create a new user.
        """
        with self._db_connection() as connection:
            return connection.create_user(
                username, password, role, description=description
            )

    @override
    def delete_user(self, usernames: str | Iterable[str]) -> bool:
        """
        Delete a user
        """
        with self._db_connection() as connection:
            return connection.drop_users(self._to_str_iter(usernames))

    @override
    def list_users(self) -> Iterable[tuple[str, str, str | None]]:
        """
        :return: list of (role, user, description)
        """
        with self._db_connection() as connection:
            yield from connection.list_users()
