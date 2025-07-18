# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from typing_extensions import override

from datacube.drivers.postgis import PostGisDb
from datacube.index.abstract import AbstractUserResource
from datacube.index.postgis._transaction import IndexResourceAddIn

if TYPE_CHECKING:
    from datacube.index.postgis.index import Index


class UserResource(AbstractUserResource, IndexResourceAddIn):
    def __init__(self, db: PostGisDb, index: Index) -> None:
        self._db = db
        self._index = index

    @override
    def grant_role(self, role: str, *usernames: str) -> None:
        """
        Grant a role to users
        """
        with self._db_connection() as connection:
            connection.grant_role(role, usernames)

    @override
    def create_user(
        self, username: str, password: str, role: str, description: str | None = None
    ) -> None:
        """
        Create a new user.
        """
        with self._db_connection() as connection:
            connection.create_user(username, password, role, description=description)

    @override
    def delete_user(self, *usernames: str) -> None:
        """
        Delete a user
        """
        with self._db_connection() as connection:
            connection.drop_users(usernames)

    @override
    def list_users(self) -> Iterable[tuple[str, str, str | None]]:
        """
        :return: list of (role, user, description)
        """
        with self._db_connection() as connection:
            yield from connection.list_users()
