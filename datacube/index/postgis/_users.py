# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Iterable
from datacube.index.abstract import AbstractUserResource
from datacube.index.postgis._transaction import IndexResourceAddIn
from datacube.drivers.postgis import PostGisDb
from typing_extensions import override


class UserResource(AbstractUserResource, IndexResourceAddIn):
    def __init__(self, db: PostGisDb, index) -> None:
        """
        :type db: datacube.drivers.postgis.PostGisDb
        """
        from datacube.index.postgres.index import Index
        self._db = db
        self._index: Index = index

    @override
    def grant_role(self, role: str, *usernames: str) -> None:
        """
        Grant a role to users
        """
        with self._db_connection() as connection:
            connection.grant_role(role, usernames)

    @override
    def create_user(self, username: str, password: str,
                    role: str, description: str | None = None) -> None:
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
        :rtype: list[(str, str, str)]
        """
        with self._db_connection() as connection:
            for role, user, description in connection.list_users():
                yield role, user, description
