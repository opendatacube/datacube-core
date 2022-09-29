# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from typing import Iterable, Optional, Tuple
from datacube.index.abstract import AbstractUserResource
from datacube.index.postgis._transaction import IndexResourceAddIn
from datacube.drivers.postgis import PostGisDb


class UserResource(AbstractUserResource, IndexResourceAddIn):
    def __init__(self,
                 db: PostGisDb,
                 index: "datacube.index.postgis.index.Index"  # noqa: F821
                ) -> None:
        """
        :type db: datacube.drivers.postgis.PostGisDb
        """
        self._db = db
        self._index = index

    def grant_role(self, role: str, *usernames: str) -> None:
        """
        Grant a role to users
        """
        with self._db_connection() as connection:
            connection.grant_role(role, usernames)

    def create_user(self, username: str, password: str,
                    role: str, description: Optional[str] = None) -> None:
        """
        Create a new user.
        """
        with self._db_connection() as connection:
            connection.create_user(username, password, role, description=description)

    def delete_user(self, *usernames: str) -> None:
        """
        Delete a user
        """
        with self._db_connection() as connection:
            connection.drop_users(usernames)

    def list_users(self) -> Iterable[Tuple[str, str, Optional[str]]]:
        """
        :return: list of (role, user, description)
        :rtype: list[(str, str, str)]
        """
        with self._db_connection() as connection:
            for role, user, description in connection.list_users():
                yield role, user, description
