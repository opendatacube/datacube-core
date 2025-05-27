# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Iterable

from typing_extensions import override

from datacube.index.abstract import AbstractUserResource


class User:
    def __init__(
        self, username: str, password: str, role: str, description: str | None = None
    ) -> None:
        self.username = username
        self.password = password
        self.default_role = role
        self.roles = [role]
        self.description = description

    def grant_role(self, role: str) -> None:
        if role not in self.roles:
            self.roles.append(role)


class UserResource(AbstractUserResource):
    def __init__(self) -> None:
        self.roles = [
            "local_user",
            # For backwards compatibility with default driver
            "agdc_user",
            "agdc_ingest",
            "agdc_manage",
            "agdc_admin",
            # For forwards compatibility with future driver(s)
            "odc_user",
            "odc_ingest",
            "odc_manage",
            "odc_admin",
        ]
        self.users = {
            "localuser": User("localuser", "password123", "local_user", "Default user")
        }

    @override
    def grant_role(self, role: str, *usernames: str) -> None:
        if role not in self.roles:
            raise ValueError(f"{role} is not a known role")
        for user in usernames:
            if user not in self.users:
                raise ValueError(f"{user} is not a known username")
        for user in usernames:
            self.users[user].grant_role(role)

    @override
    def create_user(
        self, username: str, password: str, role: str, description: str | None = None
    ) -> None:
        if username in self.users:
            raise ValueError(f"User {username} already exists")
        if role not in self.roles:
            raise ValueError(f"{role} is not a known role")
        self.users[username] = User(username, password, role, description)

    @override
    def delete_user(self, *usernames: str) -> None:
        for user in usernames:
            if user not in self.users:
                raise ValueError(f"{user} is not a known username")
        for user in usernames:
            del self.users[user]

    @override
    def list_users(self) -> Iterable[tuple[str, str, str | None]]:
        return [
            (u.default_role, u.username, u.description) for u in self.users.values()
        ]
