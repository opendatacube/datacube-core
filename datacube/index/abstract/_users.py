# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from collections.abc import Iterable


class AbstractUserResource(ABC):
    """
    Abstract base class for the User portion of an index api.

    All UserResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """

    @abstractmethod
    def grant_role(self, role: str, usernames: str | Iterable[str]) -> bool:
        """
        Grant a role to users
        :param role: name of the database role
        :param usernames: usernames to grant the role to.
        :return: True if successful
        """

    @abstractmethod
    def create_user(
        self, username: str, password: str, role: str, description: str | None = None
    ) -> bool:
        """
        Create a new user
        :param username: username of the new user
        :param password: password of the new user
        :param role: default role of the new user
        :param description: optional description for the new user
        :return: True if successful
        """

    @abstractmethod
    def delete_user(self, usernames: str | Iterable[str]) -> bool:
        """
        Delete database users
        :param usernames: usernames of users to be deleted
        :return: True if successful
        """

    @abstractmethod
    def list_users(self) -> Iterable[tuple[str, str, str | None]]:
        """
        List all database users
        :return: Iterable of (role, username, description) tuples
        """

    @staticmethod
    def _to_str_iter(sl: str | Iterable[str]) -> Iterable[str]:
        return [sl] if isinstance(sl, str) else sl
