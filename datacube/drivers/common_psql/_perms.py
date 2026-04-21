# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import contextlib
import logging
from abc import ABCMeta, abstractmethod
from enum import Enum, EnumMeta

from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from typing_extensions import Self

from ._utils import escape_pg_identifier

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

    from sqlalchemy.engine import Connection

_LOG = logging.getLogger(__name__)


class ABCEnumMeta(ABCMeta, EnumMeta):
    pass


class UserRoleBase(Enum, metaclass=ABCEnumMeta):
    """
    Base class for representing user types.

    Should be subclassed by index drivers with their own user type hierarchy, e.g. ::

        from datacube.drivers.common_psql import UserRoleBase

        class UserRole(UserRoleBase):
            # Enumerate supported user type names
            # Should contain a driver specific prefix, in this example 'drv_'.
            USER = "drv_user"
            ADVANCED = drv_advanced"
            MANAGE = "drv_manage"
            ADMIN = "drv_admin"

            ...  # Implement remaining abstract methods as discussed below

        The standard expected user types would be "user" for regular read-only users,
        "manage" for index-maintenance/read-write users, and "admin" for
        schema-owner/maintainer users, but index drivers may add additional user types.
        A linear hierarchy is assumed.
    """

    @classmethod
    @abstractmethod
    def to_pg_role(cls, role_str: str) -> Self:
        """
        Converts convert user-facing user type names to internal database names

        Should be implemented by adding a driver-specific prefix.

        :param role_str: User-facing role name (e.g. "user", "manage", "admin")
        :return: DB-facing role name (e.g. "odc_user", "drv_manage", "agdc_admin")
        """
        ...

    def simple_str(self) -> str:
        """
        Returns the user-facing user type name for this UserRole.

        Default implementation splits on underscore.  Will need to be overridden if
        the driver's mapping doesn't conform to this pattern.
        """
        return self.value.split("_", 1)[1]

    @classmethod
    def all_role_names(cls) -> Generator[str]:
        """
        Returns all user-facing user type names
        """
        for role in cls:
            yield role.simple_str()

    @abstractmethod
    def higher_roles(self) -> list[Self]:
        """
        Returns all roles that have more privileges than this one.
        """
        ...

    def lower_roles(self) -> list[Self]:
        """
        Returns all roles that have fewer privileges than this one.
        """
        return [r for r in self.__class__ if r != self and r not in self.higher_roles()]

    @abstractmethod
    def inherits_from(self) -> Self | None:
        """
        Returns the role immediately below this one in the hierarchy, or None if this
        is the most privileged role.
        """
        ...

    @abstractmethod
    def can_create_user(self) -> bool:
        """
        Returns True if this role can create new users (typically only the most
        privileged role).

        Note that the following additional restriction always applies and is not
        checked or enforced by this method:

        * A user in a user group can only ever create users with a less privileged role
          than them. This means that only a user who is PostgreSQL superuser can create
          users in the most privileged role.
        """
        ...


def has_role(
    conn: Connection,
    role_name: str,
    with_create_role: bool = False,
    superuser: bool = False,
) -> bool:
    """
    Check if a role exists.

    :param conn: A SQLAlchemy connection object
    :param role_name: The name of the role being checked
    :param with_create_role: Only return true if the role has the createrole attribute
    :param superuser: Only return true if the role is a PostgreSQL superuser
    :return: True if the role exists (with the specified attributes)
    """
    csql = " and rolcreaterole" if with_create_role else ""
    ssql = " and rolsuper" if superuser else ""
    return bool(
        conn.execute(
            text(
                f"SELECT rolname FROM pg_roles WHERE rolname='{role_name}' {csql} {ssql}"
            )
        ).fetchall()
    )


def has_roles(
    conn: Connection,
    roles: Iterable[str],
    with_create_role: bool = False,
    superuser: bool = False,
) -> bool:
    """
    Check if a group of roles exist - calls has_role for each role.

    :return: Returns True if all roles exist (with the specified attributes)
    """
    return all(has_role(conn, r, with_create_role, superuser) for r in roles)


def grant_role(conn: Connection, role: UserRoleBase, users: Iterable[str]) -> bool:
    """
    Grant a UserRole to a user(s).

    Attempts to revoke any existing memberships of more privileged roles from the
    user first.

    :param conn: An SQLAlchemy connection object
    :param role: The role to grant
    :param users: The usernames to grant the role to.
    :return: True if the role was granted successfully.
    """
    users = [escape_pg_identifier(conn, user) for user in users]
    with contextlib.suppress(ProgrammingError):
        conn.execute(
            text(
                "revoke {roles} from {users}".format(
                    users=", ".join(users),
                    roles=", ".join(r.value for r in role.higher_roles()),
                )
            )
        )
    try:
        conn.execute(
            text(
                "grant {role} to {users}".format(
                    users=", ".join(users), role=role.value
                )
            )
        )
        return True
    except ProgrammingError:
        _LOG.error("Failed to grant role: %s", role.value)
        return False


def has_role_membership(
    conn: Connection, group_role: UserRoleBase, role: UserRoleBase, admin: bool = False
) -> bool:
    """
    Check whether an extending role has been granted a base role.

    :param conn: A SQLAlchemy connection object
    :param group_role: The base role, the role that should be granted. The group role
        that the other role is a member of.
    :param role: The extending role, the role that should have the group role granted
        to it, so that it can extend it with additional permissions. The role that is
        a member of the group role.
    :return: True if role is a member of the group_role.
    """
    return bool(
        conn.execute(
            text(
                f"""
            select 1
            from pg_auth_members m
            join pg_roles r on r.oid = m.roleid
            join pg_roles gr on gr.oid = m.member
            where gr.rolname = '{group_role.value}'
            and r.rolname = '{role.value}'
            {"and m.admin_option" if admin else ""}
        """
            )
        ).scalar()
    )


def ensure_role(conn: Connection, role: UserRoleBase) -> bool:
    """
    Ensure that a role exists and applies the role hierarchy.
    :param conn: An SQLAlchemy connection object
    :param role: The role object to ensure.
    :return: Returns True on success.
    """
    try:
        # Ensure role exists and has createrole attribute if required
        if has_role(conn, role.value):
            if role.can_create_user() and not has_role(
                conn, role.value, with_create_role=True
            ):
                conn.execute(text(f"alter role {role.value} with createrole"))
        else:
            conn.execute(
                text(
                    f"create role {role.value} nologin inherit"
                    f"{' createrole' if role.can_create_user() else ''}"
                )
            )
        # Ensure hierarchical role memberships
        if role.can_create_user():
            if (group := role.inherits_from()) is not None and not has_role_membership(
                conn, group, role
            ):
                conn.execute(text(f"grant {group.value} to {role.value}"))
        else:
            for group in role.lower_roles():
                if not has_role_membership(conn, group, role, admin=True):
                    conn.execute(
                        text(f"grant {group.value} to {role.value} with admin option")
                    )
        return True
    except ProgrammingError:
        _LOG.error("Failed to create or update role: %s", role.value)
        return False


def create_user(
    conn: Connection,
    username: str,
    password: str,
    role: UserRoleBase,
    description: str | None = None,
) -> bool:
    """
    Create a new database user with the specified role.

    :param conn: An SQLAlchemy connection object
    :param username: The username for the new user
    :param password: The password for the new user
    :param role: The role to assign the user
    :param description: A description of the user (optional)
    :return: True on success, False on failure, including if the user already exists.
    """
    if has_role(conn, username):
        _LOG.error("User already exists: %s", username)
        return False
    username = escape_pg_identifier(conn, username)
    sql = text(f"create user {username} password :password in role {role.value}")
    try:
        conn.execute(sql, {"password": password})
        if description:
            sql = text(f"comment on role {username} is :description")
            conn.execute(sql, {"description": description})
        return True
    except ProgrammingError:
        _LOG.error("Insufficient permission to create user: %s", username)
        return False


def drop_users(conn: Connection, usernames: Iterable[str]) -> bool:
    """
    Drop a user or users if they exist.

    :param conn: An SQLAlchemy connection object
    :param usernames: The usernames to drop
    :return: True if all users no longer exist after calling.
    """
    failed: list[str] = []
    for user in usernames:
        if has_role(conn, user):
            try:
                conn.execute(text(f"drop role {escape_pg_identifier(conn, user)}"))
            except ProgrammingError:
                failed.append(user)
    if failed:
        _LOG.error("Insufficient permission to drop users: %s", ", ".join(failed))
        return False
    return True
