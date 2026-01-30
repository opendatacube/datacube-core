# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import contextlib
import logging
from collections.abc import Generator, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import ProgrammingError
from typing_extensions import Self

from ._utils import escape_pg_identifier

_LOG = logging.getLogger(__name__)


class UserRoleBase:
    """
    Base class for representing user types.

    Should be subclassed by index drivers with their own user type hierarchy, e.g. ::

        from datacube.drivers.common_psql import UserRoleBase
        from enum import Enum
        from typing_extensions import Self, override

        class UserRole(UserRoleBase, Enum):
            # Enumerate supported user type names
            # Should contain a driver specific prefix, in this example 'drv_'.
            USER = "drv_user"
            ADVANCED = drv_advanced"
            MANAGE = "drv_manage"
            ADMIN = "drv_admin"

            ...  # Implement remaining methods as discussed below

        The standard expected user types would be "user" for regular read-only users, "manage" for index-maintenance
        read-write users, and "admin" for schema owner/maintainer users, but index
        drivers may add additional user types.  A linear hierarchy is assumed.
    """

    # For mypy: will get overridden by Enum in concrete UserRole classes.
    value: str

    @classmethod
    def to_pg_role(cls, role_str: str) -> Self:
        """
        Converts convert user-facing user type names to internal database names

        Should be implemented by adding a driver-specific prefix.

        :param role_str: User-facing role name (e.g. "user", "manage", "admin")
        :return: DB-facing role name (e.g. "odc_user", "drv_manage", "agdc_admin")
        """
        raise NotImplementedError("UserRoleBase.to_pg_role")

    def simple_str(self) -> str:
        """
        Returns the user-facing user type name for this UserRole.

        Default implementation splits on underscore.  Will need to be overridden if the driver's mapping
        doesn't conform to this pattern.
        """
        return self.value.split("_", 1)[1]

    @classmethod
    def all_roles(cls) -> Generator[str]:
        """
        Returns all user-facing user type names
        """
        for role in cls:  #  type: ignore[attr-defined]
            yield role.simple_str()

    def higher_roles(self) -> list[Self]:
        """
        Returns all roles that have more privileges than this one.
        :return:
        """
        raise NotImplementedError("UserRoleBase.higher_roles")

    def lower_roles(self) -> list[Self]:
        """
        Returns all roles that have less privileges than this one.
        """
        return [
            r
            for r in self.__class__  # type: ignore[attr-defined]
            if r != self and r not in self.higher_roles()
        ]

    def inherits_from(self) -> Self | None:
        """
        Returns the role immediately below this one in the hierarchy, or None if this is the most privileged role.
        """
        raise NotImplementedError("UserRoleBase.inherits_from")

    def can_create_user(self) -> bool:
        """
        Returns True if this role can create new users. (typically only the most privileged role).

        Note that the following additional restriction always applies and are not checked or enforced by this method:

        * A user in a user group can only ever create users with a less privileged role than them.
          This means that only a user who is postgresql superuser can create users in the most privileged role.
        """
        raise NotImplementedError("UserRoleBase.can_create_user")


def has_role(
    conn: Connection,
    role_name: str,
    with_create_role: bool = False,
    superuser: bool = False,
) -> bool:
    cre_sql = " and rolcreaterole" if with_create_role else ""
    sup_sql = " and rolsuper" if superuser else ""
    return bool(
        conn.execute(
            text(
                f"SELECT rolname FROM pg_roles WHERE rolname='{role_name}' {cre_sql} {sup_sql}"
            )
        ).fetchall()
    )


def has_roles(
    conn: Connection,
    roles: Iterable[str],
    with_create_role: bool = False,
    superuser: bool = False,
) -> bool:
    return all(has_role(conn, r, with_create_role, superuser) for r in roles)


def grant_role(conn: Connection, role: UserRoleBase, users: Iterable[str]) -> bool:
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
    results = conn.execute(
        text("grant {role} to {users}".format(users=", ".join(users), role=role.value))
    )
    return bool(results.rowcount)


def has_role_membership(
    conn: Connection, group_role: UserRoleBase, role: UserRoleBase, admin: bool = False
) -> bool:
    """
    Check whether an extending role has been granted a base role.

    :param conn: A SQLAlchemy connection object
    :param group_role: The base role, the role that should be granted. The group role that the other
        role is a member of.
    :param role: The extending role, the role that should have the group role granted to it, so that it
        can extend it with additional permissions. The role that is a member of the group role.
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
                    f"create role {role.value} nologin inherit{' createrole' if role.can_create_user() else ''}"
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
