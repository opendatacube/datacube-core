# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import contextlib
from collections.abc import Generator, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import ProgrammingError
from typing_extensions import Self

from ._utils import escape_pg_identifier


class UserRoleBase:
    # For mypy: will get overridden by Enum in concreate UserRole classes.
    value: str

    @classmethod
    def to_pg_role(cls, role_str: str) -> Self:
        raise NotImplementedError("UserRoleBase.to_pg_role")

    def simple_str(self) -> str:
        return self.value.split("_", 1)[1]

    @classmethod
    def all_roles(cls) -> Generator[str]:
        for role in cls:  #  type: ignore[attr-defined]
            yield role.simple_str()

    def higher_roles(self) -> list[Self]:
        raise NotImplementedError("UserRoleBase.higher_roles")

    def lower_roles(self) -> list[Self]:
        return [
            r
            for r in self.__class__  # type: ignore[attr-defined]
            if r != self and r not in self.higher_roles()
        ]

    def inherits_from(self) -> Self | None:
        raise NotImplementedError("UserRoleBase.inherits_from")

    def can_create_user(self) -> bool:
        raise NotImplementedError("UserRoleBase.can_create_user")


def has_role(
    conn: Connection,
    role_name: str,
    with_create_role: bool = False,
    superuser: bool = False,
) -> bool:
    res = conn.execute(
        text(
            f"SELECT rolname FROM pg_roles WHERE rolname='{role_name}'"
            f"{' and rolcreaterole' if with_create_role else ''}"
            f"{' and rolsuper' if superuser else ''}"
        )
    ).fetchall()
    return bool(res)


def grant_role(conn: Connection, role: UserRoleBase, users: Iterable[str]) -> None:
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
    conn.execute(
        text("grant {role} to {users}".format(users=", ".join(users), role=role.value))
    )


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


def ensure_role(conn: Connection, role: UserRoleBase) -> None:
    # Ensure role exists and has createrole attribute if required
    if has_role(conn, role.value):
        if role.can_create_user() and not has_role(
            conn, role.value, with_create_role=True
        ):
            conn.execute(text(f"alter role {role.value} with createrole"))
    else:
        conn.execute(
            text(
                f"create role {role.value} nologin inherit{' with createrole' if role.can_create_user() else ''}"
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
