# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

from ._connection import as_role
from ._perms import (
    UserRoleBase,
    ensure_role,
    grant_role,
    has_role,
    has_role_membership,
)
from ._schema import create_schema, drop_schema, has_schema
from ._utils import ensure_extension, escape_pg_identifier, get_connection_info

__all__ = (
    "UserRoleBase",
    "as_role",
    "create_schema",
    "drop_schema",
    "ensure_extension",
    "ensure_role",
    "escape_pg_identifier",
    "get_connection_info",
    "grant_role",
    "has_role",
    "has_role_membership",
    "has_schema",
)
