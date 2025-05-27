# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Iterable

from typing_extensions import override

from datacube.index.abstract import AbstractUserResource


class UserResource(AbstractUserResource):
    def __init__(self) -> None:
        pass

    @override
    def grant_role(self, role: str, *usernames: str) -> None:
        raise NotImplementedError()

    @override
    def create_user(
        self, username: str, password: str, role: str, description: str | None = None
    ) -> None:
        raise NotImplementedError()

    @override
    def delete_user(self, *usernames: str) -> None:
        raise NotImplementedError()

    @override
    def list_users(self) -> Iterable[tuple[str, str, str]]:
        return []
