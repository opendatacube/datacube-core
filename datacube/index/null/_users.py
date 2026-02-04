# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Iterable

from typing_extensions import override

from datacube.index.abstract import AbstractUserResource


class UserResource(AbstractUserResource):
    def __init__(self) -> None:
        pass

    @override
    def grant_role(self, role: str, usernames: str | Iterable[str]) -> bool:
        if not self._to_str_list(usernames):
            return True
        raise NotImplementedError()

    @override
    def create_user(
        self, username: str, password: str, role: str, description: str | None = None
    ) -> bool:
        raise NotImplementedError()

    @override
    def delete_user(self, usernames: str | Iterable[str]) -> bool:
        return True

    @override
    def list_users(self) -> Iterable[tuple[str, str, str]]:
        return []
