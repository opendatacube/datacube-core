# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from typing import Iterable, Optional, Tuple
from datacube.index.abstract import AbstractUserResource


class UserResource(AbstractUserResource):
    def __init__(self) -> None:
        pass

    def grant_role(self, role: str, *usernames: str) -> None:
        raise NotImplementedError()

    def create_user(self, username: str, password: str,
                    role: str, description: Optional[str] = None) -> None:
        raise NotImplementedError()

    def delete_user(self, *usernames: str) -> None:
        raise NotImplementedError()

    def list_users(self) -> Iterable[Tuple[str, str, str]]:
        return []
