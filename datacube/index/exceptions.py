# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0


class DuplicateRecordError(Exception):
    pass


class MissingRecordError(Exception):
    pass


class IndexSetupError(Exception):
    pass


class TransactionException(Exception):  # noqa: N818
    def __init__(self, *args, commit=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.commit = commit
