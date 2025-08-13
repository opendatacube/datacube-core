# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Module
"""

import datetime
from datetime import timezone
from os import terminal_size

from sqlalchemy.dialects.postgresql import Range as PgRange

from datacube.index.fields import Field
from datacube.scripts.search_tool import write_csv, write_pretty


class MockFile:
    def __init__(self) -> None:
        self.vals: list = []

    def write(self, s) -> None:
        self.vals.append(s)

    def flush(self) -> None:
        pass

    def getvalue(self):
        return "".join(self.vals)


def test_csv_serialise() -> None:
    m = MockFile()
    write_csv(
        m,
        {"f1": Field("f1", ""), "f2": Field("f2", "")},
        [
            {"f1": 12, "f2": PgRange(1.0, 2.0)},
            {
                "f1": datetime.datetime(2014, 7, 26, 23, 48, 0, tzinfo=timezone.utc),
                "f2": PgRange(-1.0, 2.0),
            },
            {"f1": datetime.datetime(2014, 7, 26, 23, 48, 0), "f2": "landsat"},
        ],
    )

    assert m.getvalue() == (
        "f1,f2\r\n"
        "12,1.0 to 2.0\r\n"
        "2014-07-26T23:48:00+00:00,-1.0 to 2.0\r\n"
        "2014-07-26T23:48:00+00:00,landsat\r\n"
    )


def test_pretty_serialise() -> None:
    m = MockFile()
    write_pretty(
        m,
        {"f1": Field("f1", ""), "field 2": Field("f2", "")},
        [
            {"f1": 12, "field 2": PgRange(1.0, 2.0)},
            {
                "f1": datetime.datetime(2014, 7, 26, 23, 48, 0, tzinfo=timezone.utc),
                "field 2": PgRange(-1.0, 2.0),
            },
            {"f1": datetime.datetime(2014, 7, 26, 23, 48, 0), "field 2": "landsat"},
        ],
        terminal_size=terminal_size((12, 12)),
    )

    assert m.getvalue() == (
        "-[ 1 ]-----\n"
        "f1      | 12\n"
        "field 2 | 1.0 to 2.0\n"
        "-[ 2 ]-----\n"
        "f1      | 2014-07-26T23:48:00+00:00\n"
        "field 2 | -1.0 to 2.0\n"
        "-[ 3 ]-----\n"
        "f1      | 2014-07-26T23:48:00+00:00\n"
        "field 2 | landsat\n"
    )
