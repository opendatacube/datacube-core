# coding=utf-8
"""
Module
"""
from __future__ import absolute_import, unicode_literals

import datetime

from dateutil import tz
from psycopg2._range import Range, NumericRange

from datacube.index.fields import Field
from datacube.scripts.search_tool import write_csv


class MockFile():
    def __init__(self):
        self.vals = []

    def write(self, s):
        self.vals.append(s)

    def getvalue(self):
        return ''.join(self.vals)


def test_csv_serialise():
    m = MockFile()
    write_csv(
        {"f1": Field("f1", ""), "f2": Field("f2", '')},
        [
            {"f1": 12, "f2": NumericRange(1.0, 2.0)},
            {"f1": datetime.datetime(2014, 7, 26, 23, 48, 0, tzinfo=tz.tzutc()), "f2": Range(-1.0, 2.0)},
            {"f1": datetime.datetime(2014, 7, 26, 23, 48, 0), "f2": "landsat"}
        ],
        m
    )

    assert m.getvalue() == '\r\n'.join(
        [
            'f1,f2',
            '12,1.0 to 2.0',
            '2014-07-26T23:48:00+00:00,-1.0 to 2.0',
            '2014-07-26T23:48:00+00:00,landsat',
            ''
        ]
    )
