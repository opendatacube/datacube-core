# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import datetime
from decimal import Decimal

import pytest

from datacube.drivers.postgis._fields import (
    BoolDocField,
    DateDocField,
    DoubleDocField,
    IntDocField,
    NumericDocField,
    UnindexableValue,
)
from datacube.drivers.postgis._schema import Dataset


def test_numeric_parse() -> None:
    fld = NumericDocField("test_fld", "field for testing", Dataset.metadata_doc, True)
    assert isinstance(fld.parse_value("55.88"), Decimal)
    with pytest.raises(UnindexableValue):
        fld.search_value_to_alchemy(float("nan"))


def test_int_parse() -> None:
    fld = IntDocField("test_fld", "field for testing", Dataset.metadata_doc, True)
    assert fld.parse_value("55") == 55


def test_float_parse() -> None:
    fld = DoubleDocField("test_fld", "field for testing", Dataset.metadata_doc, True)
    assert isinstance(fld.parse_value("55.88"), float)


def test_date_parse() -> None:
    fld = DateDocField("test_fld", "field for testing", Dataset.metadata_doc, True)
    assert fld.parse_value("2020-07-22T14:45:22.452434+0000") == datetime.datetime(
        2020, 7, 22, 14, 45, 22, 452434, tzinfo=datetime.timezone.utc
    )


def test_bool_parse() -> None:
    fld = BoolDocField("test_fld", "field for testing", Dataset.metadata_doc, True)
    assert fld.parse_value("false") is False
    assert fld.parse_value("true") is True
