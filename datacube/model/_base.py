# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from collections import namedtuple

Range = namedtuple('Range', ('begin', 'end'))


def ranges_overlap(ra: Range, rb: Range) -> bool:
    if ra.begin <= rb.begin:
        return ra.end > rb.begin
    return rb.end > ra.begin
