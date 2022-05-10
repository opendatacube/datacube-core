# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from collections import namedtuple

Range = namedtuple('Range', ('begin', 'end'))


def ranges_overlap(ra: Range, rb: Range) -> bool:
    """
    Check whether two ranges overlap.

    (Assumes the start of the range is included in the range and the end of the range is not.)

    :return: True if the ranges overlap.
    """
    if ra.begin <= rb.begin:
        return ra.end > rb.begin
    return rb.end > ra.begin
