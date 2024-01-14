# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
""" Sample plugin "reads" zeros each time every time

TODO: not implemented yet
"""


class ZerosReaderDriver(object):
    def __init__(self):
        self.name = 'ZerosReader'
        self.protocols = ['zero']
        self.formats = ['0']

    def supports(self, protocol, fmt):
        return protocol == 'zero'

    def new_datasource(self, band):
        return None  # TODO


def init_driver():
    return ZerosReaderDriver()
