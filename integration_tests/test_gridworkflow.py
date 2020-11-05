# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from datacube.api.grid_workflow import GridWorkflow


# TODO: do we still need this now that driver manager is gone?
def test_create_gridworkflow_with_logging(index):
    from logging import getLogger, StreamHandler

    logger = getLogger(__name__)
    handler = StreamHandler()
    logger.addHandler(handler)

    try:
        gw = GridWorkflow(index)
    finally:
        logger.removeHandler(handler)
