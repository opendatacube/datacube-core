# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from datacube.utils.masking import *  # noqa: F401, F403

import warnings

warnings.warn("datacube.storage.masking has moved to datacube.utils.masking",
              category=DeprecationWarning)
