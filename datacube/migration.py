# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

class ODC2DeprecationWarning(DeprecationWarning):
    """
    Subclasss of Deprecation Warning for API elements that are deprecated in 1.9 and will be removed in 2.0.

    Provided to support suppression of 1.9 deprecation warnings in e.g. sandbox-like environments to prevent
    end-users from freaking out.
    """
    pass
