# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path


# The only thing tested under datacube/ is doctests, so only collect files
# that need testing (=have lines starting with whitespace + ">>>").
def pytest_ignore_collect(collection_path: Path) -> bool:
    if collection_path.is_dir():
        return False
    if collection_path.suffix != ".py":
        return True
    with collection_path.open() as f:
        return not any(line.strip().startswith(">>>") for line in f)
