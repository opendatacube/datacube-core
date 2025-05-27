# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from types import SimpleNamespace

import pytest

from datacube import Datacube
from datacube.testutils import gen_tiff_dataset, mk_test_image

odc_loader = pytest.importorskip("odc.loader")


def test_with_driver(tmpdir) -> None:
    tmpdir = Path(str(tmpdir))

    spatial = {
        "resolution": (15, -15),
        "offset": (11230, 1381110),
    }

    nodata = -999
    aa = mk_test_image(96, 64, "int16", nodata=nodata)

    ds, geobox = gen_tiff_dataset(
        [SimpleNamespace(name="aa", values=aa, nodata=nodata)],
        tmpdir,
        prefix="ds1-",
        timestamp="2018-07-19",
        **spatial,
    )
    assert ds.time is not None
    mm = ["aa"]
    mm = [ds.product.measurements[k] for k in mm]
    sources = Datacube.group_datasets([ds], "time")

    ds_data = Datacube.load_data(sources, geobox, mm, driver="rio", dask_chunks={})
    assert ds_data is not None
