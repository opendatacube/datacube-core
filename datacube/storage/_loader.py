# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
odc.loader based load.

separate file to reduce formatting issues.

"""

from __future__ import annotations

from datetime import datetime
from typing import Sequence

from odc.geo.geobox import GeoBox, GeoboxTiles
from odc.loader import (
    FixedCoord,
    RasterBandMetadata,
    RasterGroupMetadata,
    RasterLoadParams,
    RasterSource,
    chunked_load,
    reader_driver,
    resolve_chunk_shape,
)

from ..model import Dataset, Measurement
from . import BandInfo


def driver_based_load(
    driver,
    sources,
    geobox: GeoBox,
    measurements: Sequence[Measurement],
    dask_chunks=None,
    skip_broken_datasets=False,
    progress_cbk=None,
    extra_dims=None,
    patch_url=None,
):
    fail_on_error = not skip_broken_datasets

    if extra_dims is None:
        extra_dims = {}
    extra_coords = {k: FixedCoord(k, []) for k in extra_dims}

    rdr = reader_driver(driver)

    tss = [
        datetime.utcfromtimestamp(float(ts) * 1e-9)
        for ts in sources.coords["time"].data.ravel()
    ]
    band_query = [m.name for m in measurements]
    load_cfg = {
        m.name: RasterLoadParams(
            m.dtype,
            m.nodata,
            resampling=m.get("resampling", "nearest"),
            fail_on_error=fail_on_error,
        )
        for m in measurements
    }
    template = RasterGroupMetadata(
        bands={
            (m.name, 1): RasterBandMetadata(
                m.dtype, m.nodata, m.units, dims=m.get("dims", None)
            )
            for m in measurements
        },
        aliases={name: (name, 1) for name in band_query},
        extra_dims=extra_dims,
        extra_coords=extra_coords,
    )

    chunks = dask_chunks

    if chunks is not None:
        chunk_shape = resolve_chunk_shape(
            len(tss), geobox, chunks, "float32", cfg=load_cfg
        )
    else:
        chunk_shape = (1, 2048, 2048)

    gbt = GeoboxTiles(geobox, chunk_shape[1:])

    tyx_bins = {}  # (int,int,int) -> [int]
    srcs = []

    if patch_url is None:
        patch_url = lambda x: x

    def _dss():
        for tidx, dss in enumerate(sources.data):
            for ds in dss:
                yield tidx, ds

    def _ds_extract(ds: Dataset) -> dict[str, RasterSource]:
        out = {}
        for n in band_query:
            bi = BandInfo(ds, n)
            out[n] = RasterSource(
                patch_url(bi.uri),
                subdataset=bi.layer,
                driver_data=bi.driver_data,
            )

        return out

    for tidx, ds in _dss():
        srcs.append(_ds_extract(ds))
        for iy, ix in gbt.tiles(ds.extent):
            tyx_bins.setdefault((tidx, iy, ix), []).append(len(srcs) - 1)

    return chunked_load(
        load_cfg,
        template,
        srcs,
        tyx_bins=tyx_bins,
        gbt=gbt,
        tss=tss,
        env=rdr.capture_env(),
        rdr=rdr,
        chunks=dask_chunks,
        progress=progress_cbk,
    )
