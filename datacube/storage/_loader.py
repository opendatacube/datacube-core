# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
odc.loader based load.

separate file to reduce formatting issues.

"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from types import SimpleNamespace

import xarray as xr
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
from odc.loader.types import ReaderDriverSpec

from ..model import Dataset, ExtraDimensions, Measurement
from . import BandInfo


def ds_geobox(ds: Dataset, **kw) -> GeoBox | None:
    from ..testutils.io import eo3_geobox

    try:
        return eo3_geobox(ds, **kw)
    except ValueError:
        return None


def _extract_coords(extra_dims: ExtraDimensions) -> list[FixedCoord]:
    coords = extra_dims.dims

    return [
        FixedCoord(
            name=k,
            values=d["values"],
            dtype=str(d.get("dtype", "float32")),
            dim=k,
            units=d.get("units", None),
        )
        for k, d in coords.items()
    ]


def driver_based_load(
    driver: ReaderDriverSpec,
    sources: xr.DataArray,
    geobox: GeoBox,
    measurements: Sequence[Measurement],
    dask_chunks=None,
    skip_broken_datasets: bool = False,
    progress_cbk=None,
    extra_dims: ExtraDimensions | None = None,
    patch_url=None,
):
    fail_on_error = not skip_broken_datasets

    if extra_dims is None:
        extra_coords = []
    else:
        extra_coords = _extract_coords(extra_dims)

    tss = [
        datetime.fromtimestamp(float(ts) * 1e-9)
        for ts in sources.coords["time"].data.ravel()
    ]
    band_query: list[str] = [m.name for m in measurements]
    template = RasterGroupMetadata(
        bands={
            (m.name, 1): RasterBandMetadata(
                m.dtype, m.nodata, m.units, dims=tuple(m.get("dims", ()))
            )
            for m in measurements
        },
        aliases={name: [(name, 1)] for name in band_query},
        extra_dims={coord.dim: len(coord.values) for coord in extra_coords},
        extra_coords=extra_coords,
    )

    load_cfg = {
        m.name: RasterLoadParams(
            m.dtype,
            m.nodata,
            resampling=m.get("resampling", "nearest"),
            fail_on_error=fail_on_error,
            dims=tuple(m.get("dims", ())),
        )
        for m in measurements
    }

    chunks = dask_chunks

    if chunks is not None:
        chunk_shape = resolve_chunk_shape(
            len(tss), geobox, chunks, "float32", cfg=load_cfg
        )
    else:
        chunk_shape = (1, 2048, 2048)

    gbt = GeoboxTiles(geobox, (chunk_shape[1], chunk_shape[2]))

    tyx_bins: dict[tuple[int, int, int], list[int]] = {}
    srcs = []

    if patch_url is None:
        patch_url = lambda x: x  # noqa: E731

    def _dss():
        for tidx, dss in enumerate(sources.data):
            for ds in dss:
                yield tidx, ds

    def _ds_extract(ds: Dataset) -> dict[str, RasterSource]:
        out = {}
        for n in band_query:
            bi = BandInfo(ds, n)
            band_idx = bi.band if bi.band is not None else 1

            if bi.dims is not None and len(bi.dims) > 2:
                band_idx = 0  # 0 indicates extra dims

            out[n] = RasterSource(
                patch_url(bi.uri),
                band=band_idx,
                subdataset=bi.layer,
                geobox=ds_geobox(ds, band=n),
                meta=template.bands[(n, band_idx or 1)],
                driver_data=bi.driver_data,
            )

        return out

    for tidx, ds in _dss():
        srcs.append(_ds_extract(ds))
        for iy, ix in gbt.tiles(ds.extent):
            tyx_bins.setdefault((tidx, iy, ix), []).append(len(srcs) - 1)

    if driver == "kk-debug":
        return SimpleNamespace(
            load_cfg=load_cfg,
            template=template,
            srcs=srcs,
            tyx_bins=tyx_bins,
            gbt=gbt,
            tss=tss,
        )

    rdr = reader_driver(driver)

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
