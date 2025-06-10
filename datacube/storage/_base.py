# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

from datacube.model import Dataset
from datacube.utils.uris import pick_uri, uri_resolve


def _get_band_and_layer(b: dict[str, Any]) -> tuple[int | None, str | None]:
    """Encode legacy logic for extracting band/layer:

        on input:
        band -- Int | Nothing
        layer -- Str | Int | Nothing

    Valid combinations are:
        band  layer  Output
    ---------------------------
          -     -    ( - ,  - )
          -    int   (int,  - )
         int    -    (int,  - )
         int   str   (int, str)
          -    str   ( - , str)
    """
    band = b.get("band")
    layer = b.get("layer")

    if band is None:
        if isinstance(layer, int):
            return layer, None
        if layer is None or isinstance(layer, str):
            return None, layer

        raise ValueError(
            f"Expect `layer` to be one of None,int,str but it is {type(layer)}"
        )
    else:
        if not isinstance(band, int):
            raise ValueError(f"Expect `band` to be an integer (it is {type(band)})")
        if layer is not None and not isinstance(layer, str):
            raise ValueError(
                f"Expect `layer` to be one of None,str but it is {type(layer)}"
            )

        return band, layer


def _extract_driver_data(ds: Dataset, mm: dict[str, Any]) -> Any | None:
    ds_data = ds.metadata_doc.get("driver_data", None)
    mm_data = mm.get("driver_data", None)
    if isinstance(ds_data, dict) and isinstance(mm_data, str):
        return ds_data.get(mm_data, {})
    if mm_data is not None:
        return mm_data
    return ds_data


def measurement_paths(ds: Dataset) -> dict[str, str]:
    """
    Returns a dictionary mapping from band name to url pointing to band storage
    resource.

    :return: Band Name => URL
    """
    if not ds.uri:
        raise ValueError("No locations on this dataset")
    if not ds.has_multiple_uris():
        base = ds.uri
    else:
        base = pick_uri(ds.uris)

    return {k: uri_resolve(base, m.get("path")) for k, m in ds.measurements.items()}


class BandInfo:
    __slots__ = (
        "band",
        "crs",
        "dims",
        "driver_data",
        "dtype",
        "format",
        "layer",
        "name",
        "nodata",
        "transform",
        "units",
        "uri",
    )

    def __init__(
        self,
        ds: Dataset,
        band: str,
        uri_scheme: str | None = None,
        extra_dim_index: int | None = None,
        patch_url: Callable[[str], str] | None = None,
    ) -> None:
        try:
            (mp,) = ds.product.lookup_measurements([band]).values()
        except KeyError:
            raise ValueError(f"No such band: {band}") from None

        mm = ds.measurements.get(mp.canonical_name)

        if mm is None:
            raise ValueError(f"No such band: {band}")

        if not ds.uri:
            raise ValueError("No uris defined on a dataset")
        if not ds.has_multiple_uris():
            base_uri = ds.uri
        else:
            base_uri = ds.legacy_uri(uri_scheme)
        uri = uri_resolve(base_uri, mm.get("path"))
        if patch_url is not None:
            uri = patch_url(uri)

        bint, layer = _get_band_and_layer(mm)
        self.uri = uri
        self.name = band
        self.band = bint
        self.layer = layer
        self.dtype = mp.dtype
        self.nodata = mp.nodata
        self.units = mp.units
        self.crs = ds.crs
        self.transform = ds.transform
        self.format = ds.format or ""
        self.dims = mp.get("dims", None)
        self.driver_data = _extract_driver_data(ds, mm)

    @property
    def uri_scheme(self) -> str:
        return urlparse(self.uri).scheme or ""
