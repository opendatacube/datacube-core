# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Useful methods for tests (particularly: reading/writing and checking files)
"""

import atexit
import contextlib
import json
import math
import os
import pathlib
import shutil
import tempfile
import typing
import uuid
import warnings
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

import numpy as np
import xarray as xr
from affine import Affine
from numpy.typing import NDArray
from odc.geo import CRS, wh_
from odc.geo.geobox import GeoBox

from datacube import Datacube
from datacube.model import Dataset, Measurement, MetadataType, Product
from datacube.ui.common import get_metadata_path
from datacube.utils import SimpleDocNav, read_documents
from datacube.utils.dates import mk_time_coord
from datacube.utils.documents import parse_yaml

_DEFAULT = object()


class BandObject(typing.Protocol):
    """A protocol defining an object with name, values, and nodata attributes."""

    name: str
    values: NDArray
    nodata: typing.Any  # Maybe could be int | float | None


def assert_file_structure(
    folder: pathlib.Path,
    expected_structure: Mapping[
        str, str | Sequence[str] | Mapping[str, str | Sequence[str]]
    ],
    root: str = "",
) -> None:
    """
    Assert that the contents of a folder (filenames and subfolder names recursively)
    match the given nested dictionary structure.
    """
    expected_filenames = set(expected_structure.keys())
    actual_filenames = {f.name for f in folder.iterdir()}

    if expected_filenames != actual_filenames:
        missing_files = expected_filenames - actual_filenames
        missing_text = f"Missing: {sorted(missing_files)!r}"
        extra_files = actual_filenames - expected_filenames
        added_text = f"Extra  : {sorted(extra_files)!r}"
        raise AssertionError(
            f"Folder mismatch of {root!r}\n\t{missing_text}\n\t{added_text}"
        )

    for k, v in expected_structure.items():
        id_ = f"{root}/{k}" if root else k

        f = folder.joinpath(k)
        if isinstance(v, Mapping):
            assert f.is_dir(), f"{id_} is not a dir"
            assert_file_structure(f, v, id_)
        elif isinstance(v, str | Sequence):
            assert f.is_file(), f"{id_} is not a file"
        else:
            raise AssertionError(
                "Only strings|[strings] and dicts expected when defining a folder structure."
            )


def write_files(file_dict: Mapping[str, str | Sequence[str]]) -> pathlib.Path:
    """
    Convenience method for writing a bunch of files to a temporary directory.

    Dict format is "filename": "text content"

    If content is another dict, it is created recursively in the same manner.

    writeFiles({'test.txt': 'contents of text file'})

    :return: Created temporary directory path
    """
    containing_dir = tempfile.mkdtemp(suffix="neotestrun")
    _write_files_to_dir(containing_dir, file_dict)

    def remove_if_exists(path) -> None:
        if os.path.exists(path):
            shutil.rmtree(path)

    atexit.register(remove_if_exists, containing_dir)
    return pathlib.Path(containing_dir)


def _write_files_to_dir(
    directory_path: str, file_dict: Mapping[str, str | Sequence[str]]
) -> None:
    """
    Convenience method for writing a bunch of files to a given directory.
    """
    for filename, contents in file_dict.items():
        path = os.path.join(directory_path, filename)
        if isinstance(contents, Mapping):
            os.mkdir(path)
            _write_files_to_dir(path, contents)
        else:
            with open(path, "w") as f:
                if isinstance(contents, str):
                    f.write(contents)
                elif isinstance(contents, Sequence):
                    f.writelines(contents)
                else:
                    raise ValueError(f"Unexpected file contents: {type(contents)}")


def isclose(a: float, b: float, rel_tol: float = 1e-09, abs_tol: float = 0.0) -> float:
    """
    Testing approximate equality for floats
    See https://docs.python.org/3/whatsnew/3.5.html#pep-485-a-function-for-testing-approximate-equality
    """
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def geobox_to_gridspatial(
    geobox: GeoBox | None,
) -> dict[str, dict[str, dict[str, str | dict[str, dict[str, float]]]]]:
    if geobox is None:
        return {}

    l, b, r, t = geobox.extent.boundingbox  # noqa: E741
    return {
        "grid_spatial": {
            "projection": {
                "geo_ref_points": {
                    "ll": {"x": l, "y": b},
                    "lr": {"x": r, "y": b},
                    "ul": {"x": l, "y": t},
                    "ur": {"x": r, "y": t},
                },
                "spatial_reference": str(geobox.crs),
            }
        }
    }


def mk_sample_eo(name: str = "eo") -> MetadataType:
    eo_yaml = f"""
name: {name}
description: Sample
dataset:
    id: ['id']
    label: ['ga_label']
    creation_time: ['creation_dt']
    measurements: ['image', 'bands']
    sources: ['lineage', 'source_datasets']
    format: ['format', 'name']
    grid_spatial: ['grid_spatial', 'projection']
    search_fields:
       time:
         type: 'datetime-range'
         min_offset: [['time']]
         max_offset: [['time']]
    """
    return MetadataType(parse_yaml(eo_yaml))


def mk_sample_product(
    name: str,
    description: str = "Sample",
    measurements: Sequence[str | tuple | dict] = ("red", "green", "blue"),
    with_grid_spec: bool = False,
    metadata_type=None,
    storage=None,
    load: bool | None = None,
) -> Product:
    if storage is None and with_grid_spec is True:
        storage = {
            "crs": "EPSG:3577",
            "resolution": {"x": 25, "y": -25},
            "tile_size": {"x": 100000.0, "y": 100000.0},
        }

    common = {"dtype": "int16", "nodata": -999, "units": "1", "aliases": []}

    if metadata_type is None:
        metadata_type = mk_sample_eo("eo")

    def mk_measurement(m):
        if isinstance(m, str):
            return dict(name=m, **common)
        elif isinstance(m, tuple):
            name, dtype, nodata = m
            m = common.copy()
            m.update(name=name, dtype=dtype, nodata=nodata)
            return m
        elif isinstance(m, dict):
            m_merged = common.copy()
            m_merged.update(m)
            return m_merged
        else:
            raise ValueError("Only support str|dict|(name, dtype, nodata)")

    measurements = [mk_measurement(m) for m in measurements]

    definition = {
        "name": name,
        "description": description,
        "metadata_type": metadata_type.name,
        "metadata": {},
        "measurements": measurements,
    }

    if storage is not None:
        definition["storage"] = storage

    if load is not None:
        definition["load"] = load

    return Product(metadata_type, definition)


def mk_sample_dataset(
    bands: list[dict],
    uri: str | list[str] | None = "file:///tmp",
    product_name: str = "sample",
    format: str = "GeoTiff",  # noqa: A002
    timestamp=None,
    id: str = "3a1df9e0-8484-44fc-8102-79184eab85dd",  # noqa: A002
    geobox: GeoBox | None = None,
    product_opts=None,
) -> Dataset:
    # pylint: disable=redefined-builtin
    image_bands_keys = "path layer band".split(" ")
    measurement_keys = "dtype units nodata aliases name".split(" ")

    def with_keys(d, keys) -> dict:
        return {k: d[k] for k in keys if k in d}

    measurements = [with_keys(m, measurement_keys) for m in bands]
    image_bands = {m["name"]: with_keys(m, image_bands_keys) for m in bands}

    if product_opts is None:
        product_opts = {}

    product = mk_sample_product(product_name, measurements=measurements, **product_opts)

    if timestamp is None:
        timestamp = "2018-06-29"
    if not uri:
        kwargs: dict[str, str | list[str] | None] = {"uri": None}
    elif isinstance(uri, str):
        kwargs = {"uri": uri}
    elif len(uri) == 1:
        kwargs = {"uri": uri[0]}
    else:
        kwargs = {"uris": uri}

    with suppress_deprecations():
        return Dataset(
            product,
            {
                "id": id,
                "format": {"name": format},
                "image": {"bands": image_bands},
                "time": timestamp,
                **geobox_to_gridspatial(geobox),
            },
            **kwargs,
        )


def make_graph_abcde(node) -> tuple[Any, Any, Any, Any, Any]:
    """
    A -> B
    |    |
    |    v
    +--> C -> D
    |
    +--> E
    """
    d = node("D")
    e = node("E")
    c = node("C", cd=d)
    b = node("B", bc=c)
    a = node("A", ab=b, ac=c, ae=e)
    return a, b, c, d, e


def dataset_maker(idx: int, t=None):
    """Return function that generates "dataset documents"

    (name, sources={}, **kwargs) -> dict
    """
    ns = uuid.UUID("c0fefefe-2470-3b03-803f-e7599f39ceff")
    postfix = "" if idx is None else f"{idx:04d}"

    if t is None:
        t = datetime.fromordinal(736637 + (0 if idx is None else idx))

    t = t.isoformat()

    def make(name: str, sources=_DEFAULT, **kwargs) -> dict:
        if sources is _DEFAULT:
            sources = {}

        return dict(
            id=str(uuid.uuid5(ns, name + postfix)),
            label=name + postfix,
            creation_dt=t,
            n=idx,
            lineage={"source_datasets": sources},
            **kwargs,
        )

    return make


def gen_dataset_test_dag(idx: int, t=None, force_tree: bool = False) -> Any:
    """Build document suitable for consumption by dataset add

    when force_tree is True pump the object graph through json
    serialise->deserialise, this converts DAG to a tree (no object sharing,
    copies instead).
    """

    def node_maker(n, t):
        mk = dataset_maker(n, t)

        def node(name: str, **kwargs):
            return mk(name, product_type=name, sources=kwargs)

        return node

    def deref(a):
        return json.loads(json.dumps(a))

    root, *_ = make_graph_abcde(node_maker(idx, t))
    return deref(root) if force_tree else root


def load_dataset_definition(path):
    if not isinstance(path, pathlib.Path):
        path = pathlib.Path(path)

    fname = get_metadata_path(path)
    for _, doc in read_documents(fname):
        return SimpleDocNav(doc)


def mk_test_image(w, h, dtype: str = "int16", nodata=-999, nodata_width: int = 4):
    """
    Create 2d ndarray where each pixel value is formed by packing x coordinate in
    to the upper half of the pixel value and y coordinate is in the lower part.

    So for uint16: im[y, x] == (x<<8) | y IF abs(x-y) >= nodata_width
                   im[y, x] == nodata     IF abs(x-y) < nodata_width

    really it's actually: im[y, x] == ((x & 0xFF ) <<8) | (y & 0xFF)

    If dtype is of floating point type:
       im[y, x] = (x + ((y%1024)/1024))

    Pixels along the diagonal are set to nodata values (to disable set nodata_width=0)
    """
    dtype = np.dtype(dtype)

    xx, yy = np.meshgrid(np.arange(w), np.arange(h))
    aa: np.ndarray[tuple[int, ...], np.dtype[np.float64 | np.signedinteger[Any]]]
    if dtype.kind == "f":
        aa = xx.astype(dtype) + (yy.astype(dtype) % 1024.0) / 1024.0
    else:
        nshift = dtype.itemsize * 8 // 2
        mask = (1 << nshift) - 1
        aa = ((xx & mask) << nshift) | (yy & mask)
        aa = aa.astype(dtype)

    if nodata is not None:
        aa[abs(xx - yy) < nodata_width] = nodata
    return aa


def split_test_image(aa):
    """
    Separate image created by mk_test_image into x,y components
    """
    if aa.dtype.kind == "f":
        y = np.round((aa % 1) * 1024)
        x = np.floor(aa)
    else:
        nshift = (aa.dtype.itemsize * 8) // 2
        mask = (1 << nshift) - 1
        y = aa & mask
        x = aa >> nshift
    return x, y


def gen_tiff_dataset(
    bands: list[BandObject],
    base_folder,
    prefix: str = "",
    timestamp: str = "2018-07-19",
    base_folder_of_record=None,
    **kwargs,
) -> tuple[Dataset, GeoBox]:
    """
       each band:
         .name    - string
         .values  - ndarray
         .nodata  - numeric|None

    :returns:  (Dataset, GeoBox)
    """
    from pathlib import Path

    from .io import write_gtiff

    if base_folder_of_record is None:
        base_folder_of_record = base_folder

    if not isinstance(bands, Sequence):
        bands = (bands,)

    # write arrays to disk and construct compatible measurement definitions
    measurement_defs = []
    for band in bands:
        name = band.name
        fname = prefix + name + ".tiff"
        meta = write_gtiff(
            base_folder / fname,
            band.values,
            nodata=band.nodata,
            overwrite=True,
            **kwargs,
        )

        geobox: GeoBox = meta.geobox

        measurement_defs.append(
            {
                "name": name,
                "path": fname,
                "layer": 1,
                "nodata": band.nodata,
                "dtype": meta.dtype,
            }
        )

    uri = Path(base_folder_of_record / "metadata.yaml").absolute().as_uri()
    ds = mk_sample_dataset(
        measurement_defs, uri=uri, timestamp=timestamp, geobox=geobox
    )
    return ds, geobox


def mk_sample_xr_dataset(
    crs: str | CRS = "EPSG:3578",
    shape=(33, 74),
    resolution: tuple[float, float] | None = None,
    xy=(0, 0),
    time="2020-02-13T11:12:13.1234567Z",
    name: str = "band",
    dtype: str = "int16",
    nodata=-999,
    units: str = "1",
):
    """Note that resolution is in Y,X order to match that of GeoBox.

    shape (height, width)
    resolution (y: float, x: float) - in YX, to match GeoBox/shape notation

    xy (x: float, y: float) -- location of the top-left corner of the top-left pixel in CRS units
    """
    if isinstance(crs, str):
        crs = CRS(crs)

    if resolution is None:
        resolution = (-10, 10) if crs is None or crs.projected else (-0.01, 0.01)

    t_coords = {}
    if time is not None:
        t_coords["time"] = mk_time_coord([time])

    transform = Affine.translation(*xy) * Affine.scale(*resolution[::-1])
    h, w = shape
    geobox = GeoBox(wh_(w, h), transform, crs)

    return Datacube.create_storage(
        t_coords,
        geobox,
        [Measurement(name=name, dtype=dtype, nodata=nodata, units=units)],
    )


def remove_crs(xx):
    xx = xx.reset_coords(["spatial_ref"], drop=True)

    for attribute_to_remove in ("crs", "grid_mapping"):
        xx.attrs.pop(attribute_to_remove, None)
        for x in xx.coords.values():
            x.attrs.pop(attribute_to_remove, None)

        if isinstance(xx, xr.Dataset):
            for x in xx.data_vars.values():
                x.attrs.pop(attribute_to_remove, None)

    return xx


def sanitise_doc(d):
    if isinstance(d, str):
        if d == "NaN":
            return math.nan
        return d
    elif isinstance(d, dict):
        return {k: sanitise_doc(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [sanitise_doc(i) for i in d]
    else:
        return d


@contextlib.contextmanager
def suppress_deprecations():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        try:
            yield
        finally:
            pass
