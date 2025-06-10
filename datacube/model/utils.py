# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import os
import platform
import sys
import uuid
from collections.abc import Generator, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Literal

import numpy
import toolz
import xarray
import yaml
from odc.geo import CRS
from odc.geo.geom import Geometry, point
from pandas import to_datetime
from xarray import DataArray

import datacube
from datacube.model import Dataset, Product
from datacube.utils import InvalidDocException, SimpleDocNav
from datacube.utils.py import sorted_items

try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper  # type: ignore


class BadMatch(Exception):  # noqa: N818
    pass


def machine_info() -> dict[str, dict[str, dict[str, str | dict[str, dict[str, str]]]]]:
    info: dict[str, str | dict[str, dict[str, str]]] = {
        "software_versions": {
            "python": {"version": sys.version},
            "datacube": {
                "version": datacube.__version__,
                "repo_url": "https://github.com/opendatacube/datacube-core.git",
            },
        },
        "hostname": platform.node(),
    }

    if hasattr(os, "uname"):
        info["uname"] = " ".join(os.uname())
    else:
        info["uname"] = " ".join(
            [
                platform.system(),
                platform.node(),
                platform.release(),
                platform.version(),
                platform.machine(),
            ]
        )

    return {"lineage": {"machine": info}}


def geobox_info(
    extent: Geometry, valid_data: Geometry | None = None
) -> dict[str, dict[str, dict[str, str | dict[str, float | dict[str, float]]]]]:
    image_bounds = extent.boundingbox
    data_bounds = valid_data.boundingbox if valid_data else image_bounds
    ul = point(data_bounds.left, data_bounds.top, crs=extent.crs).to_crs(
        CRS("EPSG:4326")
    )
    ur = point(data_bounds.right, data_bounds.top, crs=extent.crs).to_crs(
        CRS("EPSG:4326")
    )
    lr = point(data_bounds.right, data_bounds.bottom, crs=extent.crs).to_crs(
        CRS("EPSG:4326")
    )
    ll = point(data_bounds.left, data_bounds.bottom, crs=extent.crs).to_crs(
        CRS("EPSG:4326")
    )
    doc: dict[str, dict[str, dict[str, str | dict[str, float | dict[str, float]]]]] = {
        "extent": {
            "coord": {
                "ul": {"lon": ul.points[0][0], "lat": ul.points[0][1]},
                "ur": {"lon": ur.points[0][0], "lat": ur.points[0][1]},
                "lr": {"lon": lr.points[0][0], "lat": lr.points[0][1]},
                "ll": {"lon": ll.points[0][0], "lat": ll.points[0][1]},
            }
        },
        "grid_spatial": {
            "projection": {
                "spatial_reference": str(extent.crs),
                "geo_ref_points": {
                    "ul": {"x": image_bounds.left, "y": image_bounds.top},
                    "ur": {"x": image_bounds.right, "y": image_bounds.top},
                    "ll": {"x": image_bounds.left, "y": image_bounds.bottom},
                    "lr": {"x": image_bounds.right, "y": image_bounds.bottom},
                },
            }
        },
    }
    if valid_data:
        doc["grid_spatial"]["projection"]["valid_data"] = valid_data.__geo_interface__
    return doc


def new_dataset_info() -> dict[str, str]:
    return {
        "id": str(uuid.uuid4()),
        "creation_dt": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
    }


def band_info(band_names: Sequence[str], band_uris: dict | None = None) -> dict:
    """
    :param band_names: names of the bands
    :param band_uris: mapping from names to dicts with 'path' and 'layer' specs
    """
    if band_uris is None:
        band_uris = {name: {"path": "", "layer": name} for name in band_names}

    return {"image": {"bands": {name: band_uris[name] for name in band_names}}}


def time_info(
    time, start_time=None, end_time=None, key_time=None
) -> dict[str, dict[str, str]]:
    time_str = to_datetime(time).isoformat()
    start_time_str = to_datetime(start_time).isoformat() if start_time else time_str
    end_time_str = to_datetime(end_time).isoformat() if end_time else time_str
    extent = {
        "extent": {
            "from_dt": start_time_str,
            "to_dt": end_time_str,
            "center_dt": time_str,
        }
    }
    if key_time is not None:
        extent["extent"]["key_time"] = to_datetime(key_time).isoformat()
    return extent


def source_info(source_datasets):
    return {
        "lineage": {
            "source_datasets": {
                str(idx): dataset.metadata_doc
                for idx, dataset in enumerate(source_datasets)
            }
        }
    }


def datasets_to_doc(output_datasets: DataArray) -> DataArray:
    """
    Create a yaml document version of every dataset

    :param output_datasets: An array of :class:`datacube.model.Dataset`
    :return: An array of yaml document strings
    """

    def dataset_to_yaml(index, dataset):
        return yaml.dump(dataset.metadata_doc, Dumper=SafeDumper, encoding="utf-8")

    return xr_apply(output_datasets, dataset_to_yaml, dtype="O").astype("S")


def xr_iter(data_array: DataArray) -> Generator[tuple[tuple, dict, Any]]:
    """
    Iterate over every element in an xarray, returning::

        * the numerical index eg ``(10, 1)``
        * the labeled index eg ``{'time': datetime(), 'band': 'red'}``
        * the element (same as ``da[10, 1].item()``)

    :param data_array: Array to iterate over
    :return: i-index, label-index, value of da element
    """
    values = data_array.values
    coords = {coord_name: v.values for coord_name, v in data_array.coords.items()}
    for i in numpy.ndindex(data_array.shape):
        entry = values[i]
        index = {coord_name: v[i] for coord_name, v in coords.items()}
        yield i, index, entry


def xr_apply(
    data_array: DataArray, func, dtype=None, with_numeric_index: bool = False
) -> DataArray:
    """
    Apply a function to every element of a :class:`xarray.DataArray`

    :param func: function that takes a dict of labels and an element of the array,
        and returns a value of the given dtype
    :param dtype: The dtype of the returned array, default to the same as original
    :param with_numeric_index: If true include numeric index: func(index, labels, value)
    :return: The array with output of the function for every element.
    """
    if dtype is None:
        dtype = data_array.dtype

    data = numpy.empty(shape=data_array.shape, dtype=dtype)
    for i, index, entry in xr_iter(data_array):
        if with_numeric_index:
            v = func(i, index, entry)
        else:
            v = func(index, entry)
        data[i] = v
    return xarray.DataArray(data, coords=data_array.coords, dims=data_array.dims)


def make_dataset(
    product: Product,
    sources: Sequence[Dataset],
    extent: Geometry,
    center_time,
    valid_data: Geometry | None = None,
    uri: str | None = None,
    app_info: dict | None = None,
    band_uris: dict | None = None,
    start_time=None,
    end_time=None,
) -> Dataset:
    """
    Create :class:`datacube.model.Dataset` for the data

    :param product: Product the dataset is part of
    :param sources: datasets used to produce the dataset
    :param extent: extent of the dataset
    :param valid_data: extent of the valid data
    :param center_time: time of the central point of the dataset
    :param uri: The uri of the dataset
    :param app_info: Additional metadata to be stored about the generation of the product
    :param band_uris: band name to uri mapping
    :param start_time: start time of the dataset (defaults to `center_time`)
    :param end_time: end time of the dataset (defaults to `center_time`)
    """
    document: dict = {}
    merge(document, product.metadata_doc)
    merge(document, new_dataset_info())
    merge(document, machine_info())
    merge(document, band_info(list(product.measurements.keys()), band_uris=band_uris))
    merge(document, source_info(sources))
    merge(document, geobox_info(extent, valid_data))
    merge(document, time_info(center_time, start_time, end_time))
    merge(document, app_info or {})

    return Dataset(
        product,
        document,
        uris=[uri] if uri else None,
        sources={str(idx): dataset for idx, dataset in enumerate(sources)},
    )


def merge(a: dict, b: Mapping, path: list | None = None) -> dict:
    """
    Merge dictionary `b` into dictionary `a`

    See: http://stackoverflow.com/a/7205107/5262498
    """
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                raise ValueError(f"Conflict at {'.'.join(path + [str(key)])}")
        else:
            a[key] = b[key]
    return a


def traverse_datasets(
    ds: Dataset | SimpleDocNav,
    cbk,
    mode: Literal["post-order", "pre-order"] = "post-order",
    **kwargs,
) -> None:
    """Perform depth first traversal of lineage tree. Note that we assume it's a
    tree, even though it might be a DAG (Directed Acyclic Graph). If it is a
    DAG it will be treated as if it was a tree with some nodes appearing twice or more
    times in this tree.

    Order of traversal of nodes on the same level is in default sort order for
    strings (assuming your keys are strings, which is the case for Dataset
    object). NOTE: this could be problematic as order might be dependent on
    locale settings.

    If given a graph with cycles this will blow the stack, so don't do that.

    ds -- Dataset with lineage to iterate over, but really anything that has
          `sources` attribute which contains a dict from string to the same
          thing.

    cbk :: (Dataset, depth=0, name=None, **kwargs) -> None

    mode: post-order | pre-order

    mode=post-order -- Visit all lineage first, only then visit top level
    mode=pre-order --  Visit top level first, only then visit lineage
    """

    def visit_pre_order(
        ds: Dataset | SimpleDocNav, func, depth: int = 0, name: str | None = None
    ) -> None:
        func(ds, depth=depth, name=name, **kwargs)

        for k, v in sorted_items(ds.sources):
            visit_pre_order(v, func, depth=depth + 1, name=k)

    def visit_post_order(
        ds: Dataset | SimpleDocNav, func, depth: int = 0, name: str | None = None
    ) -> None:
        for k, v in sorted_items(ds.sources):
            visit_post_order(v, func, depth=depth + 1, name=k)

        func(ds, depth=depth, name=name, **kwargs)

    proc = {"post-order": visit_post_order, "pre-order": visit_pre_order}.get(
        mode, None
    )

    if proc is None:
        raise ValueError(f"Unsupported traversal mode: {mode}")

    proc(ds, cbk)


def flatten_datasets(
    ds: Dataset | SimpleDocNav, with_depth_grouping: bool = False
) -> dict | tuple[dict, list]:
    """Build a dictionary mapping from dataset.id to a list of datasets with that
    id appearing in the lineage DAG. When DAG is unrolled into a tree, some
    datasets will be reachable by multiple paths, sometimes these would be
    exactly the same python object, other times they will be duplicate views of
    the same "conceptual dataset object". If the same dataset is reachable by
    three possible paths from the root, it will appear three times in the
    flattened view.

    ds could be a Dataset object read from DB with `include_sources=True`, or
    it could be `SimpleDocNav` object created from a dataset metadata document
    read from a file.

    If with_depth_grouping=True, also build depth -> [Ds] mapping and return it
    along with Id -> [Ds] mapping. In this case top level is depth=0.
    """

    def get_list(out: dict, k):
        if k not in out:
            out[k] = []
        return out[k]

    def proc(
        ds, depth: int = 0, name: str | None = None, id_map=None, depth_map=None
    ) -> None:
        k = ds.id

        get_list(id_map, k).append(ds)
        if depth_map is not None:
            get_list(depth_map, depth).append(ds)

    id_map: dict = {}
    depth_map: dict | None = {} if with_depth_grouping else None

    traverse_datasets(ds, proc, id_map=id_map, depth_map=depth_map)

    if depth_map:
        # convert dict Int->V to just a list
        dout = [None] * len(depth_map)
        for k, v in depth_map.items():
            dout[k] = v

        return id_map, dout

    return id_map


def remap_lineage_doc(root: Mapping | SimpleDocNav, mk_node, **kwargs):
    def visit(ds):
        return mk_node(ds, {k: visit(v) for k, v in sorted_items(ds.sources)}, **kwargs)

    if not isinstance(root, SimpleDocNav):
        root = SimpleDocNav(root)

    try:
        return visit(root)
    except BadMatch as e:
        if str(root.id) not in str(e):
            raise BadMatch(f"Error loading lineage dataset: {e}") from None
        else:
            raise


def dedup_lineage(root: dict | SimpleDocNav):
    """Find duplicate nodes in the lineage tree and replace them with references.

    Will raise `ValueError` when duplicate dataset (same uuid, but different
    path from root) has either conflicting metadata or conflicting lineage
    data.

    :param root:

    Returns a new document that has the same structure as input document, but
    with duplicate entries now being aliases rather than copies.
    """

    def check_sources(a: dict, b: dict) -> bool:
        """True if two dictionaries contain same objects under the same names.
        same, not just equivalent.
        """
        if len(a) != len(b):
            return False

        for (ak, av), (bk, bv) in zip(sorted_items(a), sorted_items(b)):
            if ak != bk:
                return False
            if av is not bv:
                return False

        return True

    def mk_node(ds, sources, cache, sources_path):
        existing = cache.get(ds.id, None)
        doc = ds.doc_without_lineage_sources

        if existing is not None:
            _ds, _doc, _sources = existing

            if not check_sources(sources, _sources):
                raise InvalidDocException(
                    f"Inconsistent lineage for repeated dataset with _id: {ds.id}"
                )

            if doc != _doc:
                raise InvalidDocException(
                    f"Inconsistent metadata for repeated dataset with _id: {ds.id}"
                )

            return _ds

        out_ds = toolz.assoc_in(doc, sources_path, sources)
        cache[ds.id] = (out_ds, doc, sources)
        return out_ds

    if not isinstance(root, SimpleDocNav):
        root = SimpleDocNav(root)

    return remap_lineage_doc(root, mk_node, cache={}, sources_path=root.sources_path)
