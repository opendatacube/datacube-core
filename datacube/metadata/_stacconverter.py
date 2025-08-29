# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
EO3 -> STAC utilities.

Utilities for translating EO3 Datasets to STAC Items.
"""

import math
import mimetypes
import warnings
from collections.abc import Generator
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from pystac import Asset, Item, Link, MediaType
from pystac.extensions.eo import Band, EOExtension
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.view import ViewExtension

import datacube.utils.uris as dc_uris
from datacube.index.eo3 import is_doc_eo3, prep_eo3
from datacube.model import Dataset, Product
from datacube.utils import parse_time

from ..migration import ODC2DeprecationWarning
from ._utils import EO3_MD_TYPE, EO_MD_TYPE, eo3_to_stac_properties


def _lineage_fields(dataset: Dataset) -> dict:
    """
    Add custom lineage field to a STAC Item
    """
    if dataset.sources:
        lineage_dict = {key: [str(ds.id)] for key, ds in dataset.sources.items()}
    elif dataset.source_tree and dataset.source_tree.children:
        lineage_dict = {
            key: [str(child.dataset_id) for child in children]
            for key, children in dataset.source_tree.children.items()
        }
    else:
        return {}
    return {"odc:lineage": lineage_dict}


def _proj_fields(grid: dict[str, Any], grid_name: str = "default") -> dict:
    """
    Get any proj (Stac projection extension) fields if we have them for the grid.
    """
    if not grid:
        return {}

    grid_info = grid.get(grid_name or "default")
    if not grid_info:
        return {}

    return {
        "shape": grid_info.get("shape"),
        "transform": grid_info.get("transform"),
    }


def _media_type(path: Path) -> str:
    """
    Add media type of the asset object
    """
    if path.suffix == ".sha1":
        return MediaType.TEXT
    if path.suffix == ".yaml":
        return "text/yaml"
    mime_type = mimetypes.guess_type(path.name)[0]
    if mime_type:
        if mime_type == "image/tiff":
            return MediaType.COG
        return mime_type
    return "application/octet-stream"


def _asset_roles_fields(asset_name: str) -> list[str]:
    """
    Add roles of the asset object
    """
    if asset_name.startswith("thumbnail"):
        return ["thumbnail"]
    return ["metadata"]


def _asset_title_fields(asset_name: str) -> str | None:
    """
    Add title of the asset object
    """
    if asset_name.startswith("thumbnail"):
        return "Thumbnail image"
    return None


def _uri_resolve(location: str | None, path: str) -> str:
    # ODC's method doesn't support empty locations. Fall back to the path alone.
    if not location:
        return path

    return dc_uris.uri_resolve(location, path)


def _stac_links(
    dataset: Dataset,
    stac_url: str | None,
    self_url: str | None,
    collection_url: str | None,
) -> Generator[Link, Any, Any]:
    """
    Add links for ODC product into a STAC Item
    """
    # TODO: better logic for relative links
    if dataset.uri:
        if not self_url:
            link = Link(
                rel="self",
                media_type=MediaType.JSON,
                target=dataset.uri.replace("odc-metadata.yaml", "stac-item.json"),
            )
            yield link
        if dataset.uri.endswith("yaml"):
            yield Link(
                title="ODC Dataset YAML",
                rel="odc_yaml",
                media_type="text/yaml",
                target=dataset.uri,
            )
    if self_url:
        yield Link(
            rel="self",
            media_type=MediaType.JSON,
            target=self_url,
        )

    if collection_url:
        yield Link(
            rel="collection",
            target=collection_url,
        )
    if stac_url:
        if not collection_url:
            yield Link(
                rel="collection",
                target=urljoin(stac_url, f"/stac/collections/{dataset.product.name}"),
            )
        yield Link(
            title="ODC Product Overview",
            rel="product_overview",
            media_type="text/html",
            target=urljoin(stac_url, f"product/{dataset.product.name}"),
        )
        yield Link(
            title="ODC Dataset Overview",
            rel="alternative",
            media_type="text/html",
            target=urljoin(stac_url, f"dataset/{dataset.id}"),
        )

    if not collection_url and not stac_url:
        warnings.warn("No collection provided for STAC Item.", stacklevel=2)


def ds2stac(
    dataset: Dataset,
    stac_url: str | None = None,
    self_url: str | None = None,
    collection_url: str | None = None,
) -> Item:
    if dataset.extent is None:
        geometry = None
        bbox = None
    else:
        wgs84_geometry = dataset.extent.to_crs("EPSG:4326", math.inf)
        geometry = wgs84_geometry.json
        bbox = wgs84_geometry.boundingbox.bbox

    properties = eo3_to_stac_properties(dataset, title=dataset.metadata.label)
    properties.update(_lineage_fields(dataset))

    dt = properties.get("datetime")
    if isinstance(dt, str):
        dt = parse_time(dt)

    item = Item(
        id=str(dataset.id),
        datetime=dt,
        properties=properties,
        geometry=geometry,
        bbox=bbox,
        collection=dataset.product.name,
    )

    # Add links
    for link in _stac_links(dataset, stac_url, self_url, collection_url):
        item.links.append(link)

    EOExtension.ext(item, add_if_missing=True)

    if dataset.extent:
        proj = ProjectionExtension.ext(item, add_if_missing=True)
        assert dataset.crs is not None  # for mypy - extent will be None if crs is None
        if str(dataset.crs).startswith("EPSG"):
            proj.apply(epsg=dataset.crs.epsg, **_proj_fields(dataset.grids))
        else:
            proj.apply(wkt2=dataset.crs.wkt, **_proj_fields(dataset.grids))

    # To pass validation, only add 'view' extension when we're using it somewhere.
    if any(k.startswith("view:") for k in properties):
        ViewExtension.ext(item, add_if_missing=True)

    # Add assets that are data
    for name, measurement in dataset.measurements.items():
        if not dataset.uri and not measurement.get("path"):
            # No URL to link to. URL is mandatory for Stac validation.
            continue

        asset = Asset(
            href=_uri_resolve(dataset.uri, measurement["path"]),
            media_type=_media_type(Path(measurement["path"])),
            title=name,
            roles=["data"],
        )
        eo = EOExtension.ext(asset)

        # TODO: pull out more information about the band
        band = Band.create(name)
        eo.apply(bands=[band])

        if dataset.crs:
            proj_fields = _proj_fields(
                dataset.grids, measurement.get("grid", "default")
            )
            if proj_fields is not None:
                proj = ProjectionExtension.ext(asset)  # type: ignore[arg-type]
                proj.apply(
                    shape=proj_fields["shape"],
                    transform=proj_fields["transform"],
                    epsg=dataset.crs.epsg,
                )

        item.add_asset(name, asset=asset)

    # Add assets that are accessories
    for name, accessory in dataset.accessories.items():
        if not dataset.uri and not accessory.get("path"):
            # No URL to link to. URL is mandatory for Stac validation.
            continue

        asset = Asset(
            href=_uri_resolve(dataset.uri, accessory["path"]),
            media_type=_media_type(Path(accessory["path"])),
            title=_asset_title_fields(name),
            roles=_asset_roles_fields(name),
        )

        item.add_asset(name, asset=asset)

    return item


def infer_eo3_product(metadata_doc: dict) -> Product:
    # Create a basic Product from a dataset metadata_doc
    name = metadata_doc["product"]["name"]
    doc = {
        "name": name,
        "metadata_type": "eo3",
        "metadata": {"product": {"name": name}},
        "measurements": [{"name": key} for key in metadata_doc["measurements"]],
    }
    return Product(EO3_MD_TYPE, doc)


def infer_eo_product(metadata_doc: dict) -> Product:
    name = metadata_doc["product_type"]
    doc = {
        "name": name,
        "metadata_type": "eo",
        "metadata": {"product_type": name},
        "measurements": [{"name": key} for key in metadata_doc["image"]["bands"]],
    }
    return Product(EO_MD_TYPE, doc)


def ds_doc_to_stac(
    metadata_doc: dict,
    uri: str | None = None,
    stac_url: str | None = None,
    self_url: str | None = None,
    collection_url: str | None = None,
) -> Item:
    warnings.warn("It is strongly preferred to use ds2stac if possible.", stacklevel=2)
    if is_doc_eo3(metadata_doc):
        product = infer_eo3_product(metadata_doc)
        dataset = Dataset(product, prep_eo3(metadata_doc), uri=uri)
    else:
        warnings.warn(
            "Support for legacy eo datasets is deprecated and will require an "
            "eo3-style properties dict to be added to the metadata.",
            ODC2DeprecationWarning, stacklevel=2,
        )
        product = infer_eo_product(metadata_doc)
        dataset = Dataset(product, metadata_doc, uri=uri)
    return ds2stac(dataset, stac_url, self_url, collection_url)
