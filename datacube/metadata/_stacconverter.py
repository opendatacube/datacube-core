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
from pystac.extensions.raster import DataType, RasterBand, RasterExtension
from pystac.extensions.sar import SarExtension
from pystac.extensions.sat import SatExtension
from pystac.extensions.view import ViewExtension

import datacube.utils.uris as dc_uris
from datacube.index.eo3 import (
    EOGridsError,
    convert_eo_dataset,
    is_doc_eo3,
    prep_eo3,
)
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
    base_url: str | None,
    self_url: str | None,
    ds_yaml_url: str | None,
) -> Generator[Link, Any, Any]:
    """
    Add links for ODC product into a STAC Item
    """
    # TODO: better logic for relative links
    if self_url:
        yield Link(
            rel="self",
            media_type=MediaType.JSON,
            target=self_url,
        )
    elif base_url:
        yield Link(
            rel="self",
            media_type=MediaType.JSON,
            target=urljoin(
                base_url,
                f"/stac/collections/{dataset.product.name}/items/{dataset.id!s}",
            ),
        )
    else:
        warnings.warn("Unable to determine self link for STAC Item.", stacklevel=2)

    if ds_yaml_url:
        yield Link(
            title="ODC Dataset YAML",
            rel="odc_yaml",
            media_type="text/yaml",
            target=ds_yaml_url,
        )

    if base_url:
        if not ds_yaml_url:
            yield Link(
                title="ODC Dataset YAML",
                rel="odc_yaml",
                media_type="text/yaml",
                target=urljoin(base_url, f"/dataset/{dataset.id}.odc-metadata.yaml"),
            )
        yield Link(
            rel="collection",
            target=urljoin(base_url, f"/stac/collections/{dataset.product.name}"),
        )
        yield Link(
            title="ODC Product Overview",
            rel="product_overview",
            media_type="text/html",
            target=urljoin(base_url, f"product/{dataset.product.name}"),
        )
        yield Link(
            title="ODC Dataset Overview",
            rel="alternative",
            media_type="text/html",
            target=urljoin(base_url, f"dataset/{dataset.id}"),
        )
    else:
        warnings.warn("No collection provided for STAC Item.", stacklevel=2)


def ds2stac(
    dataset: Dataset,
    base_url: str | None = None,
    self_url: str | None = None,
    ds_yaml_url: str | None = None,
    asset_location: str | None = None,
) -> Item:
    """
    Convert an EO3-compatible ODC Dataset to a STAC Item.
    :param base_url: The URL off which the Item links are determined
    :param self_url: The Item self_link value
    :param ds_yaml_url: URL for the ODC Dataset YAML
    :param asset_location: Resolve Asset links against this URL.
        Will default to the dataset location if not provided.
    :return: pystac.Item
    """
    if not dataset.is_eo3:
        warnings.warn(
            "Support for legacy EO datasets is deprecated. "
            "The metadata will be converted to EO3.",
            ODC2DeprecationWarning,
            stacklevel=2,
        )
        try:
            dataset = convert_eo_dataset(dataset)
        except EOGridsError:
            dataset = convert_eo_dataset(dataset, open_datafiles=True)

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
    for link in _stac_links(dataset, base_url, self_url, ds_yaml_url):
        item.links.append(link)

    EOExtension.ext(item, add_if_missing=True)
    # Error: RasterExtension does not apply to type Item ???
    # RasterExtension.ext(item, add_if_missing=True)

    if dataset.extent:
        proj = ProjectionExtension.ext(item, add_if_missing=True)
        assert dataset.crs is not None  # for mypy - extent will be None if crs is None
        if str(dataset.crs).startswith("EPSG"):
            proj.apply(epsg=dataset.crs.epsg, **_proj_fields(dataset.grids))
        else:
            proj.apply(wkt2=dataset.crs.wkt, **_proj_fields(dataset.grids))

    # To pass validation, only add 'view' extension when we're using it somewhere.
    for k in properties:
        if k.startswith("view:"):
            ViewExtension.ext(item, add_if_missing=True)
        if k.startswith("sar:"):
            SarExtension.ext(item, add_if_missing=True)
        if k.startswith("sat:"):
            SatExtension.ext(item, add_if_missing=True)

    # url against which asset href can be resolved
    asset_location = asset_location or dataset.uri
    # Add assets that are data
    for name, measurement in dataset.measurements.items():
        if not asset_location and not measurement.get("path"):
            # No URL to link to. URL is mandatory for Stac validation.
            continue

        # TODO: migrate to new bands array - https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#band-migration
        asset = Asset(
            href=_uri_resolve(asset_location, measurement["path"]),
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
            if proj_fields:
                proj = ProjectionExtension.ext(asset)  # type: ignore[arg-type]
                proj.apply(
                    shape=proj_fields["shape"],
                    transform=proj_fields["transform"],
                    epsg=dataset.crs.epsg,
                )

        try:
            product = dataset.product
            m = product.measurements[product.canonical_measurement(name)]
            raster = RasterExtension.ext(asset)
            rband = RasterBand.create(
                nodata=m["nodata"],
                data_type=DataType(m["dtype"]),
                unit=m["units"],
            )
            raster.apply([rband])
        except ValueError:
            warnings.warn(
                f"Cannot determine raster extension properties for asset {name} "
                "as it is not defined in the Product.",
                stacklevel=2,
            )

        item.add_asset(name, asset=asset)

    # Add assets that are accessories
    for name, accessory in dataset.accessories.items():
        if not asset_location and not accessory.get("path"):
            # No URL to link to. URL is mandatory for Stac validation.
            continue

        asset = Asset(
            href=_uri_resolve(asset_location, accessory["path"]),
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
    name = metadata_doc[
        "product_type"
    ]  # this isn't always the product name, but it's the best we can do
    doc = {
        "name": name,
        "description": f"A barebones Product definition for an EO1 {name} dataset.",
        "metadata_type": "eo",
        "metadata": {"product_type": name},
        "measurements": [{"name": key} for key in metadata_doc["image"]["bands"]],
    }
    return Product(EO_MD_TYPE, doc)


def ds_doc_to_stac(
    metadata_doc: dict,
    ds_uri: str | None = None,
    base_url: str | None = None,
    self_url: str | None = None,
    ds_yaml_url: str | None = None,
    asset_location: str | None = None,
) -> Item:
    """
    Convert a raw dataset metadata document to a STAC Item.

    :metadata_doc: The raw ODC metadata document, loaded into a dict
    :param ds_uri: The dataset uri. Will override the location value in the metadata doc if exists.
    :param base_url: The URL off which the Item links are determined
    :param self_url: The Item self_link value
    :param ds_yaml_url: URL for the ODC Dataset YAML
    :param asset_location: Resolve Asset links against this URL
    :return: pystac.Item
    """
    warnings.warn("It is strongly preferred to use ds2stac if possible.", stacklevel=2)
    if is_doc_eo3(metadata_doc):
        product = infer_eo3_product(metadata_doc)
        metadata_doc = prep_eo3(metadata_doc)
    else:
        product = infer_eo_product(metadata_doc)
    dataset = Dataset(product, metadata_doc, uri=ds_uri or metadata_doc.get("location"))
    return ds2stac(dataset, base_url, self_url, ds_yaml_url, asset_location)
