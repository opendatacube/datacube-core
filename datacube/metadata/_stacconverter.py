# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
import math
import mimetypes
import warnings
from collections.abc import Generator
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from pystac import Asset, Item, Link, MediaType
from pystac.errors import STACError
from pystac.extensions.eo import Band, EOExtension
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.view import ViewExtension
from pystac.utils import datetime_to_str

import datacube.utils.uris as dc_uris
from datacube.model import Dataset

EO3_TO_STAC_RENAMES = {
    "dtr:end_datetime": "end_datetime",
    "dtr:start_datetime": "start_datetime",
    "eo:gsd": "gsd",
    "eo:instrument": "instruments",
    "eo:platform": "platform",
    "eo:constellation": "constellation",
    "eo:off_nadir": "view:off_nadir",
    "eo:azimuth": "view:azimuth",
    "eo:sun_azimuth": "view:sun_azimuth",
    "eo:sun_elevation": "view:sun_elevation",
    "odc:processing_datetime": "created",
}


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
    mime_type = mimetypes.guess_type(path.name)[0]
    if path.suffix == ".sha1":
        return MediaType.TEXT
    elif path.suffix == ".yaml":
        return "text/yaml"
    elif mime_type:
        if mime_type == "image/tiff":
            return MediaType.COG
        else:
            return mime_type
    else:
        return "application/octet-stream"


def _asset_roles_fields(asset_name: str) -> list[str]:
    """
    Add roles of the asset object
    """
    if asset_name.startswith("thumbnail"):
        return ["thumbnail"]
    else:
        return ["metadata"]


def _asset_title_fields(asset_name: str) -> str | None:
    """
    Add title of the asset object
    """
    if asset_name.startswith("thumbnail"):
        return "Thumbnail image"
    else:
        return None


def _uri_resolve(location: str | None, path: str):
    # ODC's method doesn't support empty locations. Fall back to the path alone.
    if not location:
        return path

    return dc_uris.uri_resolve(location, path)


def _stac_links(
    dataset: Dataset,
    stac_url: str | None,
    self_url: str | None,
    collection_url: str | None,
) -> Generator[Any, Any, Any]:
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
        warnings.warn("No collection provided for Stac Item.")


def _as_stac_instruments(value: str):
    """
    >>> _as_stac_instruments("TM")
    ['tm']
    >>> _as_stac_instruments("OLI")
    ['oli']
    >>> _as_stac_instruments("ETM+")
    ['etm']
    >>> _as_stac_instruments("OLI_TIRS")
    ['oli', 'tirs']
    """
    return [i.strip("+-").lower() for i in value.split("_")]


def _convert_value_to_stac_type(key: str, value):
    """
    Convert return type as per STAC specification
    """
    # In STAC spec, "instruments" have [String] type
    if key == "eo:instrument":
        return _as_stac_instruments(value)
    # Convert the non-default datetimes to a string
    elif isinstance(value, datetime.datetime) and key != "datetime":
        return datetime_to_str(value)
    else:
        return value


def eo3_to_stac_properties(dataset: Dataset, title: str | None = None) -> dict:
    """
    Convert EO3 properties dictionary to the Stac equivalent.
    """
    # Put the title at the top for document readability.
    properties = {"title": title} if title else {}

    properties.update(
        {
            EO3_TO_STAC_RENAMES.get(key, key): _convert_value_to_stac_type(key, val)
            for key, val in dataset.properties.items()
        },
    )

    return properties


def ds2stac(
    dataset: Dataset,
    stac_url: str | None = None,
    self_url: str | None = None,
    collection_url: str | None = None,
) -> Item:
    if dataset.extent is not None:
        wgs84_geometry = dataset.extent.to_crs("EPSG:4326", math.inf)

        geometry = wgs84_geometry.json
        bbox = wgs84_geometry.boundingbox.bbox
    else:
        geometry = None
        bbox = None

    properties = eo3_to_stac_properties(dataset, title=dataset.metadata.label)
    properties.update(_lineage_fields(dataset))

    dt = properties.get("datetime")

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
        if dataset.crs is None:
            raise STACError("Projection extension requires a crs.")
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
                proj = ProjectionExtension.ext(asset)
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
