# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from uuid import UUID
import re

import affine
import attr
from ruamel.yaml.comments import CommentedMap
from shapely.geometry.base import BaseGeometry

from .properties import Eo3Dict, Eo3Interface
from datacube.model import Dataset

DEA_URI_PREFIX = "https://collections.dea.ga.gov.au"
ODC_DATASET_SCHEMA_URL = "https://schemas.opendatacube.org/dataset"

# Either a local filesystem path or a string URI.
# (the URI can use any scheme supported by rasterio, such as tar:// or https:// or ...)
Location = Union[Path, str]


@attr.s(auto_attribs=True, slots=True)
class ProductDoc:
    """
    The product that this dataset belongs to.

    "name" is the local name in ODC.

    href is intended as a more global unique "identifier" uri for the product.
    """

    name: str = None
    href: str = None


@attr.s(auto_attribs=True, slots=True, hash=True)
class GridDoc:
    """The grid describing a measurement/band's pixels"""

    shape: Tuple[int, int]
    transform: affine.Affine


@attr.s(auto_attribs=True, slots=True)
class MeasurementDoc:
    """
    A Dataset's reference to a measurement file.
    """

    path: str
    band: Optional[int] = 1
    layer: Optional[str] = None
    grid: str = "default"

    name: str = attr.ib(metadata=dict(doc_exclude=True), default=None)
    alias: str = attr.ib(metadata=dict(doc_exclude=True), default=None)


@attr.s(auto_attribs=True, slots=True)
class AccessoryDoc:
    """
    An accessory is an extra file included in the dataset that is not
    a measurement/band.

    For example: thumbnails, alternative metadata documents, or checksum files.
    """

    path: str
    type: str = None
    name: str = attr.ib(metadata=dict(doc_exclude=True), default=None)


@attr.s(auto_attribs=True, slots=True)
class DatasetDoc(Eo3Interface):
    """
    An EO3 dataset document

    Includes :class:`.Eo3Interface` methods for metadata access::

        >>> from dateutil.tz import tzutc
        >>> p = DatasetDoc()
        >>> p.platform = 'LANDSAT_8'
        >>> p.processed = '2018-04-03'
        >>> p.properties['odc:processing_datetime']
        datetime.datetime(2018, 4, 3, 0, 0, tzinfo=tzutc())

    """

    #: Dataset UUID
    id: UUID = None
    #: Human-readable identifier for the dataset
    label: str = None
    #: The product name (local) and/or url (global)
    product: ProductDoc = None
    #: Location(s) where this dataset is stored.
    #:
    #: (ODC supports multiple locations when the same dataset is stored in multiple places)
    #:
    #: They are fully qualified URIs (``file://...`, ``https://...``, ``s3://...``)
    #:
    #: All other paths in the document (measurements, accessories) are relative to the
    #: chosen location.
    locations: List[str] = None

    #: CRS string. Eg. ``epsg:3577``
    crs: str = None
    #: Shapely geometry of the valid data coverage
    #:
    #: (it must contain all non-empty pixels of the image)
    geometry: BaseGeometry = None
    #: Grid specifications for measurements
    grids: Dict[str, GridDoc] = None
    #: Raw properties
    properties: Eo3Dict = attr.ib(factory=Eo3Dict)
    #: Loadable measurements of the dataset
    measurements: Dict[str, MeasurementDoc] = None
    #: References to accessory files
    #:
    #: Such as thumbnails, checksums, other kinds of metadata files.
    #:
    #: (any files included in the dataset that are not measurements)
    accessories: Dict[str, AccessoryDoc] = attr.ib(factory=CommentedMap)
    #: Links to source dataset uuids
    lineage: Dict[str, List[UUID]] = attr.ib(factory=CommentedMap)


# Conversion functions copied from datacube-alchemist for ease of transition

def dataset_to_datasetdoc(ds: Dataset) -> DatasetDoc:
    """
    Convert to the DatasetDoc format that eodatasets expects.
    """
    if ds.metadata_type.name in {"eo_plus", "eo_s2_nrt", "gqa_eo"}:
        # Handle S2 NRT metadata identically to eo_plus files.
        # gqa_eo is the S2 ARD with extra quality check fields.
        return _convert_eo_plus(ds)

    if ds.metadata_type.name == "eo":
        return _convert_eo(ds)

    # Else we have an already mostly eo3 style dataset
    product = ProductDoc(name=ds.type.name)
    # Wrap properties to avoid typos and the like
    properties = Eo3Dict(ds.metadata_doc.get("properties", {}))
    if properties.get("eo:gsd"):
        del properties["eo:gsd"]
    return DatasetDoc(
        id=ds.id,
        product=product,
        crs=str(ds.crs),
        properties=properties,
        geometry=ds.extent,
    )


def _convert_eo_plus(ds) -> DatasetDoc:
    # Definitely need: # - 'datetime' # - 'eo:instrument' # - 'eo:platform' # - 'odc:region_code'
    region_code = _guess_region_code(ds)
    properties = Eo3Dict(
        {
            "odc:region_code": region_code,
            "datetime": ds.center_time,
            "eo:instrument": ds.metadata.instrument,
            "eo:platform": ds.metadata.platform,
            "landsat:landsat_scene_id": ds.metadata_doc.get(
                "tile_id", "??"
            ),  # Used to find abbreviated instrument id
            "sentinel:sentinel_tile_id": ds.metadata_doc.get("tile_id", "??"),
        }
    )
    product = ProductDoc(name=ds.type.name)
    return DatasetDoc(id=ds.id, product=product, crs=str(ds.crs), properties=properties)


def _convert_eo(ds) -> DatasetDoc:
    # Definitely need: # - 'datetime' # - 'eo:instrument' # - 'eo:platform' # - 'odc:region_code'
    region_code = _guess_region_code(ds)
    properties = Eo3Dict(
        {
            "odc:region_code": region_code,
            "datetime": ds.center_time,
            "eo:instrument": ds.metadata.instrument,
            "eo:platform": ds.metadata.platform,
            "landsat:landsat_scene_id": ds.metadata.instrument,  # Used to find abbreviated instrument id
        }
    )
    product = ProductDoc(name=ds.type.name)
    return DatasetDoc(id=ds.id, product=product, crs=str(ds.crs), properties=properties)


# Regex for extracting region codes from tile IDs.
RE_TILE_REGION_CODE = re.compile(r".*A\d{6}_T(\w{5})_N\d{2}\.\d{2}")


def _guess_region_code(ds: Dataset) -> str:
    """
    Get the region code of a dataset.
    """
    try:
        # EO plus
        return ds.metadata.region_code
    except AttributeError:
        # Not EO plus
        pass

    try:
        # EO
        return ds.metadata_doc["region_code"]
    except KeyError:
        # No region code!
        pass

    # Region code not specified, so get it from the tile ID.
    # An example of such a tile ID for S2A NRT is:
    # S2A_OPER_MSI_L1C_TL_VGS1_20201114T053541_A028185_T50JPP_N02.09
    # The region code is 50JPP.
    tile_match = RE_TILE_REGION_CODE.match(ds.metadata_doc["tile_id"])
    if not tile_match:
        raise ValueError("No region code for dataset {}".format(ds.id))
    return tile_match.group(1)
