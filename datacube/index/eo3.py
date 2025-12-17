# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
# TODO: type hints need attention
"""Tools for working with EO3 metadata"""

from collections.abc import Iterable, Mapping
from functools import reduce
from typing import Any, cast
from uuid import UUID

from affine import Affine
from odc.geo import (
    CRS,
    BoundingBox,
    CoordList,
    Geometry,
    SomeCRS,
    xy_,
)
from odc.geo.crs import norm_crs
from odc.geo.geobox import GeoBox
from odc.geo.geom import lonlat_bounds, multipolygon, polygon
from odc.stac._mdtools import _group_geoboxes
from toolz.dicttoolz import get_in

from datacube.model import Dataset, Product, Range
from datacube.storage import BandInfo
from datacube.storage._rio import RasterDatasetDataSource
from datacube.utils import DatacubeException, DocReader

EO3_SCHEMA = "https://schemas.opendatacube.org/dataset"


class EO3Grid:
    def __init__(self, grid: dict[str, Any]) -> None:
        shape = grid.get("shape")
        if shape is None:
            raise ValueError("Each grid must have a shape")
        if len(shape) != 2:
            raise ValueError("Grid shape must be two dimensional")
        self.shape = cast(tuple[int, int], tuple(int(x) for x in shape))
        xform = grid.get("transform")
        if xform is None:
            raise ValueError("Each grid must have a transform")
        if len(xform) != 6 and len(xform) != 9:
            raise ValueError("Grid transform must have 6 or 9 elements.")
        for elem in xform:
            if type(elem) not in (int, float):
                raise ValueError("All grid transform elements must be numbers")
        if len(xform) == 9 and list(xform[6:]) != [0, 0, 1]:
            raise ValueError("Grid transform must be a valid Affine matrix")
        self.transform = Affine(*xform[:6])

    def points(self, ring: bool = False) -> CoordList:
        ny, nx = (float(dim) for dim in self.shape)
        pts = [(0.0, 0.0), (nx, 0.0), (nx, ny), (0.0, ny)]
        if ring:
            pts += pts[:1]
        return [self.transform * pt for pt in pts]

    def ref_points(self) -> dict[str, dict[str, float]]:
        nn = ["ul", "ur", "lr", "ll"]
        return {n: {"x": x, "y": y} for n, (x, y) in zip(nn, self.points())}

    def polygon(self, crs: SomeCRS | None = None) -> Geometry:
        return polygon(self.points(ring=True), crs=crs)


def eo3_lonlat_bbox(
    grids: Iterable[EO3Grid],
    crs: CRS,
    valid_data: Geometry | None = None,
    resolution: float | None = None,
) -> BoundingBox:
    """Compute bounding box for all grids in Lon/Lat"""
    if valid_data is not None:
        return lonlat_bounds(valid_data, resolution=resolution)

    all_grids_extent = reduce(
        lambda x, y: x.union(y), (grid.polygon(crs) for grid in grids)
    )
    return lonlat_bounds(all_grids_extent, resolution=resolution)


def eo3_grid_spatial(
    doc: Mapping[str, Any], resolution: float | None = None, grid_name: str = "default"
) -> dict[str, Any]:
    """Using doc[grids|crs|geometry] compute EO3 style grid spatial:

    Note that `geo_ref_points` are set to the 4 corners of the default grid
    only, while lon/lat bounds are computed using all the grids, unless tighter
    valid region is defined via `geometry` key, in which case it is used to
    determine lon/lat bounds instead.
    Uses the default grid.

    inputs:

    ```
    crs: "<:str>"
    geometry: <:GeoJSON object>  # optional
    grids:
       default:
          shape: [ny: int, nx: int]
          transform: [a0, a1, a2, a3, a4, a5, 0, 0, 1]
       <...> # optionally more grids
    ```

    Where transform is a linear mapping matrix from pixel space to projected
    space encoded in row-major order:

       [X]   [a0, a1, a2] [ Pixel]
       [Y] = [a3, a4, a5] [ Line ]
       [1]   [ 0,  0,  1] [  1   ]

    outputs:
    ```
      extent:
        lat: {begin=<>, end=<>}
        lon: {begin=<>, end=<>}

      grid_spatial:
        projection:
          spatial_reference: "<crs>"
          geo_ref_points: {ll: {x:<>, y:<>}, ...}
          valid_data: {...}
    ```
    """
    gridspecs = doc.get("grids", {})
    crs = doc.get("crs")
    if crs is None or not gridspecs:
        raise ValueError("Input must have crs and grids.")
    grids = {name: EO3Grid(grid_spec) for name, grid_spec in gridspecs.items()}
    grid = grids.get(grid_name)
    if not grid:
        raise ValueError(f"Input must have grids.{grid_name}")

    geometry = doc.get("geometry")
    if geometry is not None:
        valid_data: dict[str, Any] = {"valid_data": geometry}
        if geometry["type"] == "Polygon":
            valid_geom: Geometry | None = polygon(
                valid_data["valid_data"]["coordinates"][0], crs=crs
            )
        elif geometry["type"] == "MultiPolygon":
            valid_geom = multipolygon(geometry["coordinates"], crs=crs)
        else:
            raise ValueError(f"Unknown geometry type {geometry['type']}")
    else:
        valid_data = {"valid_data": grid.polygon().json}
        valid_geom = None

    oo = {
        "grid_spatial": {
            "projection": {
                "spatial_reference": crs,
                "geo_ref_points": grid.ref_points(),
                **valid_data,
            }
        }
    }

    x1, y1, x2, y2 = eo3_lonlat_bbox(
        grids.values(), crs, valid_data=valid_geom, resolution=resolution
    )
    oo["extent"] = {"lon": {"begin": x1, "end": x2}, "lat": {"begin": y1, "end": y2}}
    return oo


def add_eo3_parts(
    doc: Mapping[str, Any], resolution: float | None = None
) -> dict[str, Any]:
    """Add spatial keys the DB requires to eo3 metadata"""
    # Clone and update to ensure idempotency
    out = dict(**doc)
    out.update(eo3_grid_spatial(doc, resolution=resolution))
    return out


def is_doc_eo3(doc: Mapping[str, Any]) -> bool:
    """Is this document eo3?

    :param doc: Parsed ODC Dataset metadata document

    :returns:
        False if this document is a legacy dataset
        True if this document is eo3

    :raises ValueError: For an unsupported document
    """
    schema = doc.get("$schema")
    # All legacy documents had no schema at all.
    if schema is None:
        return False

    if schema == EO3_SCHEMA:
        return True

    # Otherwise it has an unknown schema.
    #
    # Reject it for now.
    # We don't want future documents (like Stac items, or "eo4") to be quietly
    # accepted as legacy eo.
    raise ValueError(f"Unsupported dataset schema: {schema!r}")


def is_doc_geo(doc: Mapping[str, Any], check_eo3: bool = True) -> bool:
    """Is this document geospatial?

    :param doc: Parsed ODC Dataset metadata document
    :param check_eo3: Set to false to skip the EO3 check and assume doc isn't EO3.

    :returns:
        True if this document specifies geospatial dimensions
        False if this document does not specify geospatial dimensions (e.g. telemetry only)

    :raises ValueError: For an unsupported document
    """
    # EO3 is geospatial
    if check_eo3 and is_doc_eo3(doc):
        return True
    # Does this cover EO legacy datasets ok? at all??
    return "extent" in doc or "grid_spatial" in doc


def prep_eo3(
    doc: dict[str, Any],
    auto_skip: bool = False,
    resolution: float | None = None,
    remap_lineage: bool = True,
) -> dict[str, Any]:
    """Modify spatial and lineage sections of eo3 metadata

    Should be idempotent:  prep_eo3(doc, **kwargs) == prep_eo3(prep_eo3(doc, **kwargs), **kwargs)

    :param doc: input document
    :param auto_skip: If true check if dataset is EO3 and if not
                      silently return input dataset without modifications
    :param remap_lineage: If True (default) disambiguate lineage classifiers so that
                          source_id and classifier form a unique index (for indexes that DON'T
                          support external_lineage).
                          If False, leave lineage in the same format.
    """
    if doc is None:
        return None

    if auto_skip and not is_doc_eo3(doc):
        return doc

    def stringify(u: str | UUID | None) -> str | None:
        return u if isinstance(u, str) else str(u) if u else None

    doc["id"] = stringify(doc.get("id"))

    doc = add_eo3_parts(doc, resolution=resolution)
    if remap_lineage:
        lineage = doc.pop("lineage", {})
        if isinstance(lineage, dict) and "source_datasets" in lineage:
            # Is already in pseudo-embedded rewritten form - keep as is.
            doc["lineage"] = lineage
        else:

            def lineage_remap(name: str, uuids) -> dict[str, Any]:
                """Turn name, [uuid] -> {name: {id: uuid}}"""
                if len(uuids) == 0:
                    return {}
                if isinstance(uuids, dict) or isinstance(uuids[0], dict):
                    raise ValueError(
                        "Embedded lineage not supported for eo3 metadata types"
                    )
                if len(uuids) == 1:
                    if isinstance(uuids[0], dict):
                        return {name: uuids}
                    return {name: {"id": stringify(uuids[0])}}

                out: dict[str, Any] = {}
                for idx, uuid in enumerate(uuids, start=1):
                    if isinstance(uuids, dict):
                        out[name] = uuid
                    else:
                        out[name + str(idx)] = {"id": stringify(uuid)}
                return out

            sources = {}
            for name, uuids in lineage.items():
                sources.update(lineage_remap(name, uuids))

            doc["lineage"] = {"source_datasets": sources}
    return doc


class EOConversionError(DatacubeException):
    """An ODC Exception raised when unable to convert a legacy EO dataset to EO3"""


class EOGridsError(EOConversionError):
    """An exception raised when unable to determine grids from an EO dataset"""


def _accessories_from_eo1(metadata_doc: dict) -> dict[str, Any]:
    """Create an EO3 accessories section from an EO1 document"""
    accessories = {}

    # Browse image -> thumbnail
    if "browse" in metadata_doc:
        for name, browse in metadata_doc["browse"].items():
            accessories[f"thumbnail:{name}"] = {"path": browse["path"]}

    # Checksum
    if "checksum_path" in metadata_doc:
        accessories["checksum:sha1"] = {"path": metadata_doc["checksum_path"]}
    return accessories


def field_label(key, value):
    yield "title", value


def field_platform(key, value):
    yield "eo:platform", value.lower().replace("_", "-")


def field_instrument(key, value):
    yield "eo:instrument", value


def field_format(key, value):
    yield "odc:file_format", value


def field_product(key, value):
    yield "odc:product_family", value


def field_path_row(key, value):
    # Path/Row fields are ranges in datacube but 99% of the time
    # they are a single value
    # (they are ranges in telemetry products)
    # Stac doesn't accept a range here, so we'll skip it in those products,
    # but we can handle the 99% case when lower==higher.
    if key == "sat_path":
        kind = "landsat:wrs_path"
    elif key == "sat_row":
        kind = "landsat:wrs_row"
    else:
        raise ValueError(f"Path/row kind {key!r}")

    # If there's only one value in the range, return it.
    if isinstance(value, Range):
        if value.end is None or value.begin == value.end:
            # Standard stac
            yield kind, int(value.begin)
        else:
            # Our questionable output. Only present in telemetry products?
            yield f"odc:{key}", [value.begin, value.end]


# Other Property examples:
# collection  "landsat-8-l1"
# eo:gsd  15
# eo:platform "landsat-8"
# eo:instrument "OLI_TIRS"
# eo:off_nadir  0
# datetime  "2019-02-12T19:26:08.449265+00:00"
# eo:sun_azimuth  -172.29462212
# eo:sun_elevation  -6.62176054
# eo:cloud_cover  -1
# eo:row  "135"
# eo:column "044"
# landsat:product_id  "LC08_L1GT_044135_20190212_20190212_01_RT"
# landsat:scene_id  "LC80441352019043LGN00"
# landsat:processing_level  "L1GT"
# landsat:tier  "RT"

_EO1_PROPERTY_MAP = {
    "platform": field_platform,
    "instrument": field_instrument,
    "sat_path": field_path_row,
    "sat_row": field_path_row,
    "label": field_label,
    "format": field_format,
    "product_type": field_product,
}


def _build_properties(d: DocReader):
    for key, val in d.fields.items():
        if val is None:
            continue
        converter = _EO1_PROPERTY_MAP.get(key)
        if converter:
            yield from converter(key, val)


def make_grids(
    ds: Dataset, open_datafiles: bool
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """
    Determine grid values if possible from geo_ref_points and cell_size/shape values.
    If measurements do not contain the necessary information, see if it can be found in
    browse or else the product definition.
    If open_datafile is True, open the measurement datafiles and read the information from there.
    This will be slow and should only be used as a last resort.
    Return grids dict and (optionally) mapping of measurement name to grid name.
    """
    geoboxes = {}

    def _shape_and_transform(
        ref_points: dict,
        res: int | float | dict | None,
        shape: dict | None,
        adjust: bool = False,
    ) -> tuple[tuple[int, int], Affine] | tuple[None, None]:
        """Calculate shape (y,x) and transform, or return None if they cannot be determined"""
        if res:
            if isinstance(res, int | float):
                res_x, res_y = res, -res
            else:
                # y value is not always negative even when we are dealing with a square resolution
                res_x, res_y = res["x"], -res["y"] if res["y"] == res["x"] else res["y"]
            transform = Affine(
                res_x, 0.0, ref_points["ul"]["x"], 0.0, res_y, ref_points["ul"]["y"]
            )
            shape_x, shape_y = ~transform * (
                ref_points["lr"]["x"],
                ref_points["lr"]["y"],
            )
        elif shape:
            # the browse.full.shape values generally seem to be accurate but off by 1, so adjust accordingly
            # this logic has been extrapolated from a very small sample size and may be removed in the future
            shape_x, shape_y = (
                (shape["x"] - 1, shape["y"] - 1) if adjust else (shape["x"], shape["y"])
            )
            if not res:
                assert ds.transform is not None
                transform = ds.transform * Affine.scale(1 / shape_x, 1 / shape_y)
        else:
            return None, None
        return (int(shape_y), int(shape_x)), transform

    # We handle open_datafiles first since it should only be set when it's absolutely necessary
    if open_datafiles:
        bands = {n: BandInfo(ds, n) for n in list(ds.product.measurements)}
        for name, band in bands.items():
            source = RasterDatasetDataSource(band)
            with source.open() as rdr:
                geoboxes[name] = GeoBox(rdr.shape, rdr.transform, CRS(str(rdr.crs)))
    else:
        assert ds._gs is not None
        ref_points = ds._gs["geo_ref_points"]

        for name, m in ds.measurements.items():
            shape, transform = _shape_and_transform(
                ref_points, m.get("cell_size"), m.get("shape")
            )
            if shape is not None:
                assert transform is not None
                geoboxes[name] = GeoBox(shape, transform, ds.crs)

        # no grid info in measurements, see if we can get a default from browse or product
        if geoboxes == {}:
            full = get_in(["browse", "full"], ds.metadata_doc, default={})
            if "cell_size" in full or "shape" in full:
                shape, transform = _shape_and_transform(
                    ref_points, full.get("cell_size"), full.get("shape"), True
                )
            elif ds.product.default_resolution:
                shape, transform = _shape_and_transform(
                    ref_points,
                    {
                        "x": ds.product.default_resolution.x,
                        "y": ds.product.default_resolution.y,
                    },
                    None,
                )
            elif ds.product.grid_spec:
                # assumes we'll get a legacy GridSpec since we're dealing with legacy datasets
                shape, transform = _shape_and_transform(
                    ref_points,
                    {
                        "x": ds.product.grid_spec.resolution[1],
                        "y": ds.product.grid_spec.resolution[0],
                    },
                    None,
                )
            else:
                raise EOGridsError(
                    "Unable to retrieve resolution or shape values necessary to calculate the dataset grids for "
                    f"dataset {ds.id} \n"
                    "You may want to try again with open_datafiles=True to retrieve the information from the band "
                    "files, but be warned that it may be very slow.",
                )
            return {"default": {"shape": shape, "transform": transform[:6]}}, {}  # type: ignore[index]

    named_gboxes, band2grid = _group_geoboxes(geoboxes)
    grids = {
        name: {"shape": gbox.shape.yx, "transform": gbox.transform[:6]}  # type: ignore[index]
        for name, gbox in named_gboxes.items()
    }
    return grids, band2grid


def convert_bands(
    ds: Dataset, grid_mappings: dict[str, str]
) -> dict[str, dict[str, str]]:
    def _to_measurement(name, band):
        m = {"path": band.get("path", "")}
        if grid_mappings.get(name, "default") != "default":
            m["grid"] = grid_mappings[name]
        if "label" in band:
            m["aliases"] = [band.get("label")]
        # others?
        return m

    return {name: _to_measurement(name, band) for name, band in ds.measurements.items()}


def convert_eo_dataset(eo_ds: Dataset, open_datafiles: bool = False) -> Dataset:
    """
    Convert an EO Dataset to EO3.
    The metadata fields are restructured and renamed, and grids are calculated to align with
    the expected structure of an EO3 metadata doc. The Product is also converted to EO3.

    :param eo_ds: input dataset. If it is already EO3, it is silently returned without modifications
    :param open_datafiles: If False (default) use the dataset information to calculate grids.
                           If True, open the band files to retrieve the relevant values.
                           This approach is slow, so should only be used if known to be necessary.
    """
    if eo_ds.is_eo3:
        return eo_ds

    # we always require extent since it includes time info that cannot be extracted from the datafiles
    if "extent" not in eo_ds.metadata_doc:
        raise EOConversionError(
            f"Dataset {eo_ds.id} is missing extent and cannot be converted to EO3."
        )
    # on the other hand, grid_spatial is not required if we are retrieving info from the datafiles
    if not open_datafiles:
        if eo_ds._gs is None:
            raise EOConversionError(
                f"Dataset {eo_ds.id} is missing spatial information and cannot be converted to EO3."
            )
        if not eo_ds._gs.get("geo_ref_points"):
            raise EOConversionError(
                f"Dataset {eo_ds.id} spatial information does not include geo_ref_points, "
                "necessary for conversion to EO3."
            )
        if eo_ds.crs is None:
            if "map_projection" in eo_ds._gs:
                crs = norm_crs(
                    eo_ds._gs.get("map_projection"),
                    xy_(eo_ds.metadata.lon.begin, eo_ds.metadata.lat.end),
                )
                eo_ds.metadata_doc["grid_spatial"]["projection"][
                    "spatial_reference"
                ] = str(crs)
            else:
                raise EOConversionError(
                    f"Dataset {eo_ds.id} does not have a CRS value and cannot be converted to EO3."
                )

    grids, grid_mappings = make_grids(eo_ds, open_datafiles)

    # since grid_spatial is optional with open_datafiles but required for EO3 datasets, compute it now if needed
    if eo_ds._gs is None:
        grid = EO3Grid(grids["default"])
        crs = str(grid.polygon().crs)
        valid_data = grid.polygon().json
        grid_spatial = {
            "projection": {
                "spatial_reference": crs,
                "geo_ref_points": grid.ref_points(),
                "valid_data": valid_data,
            }
        }
    else:
        crs = str(eo_ds.crs)
        valid_data = eo_ds._gs.get("valid_data")
        grid_spatial = eo_ds.metadata_doc["grid_spatial"]

    eo3_doc = {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": str(eo_ds.id),
        "label": eo_ds.metadata.label,
        "product": {"name": eo_ds.product.name},
        "properties": {
            "datetime": get_in(
                ["extent", "center_dt"], eo_ds.metadata_doc, default=eo_ds.center_time
            ),
            "odc:processing_datetime": eo_ds.metadata.creation_dt or eo_ds.indexed_time,
            **(dict(_build_properties(eo_ds.metadata))),
        },
        "crs": crs,
        "grids": grids,
        "extent": {
            "lon": {
                "begin": eo_ds.metadata.lon.begin,
                "end": eo_ds.metadata.lon.end,
            },
            "lat": {
                "begin": eo_ds.metadata.lat.begin,
                "end": eo_ds.metadata.lat.end,
            },
        },
        "geometry": valid_data,
        "grid_spatial": grid_spatial,
        "measurements": convert_bands(eo_ds, grid_mappings),
        "accessories": _accessories_from_eo1(eo_ds.metadata_doc),
        "lineage": {
            "source_datasets": {
                name: doc.get("id") for name, doc in eo_ds.metadata.sources.items()
            }
        },
    }
    if eo_ds.time and eo_ds.time.begin != eo_ds.time.end:
        eo3_doc["properties"].update(
            {
                "dtr:start_datetime": eo_ds.time.begin.isoformat(),
                "dtr:end_datetime": eo_ds.time.end.isoformat(),
            }
        )
    location = eo_ds.metadata_doc.get("location") or eo_ds.uri
    if location:
        eo3_doc["location"] = location

    # TODO: better handling of sources/source_tree
    # method could accept an Index, or else return only eo3_doc to handle Dataset init elsewhere
    return Dataset(
        convert_eo_product(eo_ds.product), eo3_doc, uri=location, sources=eo_ds.sources
    )


def convert_eo_product(eo_product: Product) -> Product:
    """
    Convert an EO product to EO3, with the base EO3 metadata type
    """
    # import here to avoid circular import error
    # the default md types should probably find another place to live anyway
    from datacube.metadata._utils import EO3_MD_TYPE

    old_def = eo_product.definition
    metadata = old_def["metadata"]
    metadata_offsets = {
        "eo:platform": ["platform", "code"],
        "eo:instrument": ["instrument", "name"],
        "odc:file_format": ["format", "name"],
        "odc:product_family": ["product_type"],
    }
    properties = {}
    for name, offset in metadata_offsets.items():
        if (val := get_in(offset, metadata)) is not None:
            properties[name] = (
                val.lower().replace("_", "-") if name == "eo:platform" else val
            )

    new_def = {
        "name": old_def["name"],
        "license": "CC-BY-4.0",
        "metadata_type": "eo3",
        "description": old_def["description"],
        "metadata": {
            "product": {"name": old_def["name"]},
            "properties": properties,
        },
        "measurements": old_def["measurements"],
    }
    if "load" in old_def:
        new_def["load"] = old_def["load"]
    elif "storage" in old_def:
        storage = old_def["storage"]
        if "crs" in storage and "resolution" in storage:
            new_def["load"] = {
                "crs": storage["crs"],
                "resolution": storage["resolution"],
            }

    return Product(EO3_MD_TYPE, new_def)
