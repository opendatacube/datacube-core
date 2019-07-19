import itertools
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import Tuple, Dict, Optional, Iterable, List, Sequence
from uuid import UUID

import affine
import attr
import numpy
import rasterio
import rasterio.features
import shapely
import shapely.affinity
import shapely.ops
from rasterio import DatasetReader
from ruamel.yaml.comments import CommentedMap
from shapely.geometry.base import BaseGeometry

from eodatasets3 import utils
from eodatasets3.properties import StacPropertyView, EoFields

# TODO: these need discussion.
DEA_URI_PREFIX = "https://collections.dea.ga.gov.au"
ODC_DATASET_SCHEMA_URL = "https://schemas.opendatacube.org/dataset"


class FileFormat(Enum):
    GeoTIFF = 1
    NetCDF = 2


def _dea_uri(product_name, base_uri):
    return f"{base_uri}/product/{product_name}"


@attr.s(auto_attribs=True, slots=True)
class ProductDoc:
    name: str = None
    href: str = None

    @classmethod
    def dea_name(cls, name: str):
        return ProductDoc(name=name, href=_dea_uri(name, DEA_URI_PREFIX))


@attr.s(auto_attribs=True, slots=True, hash=True)
class GridDoc:
    shape: Tuple[int, int]
    transform: affine.Affine


@attr.s(auto_attribs=True, slots=True)
class MeasurementDoc:
    path: str
    band: Optional[int] = 1
    layer: Optional[str] = None
    grid: str = "default"

    name: str = attr.ib(metadata=dict(doc_exclude=True), default=None)
    alias: str = attr.ib(metadata=dict(doc_exclude=True), default=None)


@attr.s(auto_attribs=True, slots=True)
class AccessoryDoc:
    path: str
    type: str = None
    name: str = attr.ib(metadata=dict(doc_exclude=True), default=None)


class ComplicatedNamingConventions:
    """
    Naming conventions based on the DEA standard.

    Unlike the DEA standard, almost every field is optional by default.
    """

    _ABSOLUTE_MINIMAL_PROPERTIES = {
        "odc:product_family",
        # Required by Stac regardless.
        "datetime",
    }

    # Displayed to user for friendlier errors.
    _REQUIRED_PROPERTY_HINTS = {
        "odc:product_family": 'eg. "wofs" or "level1"',
        "odc:processing_datetime": "Time of processing, perhaps datetime.utcnow()?",
        "odc:producer": "Creator of data, eg 'usgs.gov' or 'ga.gov.au'",
    }

    def __init__(
        self,
        dataset: EoFields,
        base_uri: str = None,
        required_fields: Sequence[str] = (),
    ) -> None:
        self.dataset = dataset
        self.base_uri = base_uri
        self.required_fields = self._ABSOLUTE_MINIMAL_PROPERTIES.union(required_fields)

    @classmethod
    def for_standard_dea(cls, dataset: EoFields, uri=DEA_URI_PREFIX):
        """
        Strict mode to follow the full DEA naming conventions.

        Only use the (default) DEA URI if you're making DEA products.
        """
        return cls(
            dataset=dataset,
            base_uri=uri,
            # These fields are needed to fulfill official DEA naming conventions.
            required_fields=(
                # TODO: Add conventions for multi-platform/composite products?
                "eo:instrument",
                "eo:platform",
                "odc:dataset_version",
                "odc:processing_datetime",
                "odc:producer",
                "odc:product_family",
                "odc:region_code",
            ),
        )

    def _check_enough_properties_to_name(self):
        """
        Do we have enough properties to generate file or product names?
        """
        for f in self.required_fields:
            if f not in self.dataset.properties:
                raise ValueError(
                    f"Property {f!r} is required. "
                    f"{self._REQUIRED_PROPERTY_HINTS.get(f, '')}"
                )

    @property
    def product_name(self) -> str:
        self._check_enough_properties_to_name()

        org_number = self._org_collection_number
        if org_number:
            return f"{self._product_group()}_{org_number}"
        return self._product_group()

    @property
    def _org_collection_number(self) -> Optional[int]:
        if not self.dataset.dataset_version:
            return None
        return int(self.dataset.dataset_version.split(".")[0])

    def _product_group(self, subname=None):
        # Fallback to the whole product's name
        if not subname:
            subname = self.dataset.product_family

        parts = []
        if self.producer_abbreviated:
            parts.append(self.producer_abbreviated)

        platform = self.platform_abbreviated
        inst = self.instrument_abbreviated
        if platform and inst:
            parts.append(f"{platform}{inst}")

        if not subname:
            raise ValueError(
                "Not even metadata to create a useful filename! "
                'Set the `product_family` (eg. "wofs") or a subname'
            )
        parts.append(subname)

        return "_".join(parts)

    @property
    def product_uri(self) -> Optional[str]:
        self._check_enough_properties_to_name()
        if not self.base_uri:
            return None

        return _dea_uri(self.product_name, base_uri=self.base_uri)

    @property
    def dataset_label(self) -> str:
        """
        Label for a dataset
        """
        self._check_enough_properties_to_name()
        # TODO: Dataset label Configurability?
        d = self.dataset
        version = d.dataset_version.replace(".", "-")

        fs = (
            f"{self.product_name}-{version}",
            self._displayable_region_code,
            f"{d.datetime:%Y-%m-%d}",
        )

        if "dea:dataset_maturity" in d:
            fs = fs + (d.properties["dea:dataset_maturity"],)
        return "_".join(fs)

    def destination_folder(self, base: Path):
        self._check_enough_properties_to_name()
        # DEA naming conventions folder hierarchy.
        # Example: "ga_ls8c_ard_3/092/084/2016/06/28"

        parts = [self.product_name]

        # Cut the region code in subfolders
        region_code = self.dataset.region_code
        if region_code:
            parts.extend(utils.subfolderise(region_code))

        parts.extend(f"{self.dataset.datetime:%Y/%m/%d}".split("/"))

        return base.joinpath(*parts)

    def metadata_path(self, work_dir: Path, kind: str = "", suffix: str = "yaml"):
        self._check_enough_properties_to_name()
        return self._file(work_dir, kind, suffix)

    def checksum_path(self, work_dir: Path, suffix: str = "sha1"):
        self._check_enough_properties_to_name()
        return self._file(work_dir, "", suffix)

    def measurement_file_path(
        self, work_dir: Path, measurement_name: str, suffix: str, file_id: str = None
    ) -> Path:
        self._check_enough_properties_to_name()
        if ":" in measurement_name:
            subgroup, name = measurement_name.split(":")
        else:
            subgroup, name = None, measurement_name

        return self._file(
            work_dir,
            # We use 'band01'/etc in the filename if provided, rather than 'red'
            file_id or name,
            suffix,
            sub_name=subgroup,
        )

    def _file(self, work_dir: Path, file_id: str, suffix: str, sub_name: str = None):
        p = self.dataset
        if p.dataset_version:
            version = p.dataset_version.replace(".", "-")
        else:
            version = "beta"

        maturity = p.properties.get("dea:dataset_maturity") or "user"
        if file_id:
            end = f'{maturity}_{file_id.replace("_", "-")}.{suffix}'
        else:
            end = f"{maturity}.{suffix}"

        return work_dir / "_".join(
            (
                self._product_group(sub_name),
                version,
                self._displayable_region_code,
                f"{p.datetime:%Y-%m-%d}",
                end,
            )
        )

    @property
    def _displayable_region_code(self):
        return self.dataset.region_code or "x"

    def thumbnail_name(self, work_dir: Path, kind: str = None, suffix: str = "jpg"):
        self._check_enough_properties_to_name()
        if kind:
            name = f"{kind}:thumbnail"
        else:
            name = "thumbnail"
        return self.measurement_file_path(work_dir, name, suffix)

    @property
    def platform_abbreviated(self) -> Optional[str]:
        """Abbreviated form of a satellite, as used in dea product names. eg. 'ls7'."""
        p = self.dataset.platform
        if not p:
            return None

        if not p.startswith("landsat"):
            raise NotImplementedError(
                f"TODO: implement non-landsat platform abbreviation " f"(got {p!r})"
            )

        return f"ls{p[-1]}"

    @property
    def instrument_abbreviated(self) -> Optional[str]:
        """Abbreviated form of an instrument name, as used in dea product names. eg. 'c'."""
        p = self.dataset.platform
        if not p:
            return None
        if not p.startswith("landsat"):
            raise NotImplementedError(
                f"TODO: implement non-landsat instrument abbreviation " f"(got {p!r})"
            )

        # Extract from usgs standard:
        # landsat:landsat_product_id: LC08_L1TP_091075_20161213_20170316_01_T2
        # landsat:landsat_scene_id: LC80910752016348LGN01
        landsat_id: str = self.dataset.properties.get(
            "landsat:landsat_product_id"
        ) or self.dataset.properties.get("landsat:landsat_scene_id")
        if not landsat_id:
            raise NotImplementedError(
                f"TODO: Can only currently abbreviate instruments from landsat references."
            )

        return landsat_id[1].lower()

    @property
    def producer_abbreviated(self) -> Optional[str]:
        """Abbreviated form of a satellite, as used in dea product names. eg. 'ls7'."""
        if not self.dataset.producer:
            return None
        producer_domains = {"ga.gov.au": "ga", "usgs.gov": "usgs"}
        try:
            return producer_domains[self.dataset.producer]
        except KeyError:
            raise NotImplementedError(
                f"TODO: cannot yet abbreviate organisation domain name {self.dataset.producer!r}"
            )


@attr.s(auto_attribs=True, slots=True)
class DatasetDoc(EoFields):
    id: UUID = None
    product: ProductDoc = None
    locations: List[str] = None

    crs: str = None
    geometry: BaseGeometry = None
    grids: Dict[str, GridDoc] = None

    properties: StacPropertyView = attr.ib(factory=StacPropertyView)

    measurements: Dict[str, MeasurementDoc] = None

    # Paths to accessory files, such as thumbnails.
    accessories: Dict[str, AccessoryDoc] = attr.ib(factory=CommentedMap)

    lineage: Dict[str, Sequence[UUID]] = attr.ib(factory=CommentedMap)


def resolve_absolute_offset(
    dataset_path: Path, offset: str, target_path: Optional[Path] = None
) -> str:
    """
    Expand a filename (offset) relative to the dataset.

    >>> external_metadata_loc = Path('/tmp/target-metadata.yaml')
    >>> resolve_absolute_offset(
    ...     Path('/tmp/great_test_dataset'),
    ...     'band/my_great_band.jpg',
    ...     external_metadata_loc,
    ... )
    '/tmp/great_test_dataset/band/my_great_band.jpg'
    >>> resolve_absolute_offset(
    ...     Path('/tmp/great_test_dataset.tar.gz'),
    ...     'band/my_great_band.jpg',
    ...     external_metadata_loc,
    ... )
    'tar:/tmp/great_test_dataset.tar.gz!band/my_great_band.jpg'
    >>> resolve_absolute_offset(
    ...     Path('/tmp/great_test_dataset.tar'),
    ...     'band/my_great_band.jpg',
    ... )
    'tar:/tmp/great_test_dataset.tar!band/my_great_band.jpg'
    >>> resolve_absolute_offset(
    ...     Path('/tmp/MY_DATASET'),
    ...     'band/my_great_band.jpg',
    ...     Path('/tmp/MY_DATASET/ga-metadata.yaml'),
    ... )
    'band/my_great_band.jpg'
    """
    dataset_path = dataset_path.absolute()

    if target_path:
        # If metadata is stored inside the dataset, keep paths relative.
        if str(target_path.absolute()).startswith(str(dataset_path)):
            return offset
    # Bands are inside a tar file

    if ".tar" in dataset_path.suffixes:
        return "tar:{}!{}".format(dataset_path, offset)
    else:
        return str(dataset_path / offset)


class Intern(dict):
    def __missing__(self, key):
        self[key] = key
        return key


def valid_region(
    path: Path, measurements: Iterable[MeasurementDoc], mask_value=None
) -> Tuple[BaseGeometry, Dict[str, GridDoc]]:
    mask = None

    if not measurements:
        raise ValueError("No measurements: cannot calculate valid region")

    measurements_by_grid: Dict[GridDoc, List[MeasurementDoc]] = defaultdict(list)
    mask_by_grid: Dict[GridDoc, numpy.ndarray] = {}

    for measurement in measurements:
        measurement_path = resolve_absolute_offset(path, measurement.path)
        with rasterio.open(str(measurement_path), "r") as ds:
            ds: DatasetReader
            transform: affine.Affine = ds.transform

            if not len(ds.indexes) == 1:
                raise NotImplementedError(
                    f"Only single-band tifs currently supported. File {measurement_path!r}"
                )
            img = ds.read(1)
            grid = GridDoc(shape=ds.shape, transform=transform)
            measurements_by_grid[grid].append(measurement)

            if mask_value is not None:
                new_mask = img & mask_value == mask_value
            else:
                new_mask = img != ds.nodata

            mask = mask_by_grid.get(grid)
            if mask is None:
                mask = new_mask
            else:
                mask |= new_mask
            mask_by_grid[grid] = mask

    grids_by_frequency: List[Tuple[GridDoc, List[MeasurementDoc]]] = sorted(
        measurements_by_grid.items(), key=lambda k: len(k[1])
    )

    def name_grid(grid, measurements: List[MeasurementDoc], name=None):
        name = name or "_".join(m.alias or m.name for m in measurements)
        for m in measurements:
            m.grid = name

        return name, grid

    grids = dict(
        [
            # most frequent is called "default", others use band names.
            name_grid(*(grids_by_frequency[-1]), name="default"),
            *(name_grid(*g) for g in grids_by_frequency[:-1]),
        ]
    )

    shapes = itertools.chain(
        *[
            rasterio.features.shapes(mask.astype("uint8"), mask=mask)
            for mask in mask_by_grid.values()
        ]
    )
    shape = shapely.ops.unary_union(
        [shapely.geometry.shape(shape) for shape, val in shapes if val == 1]
    )

    # convex hull
    geom = shape.convex_hull

    # buffer by 1 pixel
    geom = geom.buffer(1, join_style=3, cap_style=3)

    # simplify with 1 pixel radius
    geom = geom.simplify(1)

    # intersect with image bounding box
    geom = geom.intersection(shapely.geometry.box(0, 0, mask.shape[1], mask.shape[0]))

    # transform from pixel space into CRS space
    geom = shapely.affinity.affine_transform(
        geom,
        (
            transform.a,
            transform.b,
            transform.d,
            transform.e,
            transform.xoff,
            transform.yoff,
        ),
    )
    # output = shapely.geometry.mapping(geom)
    return geom, grids
