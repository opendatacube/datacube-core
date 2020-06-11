from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Tuple, Dict, Optional, List, Sequence, Union
from uuid import UUID

import affine
import attr
from eodatasets3 import utils
from eodatasets3.properties import StacPropertyView, EoFields
from ruamel.yaml.comments import CommentedMap
from shapely.geometry.base import BaseGeometry

# TODO: these need discussion.
DEA_URI_PREFIX = "https://collections.dea.ga.gov.au"
ODC_DATASET_SCHEMA_URL = "https://schemas.opendatacube.org/dataset"


class FileFormat(Enum):
    GeoTIFF = 1
    NetCDF = 2


# Either a local filesystem path or a string URI.
# (the URI can use any scheme supported by rasterio, such as tar:// or https:// or ...)
Location = Union[Path, str]


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
        "odc:dataset_version": "eg. 1.0.0",
    }

    def __init__(
        self,
        dataset: EoFields,
        base_product_uri: str = None,
        required_fields: Sequence[str] = (),
        dataset_separator_field: Optional[str] = None,
    ) -> None:
        self.dataset = dataset
        self.base_product_uri = base_product_uri
        self.required_fields = self._ABSOLUTE_MINIMAL_PROPERTIES.union(required_fields)

        # An extra folder to put each dataset inside, using the value of the given property name.
        self.dataset_separator_field = dataset_separator_field

        if self.dataset_separator_field is not None:
            self.required_fields.add(dataset_separator_field)

    @classmethod
    def for_standard_dea(cls, dataset: EoFields, uri=DEA_URI_PREFIX):
        """
        Strict mode to follow the full DEA naming conventions.

        Only use the (default) DEA URI if you're making DEA products.
        """
        return cls(
            dataset=dataset,
            base_product_uri=uri,
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

    @classmethod
    def for_standard_dea_s2(cls, dataset: EoFields, uri=DEA_URI_PREFIX):
        """
        DEA naming conventions, but with an extra subfolder for each unique datatake.

        It will figure out the datatake if you set a sentinel_tile_id.
        """
        return cls(
            dataset=dataset,
            base_product_uri=uri,
            # These fields are needed to fulfill official DEA naming conventions.
            required_fields=(
                "eo:instrument",
                "eo:platform",
                "odc:dataset_version",
                "odc:processing_datetime",
                "odc:producer",
                "odc:product_family",
                "odc:region_code",
                "sentinel:sentinel_tile_id",
            ),
            dataset_separator_field="sentinel:datatake_start_datetime",
        )

    def _check_enough_properties_to_name(self):
        """
        Do we have enough properties to generate file or product names?
        """
        missing_props = []
        for f in self.required_fields:
            if f not in self.dataset.properties:
                missing_props.append(f)
        if missing_props:
            examples = []
            for p in sorted(missing_props):
                hint = self._REQUIRED_PROPERTY_HINTS.get(p, "")
                if hint:
                    hint = f" ({hint})"
                examples.append(f"\n- {p!r}{hint}")

            raise ValueError(
                f"Need more properties to fulfill naming conventions."
                f"{''.join(examples)}"
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

    def _product_group(self, subname=None) -> str:
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
        if not self.base_product_uri:
            return None

        return _dea_uri(self.product_name, base_uri=self.base_product_uri)

    @property
    def dataset_label(self) -> str:
        """
        Label for a dataset
        """
        self._check_enough_properties_to_name()
        return self._dataset_label()

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

        if self.dataset_separator_field is not None:
            val = self.dataset.properties[self.dataset_separator_field]
            # TODO: choosable formatter?
            if isinstance(val, datetime):
                val = f"{val:%Y%m%dT%H%M%S}"
            parts.append(val)
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

    def _dataset_label(self, sub_name: str = None):
        p = self.dataset

        version = p.dataset_version.replace(".", "-") if p.dataset_version else None
        maturity: str = p.properties.get("dea:dataset_maturity")
        return "_".join(
            [
                p
                for p in (
                    self._product_group(sub_name),
                    version,
                    self._displayable_region_code,
                    f"{p.datetime:%Y-%m-%d}",
                    maturity,
                )
                if p
            ]
        )

    def _file(self, work_dir: Path, file_id: str, suffix: str, sub_name: str = None):
        file_id = "_" + file_id.replace("_", "-") if file_id else ""

        return work_dir / (
            f"{self._dataset_label(sub_name=sub_name)}{file_id}.{suffix}"
        )

    @property
    def _displayable_region_code(self):
        return self.dataset.region_code

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

        if p.startswith("sentinel-2"):
            return f"s2{p[-1]}"

        if p.startswith("sentinel-1"):
            return f"s1{p[-1]}"

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

        if p.startswith("sentinel-2"):
            return self.dataset.instrument[0].lower()

        if p.startswith("sentinel-1"):
            return self.dataset.instrument[0].lower()

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
                "TODO: Can only currently abbreviate instruments from Landsat references."
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
    label: str = None
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
