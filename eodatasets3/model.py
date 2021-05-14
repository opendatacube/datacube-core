import re
from datetime import datetime
from pathlib import Path
from typing import Tuple, Dict, Optional, List, Sequence, Union
from uuid import UUID

import affine
import attr
from ruamel.yaml.comments import CommentedMap
from shapely.geometry.base import BaseGeometry

from eodatasets3 import utils
from eodatasets3.properties import StacPropertyView, EoFields

# TODO: these need discussion.
DEA_URI_PREFIX = "https://collections.dea.ga.gov.au"
ODC_DATASET_SCHEMA_URL = "https://schemas.opendatacube.org/dataset"

# Either a local filesystem path or a string URI.
# (the URI can use any scheme supported by rasterio, such as tar:// or https:// or ...)
Location = Union[Path, str]


def _dea_uri(product_name, base_uri):
    return f"{base_uri}/product/{product_name}"


@attr.s(auto_attribs=True, slots=True)
class ProductDoc:
    """
    The product that this dataset belongs to.

    "name" is the local name in ODC.

    href is intended as a more global unique "identifier" uri for the product.
    """

    name: str = None
    href: str = None

    @classmethod
    def dea_name(cls, name: str):
        return ProductDoc(name=name, href=_dea_uri(name, DEA_URI_PREFIX))


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

    KNOWN_PRODUCER_ABBREVIATIONS = {
        "ga.gov.au": "ga",
        "usgs.gov": "usgs",
        "sinergise.com": "sinergise",
        "digitalearthafrica.org": "deafrica",
        "esa.int": "esa",
        # Is there another organisation you want to use? Pull requests very welcome!
    }

    KNOWN_PLATFORM_ABBREVIATIONS = {
        "landsat-5": "ls5",
        "landsat-7": "ls7",
        "landsat-8": "ls8",
        "landsat-9": "ls9",
        "sentinel-1a": "s1a",
        "sentinel-1b": "s1b",
        "sentinel-2a": "s2a",
        "sentinel-2b": "s2b",
        "aqua": "aqu",
        "terra": "ter",
    }

    # If all platforms match a pattern, return this group name instead.
    KNOWN_PLATFORM_GROUPINGS = {
        "ls": re.compile(r"ls\d+"),
        "s1": re.compile(r"s1[a-z]+"),
        "s2": re.compile(r"s2[a-z]+"),
    }

    def __init__(
        self,
        dataset: EoFields,
        base_product_uri: str = None,
        required_fields: Sequence[str] = (),
        dataset_separator_field: Optional[str] = None,
        allow_unknown_abbreviations: bool = True,
    ) -> None:
        self.dataset = dataset
        self.base_product_uri = base_product_uri
        self.required_fields = self._ABSOLUTE_MINIMAL_PROPERTIES.union(required_fields)

        # An extra folder to put each dataset inside, using the value of the given property name.
        self.dataset_separator_field = dataset_separator_field

        if self.dataset_separator_field is not None:
            self.required_fields.add(dataset_separator_field)

        self.allow_unknown_abbreviations = allow_unknown_abbreviations

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
            # DEA wants consistency via the naming-conventions doc.
            allow_unknown_abbreviations=False,
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
            ),
            dataset_separator_field="sentinel:datatake_start_datetime",
            # DEA wants consistency via the naming-conventions doc.
            allow_unknown_abbreviations=False,
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
        if self.dataset.product_name:
            return self.dataset.product_name

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
        parts = []

        # If they've given a product name, just use it.
        if self.dataset.product_name:
            parts.append(self.dataset.product_name)
            if subname:
                parts.append(subname)
        else:
            self._check_enough_properties_to_name()

            # They're not specifying a sub-file. Fallback to the whole product's category.
            if not subname:
                subname = self.dataset.product_family

            if self.producer_abbreviated:
                parts.append(self.producer_abbreviated)

            platform = self.platform_abbreviated
            inst = self.instrument_abbreviated or ""
            if platform:
                parts.append(f"{platform}{inst}")

            if not subname:
                raise ValueError(
                    "Not enough metadata to create a useful filename! "
                    'Set the `product_family` (eg. "wofs") or a subname'
                )
            parts.append(subname)

        return "_".join(parts)

    @property
    def product_uri(self) -> Optional[str]:
        if not self.base_product_uri:
            return None

        return _dea_uri(self.product_name, base_uri=self.base_product_uri)

    @property
    def dataset_label(self) -> str:
        """
        Label for a dataset
        """
        return self._dataset_label()

    def destination_folder(self, base: Path) -> Path:
        # DEA naming conventions folder hierarchy.
        # Example: "ga_ls8c_ard_3/092/084/2016/06/28"

        parts = [self.product_name]

        # Cut the region code in subfolders
        region_code = self.dataset.region_code
        if region_code:
            parts.extend(utils.subfolderise(region_code))

        parts.extend(f"{self.dataset.datetime:%Y/%m/%d}".split("/"))

        # If it's not a final product, append the maturity to the folder.
        maturity: str = self.dataset.properties.get("dea:dataset_maturity")
        if maturity and maturity != "final":
            parts[-1] = f"{parts[-1]}_{maturity}"

        if self.dataset_separator_field is not None:
            val = self.dataset.properties[self.dataset_separator_field]
            # TODO: choosable formatter?
            if isinstance(val, datetime):
                val = f"{val:%Y%m%dT%H%M%S}"
            parts.append(val)
        return base.joinpath(*parts)

    def metadata_path(self, work_dir: Path, kind: str = "", suffix: str = "yaml"):
        return self._file(work_dir, kind, suffix)

    def checksum_path(self, work_dir: Path, suffix: str = "sha1"):
        return self._file(work_dir, "", suffix)

    def measurement_file_path(
        self, work_dir: Path, measurement_name: str, suffix: str, file_id: str = None
    ) -> Path:
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

        p = self.dataset.platforms
        if not p:
            return None

        if not self.allow_unknown_abbreviations:
            unknowns = p.difference(self.KNOWN_PLATFORM_ABBREVIATIONS)
            if unknowns:
                raise ValueError(
                    f"We don't know the DEA abbreviation for platforms {unknowns!r}. "
                    f"We'd love to add more! Raise an issue on Github: "
                    f"https://github.com/GeoscienceAustralia/eo-datasets/issues/new' "
                )

        abbreviations = sorted(
            self.KNOWN_PLATFORM_ABBREVIATIONS.get(s, s.replace("-", "")) for s in p
        )

        if len(abbreviations) == 1:
            return abbreviations[0]

        # If all abbreviations are in a group, name it using that group.
        # (eg. "ls" instead of "ls5-ls7-ls8")
        for group_name, pattern in self.KNOWN_PLATFORM_GROUPINGS.items():
            if all(pattern.match(a) for a in abbreviations):
                return group_name

        # Otherwise, there's a mix of platforms.

        # Is there a common constellation?
        constellation = self.dataset.properties.get("constellation")
        if constellation:
            return constellation

        # Don't bother to show anything for un-groupable mixes of platforms.
        return None

    @property
    def instrument_abbreviated(self) -> Optional[str]:
        """Abbreviated form of an instrument name, as used in dea product names. eg. 'c'."""
        platforms = self.dataset.platforms
        if not platforms or len(platforms) > 1:
            return None

        [p] = platforms

        if p.startswith("sentinel-1") or p.startswith("sentinel-2"):
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
        """Abbreviated form of a producer, as used in dea product names. eg. 'ga', 'usgs'."""
        if not self.dataset.producer:
            return None

        try:
            return self.KNOWN_PRODUCER_ABBREVIATIONS[self.dataset.producer]
        except KeyError:
            raise NotImplementedError(
                f"We don't know how to abbreviate organisation domain name {self.dataset.producer!r}. "
                f"We'd love to add more orgs! Raise an issue on Github: "
                f"https://github.com/GeoscienceAustralia/eo-datasets/issues/new' "
            )


class ComplicatedNamingConventionsDerivatives(ComplicatedNamingConventions):
    """
    This class is inherited from ComplicatedNamingConventions
    and overrides few attributes specific to C3 data processing for data other than ARD.
    """

    @classmethod
    def for_c3_derivatives(cls, dataset: EoFields, uri=DEA_URI_PREFIX):
        """
        The required fields for the c3 data processing are controlled here.
        """
        return cls(
            dataset=dataset,
            base_product_uri=uri,
            required_fields=(
                "eo:platform",
                "odc:dataset_version",
                "odc:collection_number",
                "odc:processing_datetime",
                "odc:producer",
                "odc:product_family",
                "odc:region_code",
                "dea:dataset_maturity",
            ),
        )

    @classmethod
    def for_s2_derivatives(cls, dataset: EoFields, uri=DEA_URI_PREFIX):
        """
        The required fields for the S2 data processing are controlled here.
        """
        return cls(
            dataset=dataset,
            base_product_uri=uri,
            # These fields are needed to fulfill official DEA naming conventions.
            required_fields=(
                "eo:platform",
                "odc:dataset_version",
                "odc:collection_number",
                "odc:processing_datetime",
                "odc:producer",
                "odc:product_family",
                "odc:region_code",
                "dea:dataset_maturity",
            ),
            dataset_separator_field="sentinel:datatake_start_datetime",
        )

    @property
    def _org_collection_number(self) -> Optional[int]:
        # Deliberately fail if collection_number is not defined in the config yaml
        return int(self.dataset.collection_number)

    def _product_group(self, subname=None) -> str:
        # Computes product group, e.g "ga_ls_wo_3"
        # Deliberately fail if any of these attributes not found.
        parts = [
            self.producer_abbreviated,
            self.platform_abbreviated,
            self.dataset.product_family,
        ]
        return "_".join(parts)

    def destination_folder(self, base: Path) -> Path:
        self._check_enough_properties_to_name()
        parts = [self.product_name, self.dataset.dataset_version.replace(".", "-")]
        parts.extend(utils.subfolderise(self.dataset.region_code))
        parts.extend(f"{self.dataset.datetime:%Y/%m/%d}".split("/"))

        if self.dataset_separator_field is not None:
            val = self.dataset.properties[self.dataset_separator_field]
            # TODO: choosable formatter?
            if isinstance(val, datetime):
                val = f"{val:%Y%m%dT%H%M%S}"
            parts.append(val)

        return base.joinpath(*parts)

    def _dataset_label(self, sub_name: str = None):
        """
        Responsible for producing the string of product name, regioncode, datetime and maturity
        ex: 'ga_ls_wo_3_090081_1998-07-30_interim'

        Redundant parameter sub_name is required, since the parent class and other invocations wants it so.
        """
        parts = [
            self.product_name,
            self._displayable_region_code,
            f"{self.dataset.datetime:%Y-%m-%d}",
            self.dataset.maturity,
        ]
        return "_".join(parts)

    @property
    def platform_abbreviated(self) -> Optional[str]:
        # For now from Alchemist the platform is always landsat for C3 processing
        if "landsat" in self.dataset.platform:
            return "ls"

        if "sentinel-2" in self.dataset.platform:
            return "s2"

        raise NotImplementedError(
            f"Only Landsat and Sentinel 2 platforms currently implemented for dea_c3 "
            f"and dea_s2_derivative naming schemes "
            f"(got {self.dataset.platform!r})"
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
