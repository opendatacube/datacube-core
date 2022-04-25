import collections.abc
import warnings
from abc import abstractmethod
from collections import defaultdict
from datetime import datetime
from enum import Enum, EnumMeta
from textwrap import dedent
from typing import Any, Callable, Dict, Mapping, Optional, Set, Tuple, Union
from urllib.parse import urlencode

import ciso8601
from ruamel.yaml.timestamp import TimeStamp as RuamelTimeStamp

from eodatasets3.utils import default_utc


class FileFormat(Enum):
    GeoTIFF = 1
    NetCDF = 2
    Zarr = 3
    JPEG2000 = 4


def nest_properties(d: Mapping[str, Any], separator=":") -> Dict[str, Any]:
    """
    Split keys with embedded colons into sub dictionaries.

    Intended for stac-like properties

    >>> nest_properties({'landsat:path':1, 'landsat:row':2, 'clouds':3})
    {'landsat': {'path': 1, 'row': 2}, 'clouds': 3}
    """
    out = defaultdict(dict)
    for key, val in d.items():
        section, *remainder = key.split(separator, 1)
        if remainder:
            [sub_key] = remainder
            out[section][sub_key] = val
        else:
            out[section] = val

    for key, val in out.items():
        if isinstance(val, dict):
            out[key] = nest_properties(val, separator=separator)

    return dict(out)


def datetime_type(value):
    # Ruamel's TimeZone class can become invalid from the .replace(utc) call.
    # (I think it no longer matches the internal ._yaml fields.)
    # Convert to a regular datetime.
    if isinstance(value, RuamelTimeStamp):
        value = value.isoformat()

    if isinstance(value, str):
        value = ciso8601.parse_datetime(value)

    # Store all dates with a timezone.
    # yaml standard says all dates default to UTC.
    # (and ruamel normalises timezones to UTC itself)
    return default_utc(value)


def of_enum_type(
    vals: Union[EnumMeta, Tuple[str, ...]] = None, lower=False, upper=False, strict=True
) -> Callable[[str], str]:
    if isinstance(vals, EnumMeta):
        vals = tuple(vals.__members__.keys())

    def normalise(v: str):
        if isinstance(v, Enum):
            v = v.name

        if upper:
            v = v.upper()
        if lower:
            v = v.lower()

        if v not in vals:
            msg = f"Unexpected value {v!r}. Expected one of: {', '.join(vals)},"
            if strict:
                raise ValueError(msg)
            else:
                warnings.warn(msg)
        return v

    return normalise


def percent_type(value):
    value = float(value)

    if not (0.0 <= value <= 100.0):
        raise ValueError("Expected percent between 0,100")
    return value


def normalise_platforms(value: Union[str, list, set]):
    """
    >>> normalise_platforms('LANDSAT_8')
    'landsat-8'
    >>> # Multiple can be comma-separated. They're normalised independently and sorted.
    >>> normalise_platforms('LANDSAT_8,Landsat-5,landsat-7')
    'landsat-5,landsat-7,landsat-8'
    >>> # Can be given as a list.
    >>> normalise_platforms(['sentinel-2b','SENTINEL-2a'])
    'sentinel-2a,sentinel-2b'
    >>> # Deduplicated too
    >>> normalise_platforms('landsat-5,landsat-5,LANDSAT-5')
    'landsat-5'
    """
    if not isinstance(value, (list, set, tuple)):
        value = value.split(",")

    platforms = sorted({s.strip().lower().replace("_", "-") for s in value if s})
    if not platforms:
        return None

    return ",".join(platforms)


def degrees_type(value):
    value = float(value)

    if not (-360.0 <= value <= 360.0):
        raise ValueError("Expected degrees between -360,+360")

    return value


def identifier_type(v: str):
    v = v.replace("-", "_")
    if not v.isidentifier() or not v.islower():
        warnings.warn(
            f"{v!r} is expected to be an identifier "
            "(alphanumeric with underscores, typically lowercase)"
        )
    return v


def producer_check(value):
    if "." not in value:
        warnings.warn(
            "Property 'odc:producer' is expected to be a domain name, "
            "eg 'usgs.gov' or 'ga.gov.au'"
        )
    return value


def parsed_sentinel_tile_id(tile_id) -> Tuple[str, Dict]:
    """Extract useful extra fields from a sentinel tile id

    >>> val, props = parsed_sentinel_tile_id("S2B_OPER_MSI_L1C_TL_EPAE_20201011T011446_A018789_T55HFA_N02.09")
    >>> val
    'S2B_OPER_MSI_L1C_TL_EPAE_20201011T011446_A018789_T55HFA_N02.09'
    >>> props
    {'sentinel:datatake_start_datetime': datetime.datetime(2020, 10, 11, 1, 14, 46, tzinfo=datetime.timezone.utc)}
    """
    extras = {}
    split_tile_id = tile_id.split("_")
    try:
        datatake_sensing_time = datetime_type(split_tile_id[-4])
        extras["sentinel:datatake_start_datetime"] = datatake_sensing_time
    except IndexError:
        pass

    # TODO: we could extract other useful fields?

    return tile_id, extras


def parsed_sentinel_datastrip_id(tile_id) -> Tuple[str, Dict]:
    """Extract useful extra fields from a sentinel datastrip id

    >>> val, props = parsed_sentinel_datastrip_id("S2B_OPER_MSI_L1C_DS_EPAE_20201011T011446_S20201011T000244_N02.09")
    >>> val
    'S2B_OPER_MSI_L1C_DS_EPAE_20201011T011446_S20201011T000244_N02.09'
    >>> props
    {'sentinel:datatake_start_datetime': datetime.datetime(2020, 10, 11, 1, 14, 46, tzinfo=datetime.timezone.utc)}
    """
    extras = {}
    split_tile_id = tile_id.split("_")
    try:
        datatake_sensing_time = datetime_type(split_tile_id[-3])
        extras["sentinel:datatake_start_datetime"] = datatake_sensing_time
    except IndexError:
        pass

    # TODO: we could extract other useful fields?

    return tile_id, extras


# The primitive types allowed as stac values.
PrimitiveType = Union[str, int, float, datetime]

ExtraProperties = Dict
# A function to normalise a value.
# (eg. convert to int, or make string lowercase).
# They throw a ValueError if not valid.
NormaliseValueFn = Callable[
    [Any],
    # It returns the normalised value, but can optionally also return extra property values extracted from it.
    Union[PrimitiveType, Tuple[PrimitiveType, ExtraProperties]],
]

# Extras typically on the ARD product.
_GQA_FMASK_PROPS = {
    "fmask:clear": float,
    "fmask:cloud": float,
    "fmask:cloud_shadow": float,
    "fmask:snow": float,
    "fmask:water": float,
    "s2cloudless:clear": float,
    "s2cloudless:cloud": float,
    "gqa:abs_iterative_mean_x": float,
    "gqa:abs_iterative_mean_xy": float,
    "gqa:abs_iterative_mean_y": float,
    "gqa:abs_x": float,
    "gqa:abs_xy": float,
    "gqa:abs_y": float,
    "gqa:cep90": float,
    "gqa:error_message": None,
    "gqa:final_gcp_count": int,
    "gqa:iterative_mean_x": float,
    "gqa:iterative_mean_xy": float,
    "gqa:iterative_mean_y": float,
    "gqa:iterative_stddev_x": float,
    "gqa:iterative_stddev_xy": float,
    "gqa:iterative_stddev_y": float,
    "gqa:mean_x": float,
    "gqa:mean_xy": float,
    "gqa:mean_y": float,
    "gqa:ref_source": None,
    "gqa:stddev_x": float,
    "gqa:stddev_xy": float,
    "gqa:stddev_y": float,
}

# Typically only from LPGS (ie. Level 1 products)
_LANDSAT_EXTENDED_PROPS = {
    "landsat:algorithm_source_surface_reflectance": None,
    "landsat:collection_category": None,
    "landsat:collection_number": int,
    "landsat:data_type": None,
    "landsat:earth_sun_distance": None,
    "landsat:ephemeris_type": None,
    "landsat:geometric_rmse_model": None,
    "landsat:geometric_rmse_model_x": None,
    "landsat:geometric_rmse_model_y": None,
    "landsat:geometric_rmse_verify": None,
    "landsat:ground_control_points_model": None,
    "landsat:ground_control_points_verify": None,
    "landsat:ground_control_points_version": None,
    "landsat:image_quality_oli": None,
    "landsat:image_quality_tirs": None,
    "landsat:processing_software_version": None,
    "landsat:scan_gap_interpolation": float,
    "landsat:station_id": None,
    # Landsat USGS Properties
    "landsat:rmse": None,
    "landsat:rmse_x": None,
    "landsat:rmse_y": None,
    "landsat:wrs_type": None,
    "landsat:correction": None,
    "landsat:cloud_cover_land": None,
}

_SENTINEL_EXTENDED_PROPS = {
    "sentinel:sentinel_tile_id": parsed_sentinel_tile_id,
    "sentinel:datatake_start_datetime": datetime_type,
    "sentinel:datastrip_id": parsed_sentinel_datastrip_id,
    "sentinel:datatake_type": None,
    "sentinel:processing_baseline": None,
    "sentinel:processing_center": None,
    "sentinel:product_name": None,
    "sentinel:reception_station": None,
    "sentinel:utm_zone": int,
    "sentinel:latitude_band": None,
    "sentinel:grid_square": None,
    "sinergise_product_id": None,
}


_STAC_MISC_PROPS = {
    "providers": None,  # https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md#provider,
    # Projection extension
    "proj:epsg": int,
    "proj:shape": None,
    "proj:transform": None,
}


class Eo3Dict(collections.abc.MutableMapping):
    """
    This acts like a dictionary, but will normalise known properties (consistent
    case, types etc) and warn about common mistakes.

    It wraps an inner dictionary. By default it will normalise the fields in
    the input dictionary on creation, but you can disable this with `normalise_input=False`.
    """

    # Every property we've seen or dealt with so far. Feel free to expand with abandon...
    # This is to minimise minor typos, case differences, etc, which plagued previous systems.
    # Keep sorted.
    KNOWN_PROPERTIES: Mapping[str, Optional[NormaliseValueFn]] = {
        "datetime": datetime_type,
        "dea:dataset_maturity": of_enum_type(("final", "interim", "nrt"), lower=True),
        "dea:product_maturity": of_enum_type(("stable", "provisional"), lower=True),
        "dtr:end_datetime": datetime_type,
        "dtr:start_datetime": datetime_type,
        "eo:azimuth": float,
        "eo:cloud_cover": percent_type,
        "eo:epsg": None,
        "eo:gsd": None,
        "eo:instrument": None,
        "eo:off_nadir": float,
        "eo:platform": normalise_platforms,
        "eo:constellation": None,
        "eo:sun_azimuth": degrees_type,
        "eo:sun_elevation": degrees_type,
        "sat:orbit_state": None,
        "sat:relative_orbit": int,
        "sat:absolute_orbit": int,
        "landsat:landsat_product_id": None,
        "landsat:scene_id": None,
        "landsat:landsat_scene_id": None,
        "landsat:wrs_path": int,
        "landsat:wrs_row": int,
        "odc:dataset_version": None,
        "odc:collection_number": int,
        "odc:naming_conventions": None,
        # Not strict as there may be more added in ODC...
        "odc:file_format": of_enum_type(FileFormat, strict=False),
        "odc:processing_datetime": datetime_type,
        "odc:producer": producer_check,
        "odc:product": None,
        "odc:product_family": identifier_type,
        "odc:region_code": None,
        "odc:sat_row": None,  # When a dataset has a range of rows (... telemetry)
        **_LANDSAT_EXTENDED_PROPS,
        **_GQA_FMASK_PROPS,
        **_SENTINEL_EXTENDED_PROPS,
        **_STAC_MISC_PROPS,
    }

    # For backwards compatibility, in case users are extending at runtime.
    KNOWN_STAC_PROPERTIES = KNOWN_PROPERTIES

    def __init__(self, properties: Mapping = None, normalise_input=True) -> None:
        if properties is None:
            properties = {}
        self._props = properties
        # We normalise the properties they gave us.
        for key in list(self._props):
            # We always want to normalise dates as datetime objects rather than strings
            # for consistency.
            if normalise_input or ("datetime" in key):
                self.normalise_and_set(key, self._props[key], expect_override=True)
        self._finished_init_ = True

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Prevent against users accidentally setting new properties (it has happened multiple times).
        """
        if hasattr(self, "_finished_init_") and not hasattr(self, name):
            raise TypeError(
                f"Cannot set new field '{name}' on a dict. "
                f"(Perhaps you meant to set it as a dictionary field??)"
            )
        super().__setattr__(name, value)

    def __getitem__(self, item):
        return self._props[item]

    def __iter__(self):
        return iter(self._props)

    def __len__(self):
        return len(self._props)

    def __delitem__(self, name: str) -> None:
        del self._props[name]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._props!r})"

    def __setitem__(self, key, value):
        self.normalise_and_set(
            key,
            value,
            # They can override properties but will receive a warning.
            allow_override=True,
        )

    def normalise_and_set(self, key, value, allow_override=True, expect_override=False):
        """
        Set a property with the usual normalisation.

        This has some options that are not available on normal dictionary item
        setting (``self[key] = val``)

        The default behaviour of this class is very conservative in order to catch common errors
        of users. You can loosen the settings here.

        :argument allow_override: Is it okay to overwrite an existing value? (if not, error will be thrown)
        :argument expect_override: We expect to overwrite a property, so don't produce a warning or error.
        """
        if key not in self.KNOWN_PROPERTIES:
            warnings.warn(
                f"Unknown Stac property {key!r}. "
                f"If this is valid property, please tell us on Github here so we can add it: "
                f"\n\t{_github_suggest_new_property_url(key, value)}"
            )

        if value is not None:
            normalise = self.KNOWN_PROPERTIES.get(key)
            if normalise:
                value = normalise(value)
                # If the normaliser has extracted extra properties, we'll get two return values.
                if isinstance(value, Tuple):
                    value, extra_properties = value
                    for k, v in extra_properties.items():
                        if k == key:
                            raise RuntimeError(
                                f"Infinite loop: writing key {k!r} from itself"
                            )
                        self.normalise_and_set(k, v, allow_override=allow_override)

        if key in self._props and value != self[key] and (not expect_override):
            message = (
                f"Overriding property {key!r} " f"(from {self[key]!r} to {value!r})"
            )
            if allow_override:
                warnings.warn(message, category=PropertyOverrideWarning)
            else:
                raise KeyError(message)

        self._props[key] = value

    def nested(self):
        return nest_properties(self._props)


class StacPropertyView(Eo3Dict):
    """
    Backwards compatibility class name. Deprecated.

    Use the identical 'Eo3Dict' instead.

    These were called  "StacProperties" in Stac 0.6, but many of them have
    changed in newer versions and we're sticking to the old names for consistency
    and backwards-compatibility. So they're now EO3 Properties.

    (The eo3-to-stac tool to convert EO3 properties to real Stac properties.)
    """

    def __init__(self, properties=None) -> None:
        super().__init__(properties)
        warnings.warn(
            "The class name 'StacPropertyView' is deprecated as it's misleading. "
            "Please change your import to the (identical) 'Eo3Dict'.",
            category=DeprecationWarning,
        )


class PropertyOverrideWarning(UserWarning):
    """A warning that a property was set twice with different values."""

    ...


class Eo3Interface:
    """
    These are convenience properties for common metadata fields. They are available
    on DatasetAssemblers and within other naming APIs.

    (This is abstract. If you want one of these of your own, you probably want to create
    an :class:`eodatasets3.DatasetDoc`)

    """

    @property
    @abstractmethod
    def properties(self) -> Eo3Dict:
        raise NotImplementedError

    @property
    def platform(self) -> Optional[str]:
        """
        Unique name of the specific platform the instrument is attached to.

        For satellites this would be the name of the satellite (e.g., ``landsat-8``, ``sentinel-2a``),
        whereas for drones this would be a unique name for the drone.

        In derivative products, multiple platforms can be specified with a comma: ``landsat-5,landsat-7``.

        Shorthand for ``eo:platform`` property
        """
        return self.properties.get("eo:platform")

    @platform.setter
    def platform(self, value: str):
        self.properties["eo:platform"] = value

    @property
    def platforms(self) -> Set[str]:
        """
        Get platform as a set (containing zero or more items).

        In EO3, multiple platforms are specified by comma-separating them.
        """
        if not self.platform:
            return set()
        return set(self.properties.get("eo:platform", "").split(","))

    @platforms.setter
    def platforms(self, value: Set[str]):
        # The normaliser supports sets/lists
        self.properties["eo:platform"] = value

    @property
    def instrument(self) -> str:
        """
        Name of instrument or sensor used (e.g., MODIS, ASTER, OLI, Canon F-1).

        Shorthand for ``eo:instrument`` property
        """
        return self.properties.get("eo:instrument")

    @instrument.setter
    def instrument(self, value: str):
        self.properties["eo:instrument"] = value

    @property
    def constellation(self) -> str:
        """
        Constellation. Eg ``sentinel-2``.
        """
        return self.properties.get("eo:constellation")

    @constellation.setter
    def constellation(self, value: str):
        self.properties["eo:constellation"] = value

    @property
    def product_name(self) -> Optional[str]:
        """
        The ODC product name
        """
        return self.properties.get("odc:product")

    @product_name.setter
    def product_name(self, value: str):
        self.properties["odc:product"] = value

    @property
    def producer(self) -> str:
        """
        Organisation that produced the data.

        eg. ``usgs.gov`` or ``ga.gov.au``

        Shorthand for ``odc:producer`` property
        """
        return self.properties.get("odc:producer")

    @producer.setter
    def producer(self, domain: str):
        self.properties["odc:producer"] = domain

    @property
    def datetime_range(self) -> Tuple[datetime, datetime]:
        """
        An optional date range for the dataset.

        The ``datetime`` is still mandatory when this is set.

        This field is a shorthand for reading/setting the datetime-range
        stac 0.6 extension properties: ``dtr:start_datetime`` and ``dtr:end_datetime``
        """
        return (
            self.properties.get("dtr:start_datetime"),
            self.properties.get("dtr:end_datetime"),
        )

    @datetime_range.setter
    def datetime_range(self, val: Tuple[datetime, datetime]):
        # TODO: string type conversion, better validation/errors
        start, end = val
        self.properties["dtr:start_datetime"] = start
        self.properties["dtr:end_datetime"] = end

    @property
    def processed(self) -> datetime:
        """When the dataset was created (Defaults to UTC if not specified)

        Shorthand for the ``odc:processing_datetime`` field
        """
        return self.properties.get("odc:processing_datetime")

    @processed.setter
    def processed(self, value: Union[str, datetime]):
        self.properties["odc:processing_datetime"] = value

    def processed_now(self):
        """
        Shorthand for when the dataset was processed right now on the current system.
        """
        self.properties["odc:processing_datetime"] = datetime.utcnow()

    @property
    def dataset_version(self) -> str:
        """
        The version of the dataset.

        Typically digits separated by a dot. Eg. `1.0.0`

        The first digit is usually the collection number for
        this 'producer' organisation, such as USGS Collection 1 or
        GA Collection 3.
        """
        return self.properties.get("odc:dataset_version")

    @property
    def collection_number(self) -> int:
        """
        The version of the collection.

        Eg.::

           metadata:
             product_family: wofs
             dataset_version: 1.6.0
             collection_number: 3

        """
        return self.properties.get("odc:collection_number")

    @dataset_version.setter
    def dataset_version(self, value):
        self.properties["odc:dataset_version"] = value

    @collection_number.setter
    def collection_number(self, value):
        self.properties["odc:collection_number"] = value

    @property
    def naming_conventions(self) -> str:
        return self.properties.get("odc:naming_conventions")

    @naming_conventions.setter
    def naming_conventions(self, value):
        self.properties["odc:naming_conventions"] = value

    @property
    def product_family(self) -> str:
        """
        The identifier for this "family" of products, such as ``ard``, ``level1`` or ``fc``.
        It's used for grouping similar products together.

        They products in a family are usually produced the same way but have small variations:
        they come from different sensors, or are written in different projections, etc.

        ``ard`` family of products: ``ls7_ard``, ``ls5_ard`` ....

        On older versions of Open Data Cube this was called ``product_type``.

        Shorthand for ``odc:product_family`` property.
        """
        return self.properties.get("odc:product_family")

    @product_family.setter
    def product_family(self, value):
        self.properties["odc:product_family"] = value

    @product_family.deleter
    def product_family(self):
        del self.properties["odc:product_family"]

    @property
    def region_code(self) -> Optional[str]:
        """
        The "region" of acquisition. This is a platform-agnostic representation of things like
        the Landsat Path+Row. Datasets with the same Region Code will *roughly* (but usually
        not *exactly*) cover the same spatial footprint.

        It's generally treated as an opaque string to group datasets and process as stacks.

        For Landsat products it's the concatenated ``{path}{row}`` (both numbers formatted to three digits).

        For Sentinel 2, it's the MGRS grid (TODO presumably?).

        Shorthand for ``odc:region_code`` property.
        """
        return self.properties.get("odc:region_code")

    @region_code.setter
    def region_code(self, value: str):
        self.properties["odc:region_code"] = value

    @property
    def maturity(self) -> str:
        """
        The dataset maturity. The same data may be processed multiple times -- becoming more
        mature -- as new ancillary data becomes available.

        Typical values (from least to most mature): ``nrt`` (near real time), ``interim``, ``final``
        """
        return self.properties.get("dea:dataset_maturity")

    @maturity.setter
    def maturity(self, value):
        self.properties["dea:dataset_maturity"] = value

    @property
    def product_maturity(self) -> str:
        """
        Classification: is this a 'provisional' or 'stable' release of the product?
        """
        return self.properties.get("dea:product_maturity")

    @product_maturity.setter
    def product_maturity(self, value):
        self.properties["dea:product_maturity"] = value

    # Note that giving a method the name 'datetime' will override the 'datetime' type
    # for class-level declarations (ie, for any types on functions!)
    # So we make an alias:
    from datetime import datetime as datetime_

    @property
    def datetime(self) -> datetime_:
        """
        The searchable date and time of the assets. (Default to UTC if not specified)
        """
        return self.properties.get("datetime")

    @datetime.setter
    def datetime(self, val: datetime_):
        self.properties["datetime"] = val


def _github_suggest_new_property_url(key: str, value: object) -> str:
    """Get a URL to create a Github issue suggesting new properties to be added."""
    issue_parameters = urlencode(
        dict(
            title=f"Include property {key!r}",
            labels="known-properties",
            body=dedent(
                f"""\
                   Hello! The property {key!r} does not appear to be in the KNOWN_PROPERTIES list,
                   but I believe it to be valid.

                   An example value of this property is: {value!r}

                   Thank you!
                   """
            ),
        )
    )
    return f"https://github.com/GeoscienceAustralia/eo-datasets/issues/new?{issue_parameters}"
