import collections.abc
import warnings
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Tuple, Dict, Optional, Any, Mapping, Callable, Union

import ciso8601

from eodatasets3.utils import default_utc


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
    if isinstance(value, str):
        value = ciso8601.parse_datetime(value)

    # Store all dates with a timezone.
    # yaml standard says all dates default to UTC.
    # (and ruamel normalises timezones to UTC itself)
    if isinstance(value, datetime):
        value = default_utc(value)

    return value


def of_enum_type(vals: Tuple[str, ...], lower=False, upper=False, strict=True):
    def normalise(v: str):
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


def normalise_platform(s: str):
    """
    >>> normalise_platform('LANDSAT_8')
    'landsat-8'
    """
    return s.lower().replace("_", "-")


def degrees_type(value):
    value = float(value)

    if not (-360.0 <= value <= 360.0):
        raise ValueError("Expected percent between 0,100")

    return value


def producer_check(value):
    if "." not in value:
        warnings.warn(
            "Property 'odc:producer' is expected to be a domain name, "
            "eg 'usgs.gov' or 'ga.gov.au'"
        )
    return value


# The primitive types allowed as stac values.
PrimitiveType = Union[str, int, float, datetime]
# A function to normalise a value.
# (eg. convert to int, or make string lowercase).
# They throw a ValueError if not valid.
NormaliseValueFn = Callable[[Any], PrimitiveType]

# Extras typically on the ARD product.
_GQA_FMASK_PROPS = {
    "fmask:clear": float,
    "fmask:cloud": float,
    "fmask:cloud_shadow": float,
    "fmask:snow": float,
    "fmask:water": float,
    "gqa:abs_iterative_mean_x": float,
    "gqa:abs_iterative_mean_xy": float,
    "gqa:abs_iterative_mean_y": float,
    "gqa:abs_x": float,
    "gqa:abs_xy": float,
    "gqa:abs_y": float,
    "gqa:cep90": float,
    "gqa:iterative_mean_x": float,
    "gqa:iterative_mean_xy": float,
    "gqa:iterative_mean_y": float,
    "gqa:iterative_stddev_x": float,
    "gqa:iterative_stddev_xy": float,
    "gqa:iterative_stddev_y": float,
    "gqa:mean_x": float,
    "gqa:mean_xy": float,
    "gqa:mean_y": float,
    "gqa:stddev_x": float,
    "gqa:stddev_xy": float,
    "gqa:stddev_y": float,
}

# Typically only from LPGS (ie. Level 1 products)
_LANDSAT_EXTENDED_PROPS = {
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
    "landsat:station_id": None,
}


class StacPropertyView(collections.abc.Mapping):
    # Every property we've seen or dealt with so far. Feel free to expand with abandon...
    # This is to minimise minor typos, case differences, etc, which plagued previous systems.
    # Keep sorted.
    KNOWN_STAC_PROPERTIES: Mapping[str, Optional[NormaliseValueFn]] = {
        "datetime": datetime_type,
        "dea:dataset_maturity": of_enum_type(("final", "interim", "nrt"), lower=True),
        "dea:processing_level": None,
        "dtr:end_datetime": datetime_type,
        "dtr:start_datetime": datetime_type,
        "eo:azimuth": float,
        "eo:cloud_cover": percent_type,
        "eo:epsg": None,
        "eo:gsd": None,
        "eo:instrument": None,
        "eo:off_nadir": None,
        "eo:platform": normalise_platform,
        "eo:sun_azimuth": degrees_type,
        "eo:sun_elevation": degrees_type,
        "landsat:landsat_product_id": None,
        "landsat:landsat_scene_id": None,
        "landsat:wrs_path": int,
        "landsat:wrs_row": int,
        "odc:dataset_version": None,
        "odc:file_format": of_enum_type(("GeoTIFF", "NetCDF"), strict=False),
        "odc:processing_datetime": datetime_type,
        "odc:producer": producer_check,
        "odc:product_family": None,
        "odc:reference_code": None,
        **_LANDSAT_EXTENDED_PROPS,
        **_GQA_FMASK_PROPS,
    }

    def __init__(self, properties=None) -> None:
        self._props = properties or {}

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

    def __setitem__(self, key, value):
        if key in self._props and value != self[key]:
            warnings.warn(
                f"Overriding property {key!r} " f"(from {self[key]!r} to {value!r})"
            )

        if key not in self.KNOWN_STAC_PROPERTIES:
            warnings.warn(f"Unknown stac property {key!r}")

        if value is not None:
            normalise = self.KNOWN_STAC_PROPERTIES.get(key)
            if normalise:
                value = normalise(value)

        self._props[key] = value

    def nested(self):
        return nest_properties(self._props)


class EoFields(metaclass=ABCMeta):
    """
    Convenient access fields for the most common/essential properties in datasets
    """

    @property
    @abstractmethod
    def properties(self) -> StacPropertyView:
        raise NotImplementedError

    @property
    def platform(self) -> str:
        return self.properties["eo:platform"]

    @platform.setter
    def platform(self, value: str):
        self.properties["eo:platform"] = value

    @property
    def instrument(self) -> str:
        return self.properties["eo:instrument"]

    @instrument.setter
    def instrument(self, value: str):
        self.properties["eo:instrument"] = value

    @property
    def producer(self) -> str:
        """
        Organisation that produced the data.

        eg. usgs.gov or ga.gov.au
        """
        return self.properties.get("odc:producer")

    @producer.setter
    def producer(self, domain: str):
        self.properties["odc:producer"] = domain

    @property
    def datetime_range(self):
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
    def datetime(self) -> datetime:
        return self.properties.get("datetime")

    @datetime.setter
    def datetime(self, val: datetime) -> datetime:
        self.properties["datetime"] = val

    @property
    def processed(self) -> datetime:
        """
        When the dataset was processed (Default to UTC if not specified)
        """
        return self.properties.get("odc:processing_datetime")

    @processed.setter
    def processed(self, value):
        self.properties["odc:processing_datetime"] = value

    @property
    def dataset_version(self):
        return self.properties.get("odc:dataset_version")

    @dataset_version.setter
    def dataset_version(self, value):
        self.properties["odc:dataset_version"] = value

    @property
    def product_family(self):
        return self.properties.get("odc:product_family")

    @product_family.setter
    def product_family(self, value):
        self.properties["odc:product_family"] = value

    @property
    def reference_code(self) -> str:
        return self.properties.get("odc:reference_code")

    @reference_code.setter
    def reference_code(self, value: str):
        self.properties["odc:reference_code"] = value
