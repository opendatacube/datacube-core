# TODO: nice repr for the built-in transformations?

import numpy
import xarray

from datacube.storage.masking import make_mask as make_mask_prim
from datacube.storage.masking import mask_invalid_data as mask_invalid_data_prim

from .impl import VirtualProductException, Measurement
from .impl import Transform as transform, Transformation


def selective_apply_dict(dictionary, apply_to=None, key_map=None, value_map=None):
    def skip(key):
        return apply_to is not None and key not in apply_to

    def key_worker(key):
        if key_map is None or skip(key):
            return key

        return key_map(key)

    def value_worker(key, value):
        if value_map is None or skip(key):
            return value

        return value_map(key, value)

    return {key_worker(key): value_worker(key, value)
            for key, value in dictionary.items()}


def selective_apply(data, apply_to=None, key_map=None, value_map=None):
    return xarray.Dataset(data_vars=selective_apply_dict(data.data_vars, apply_to=apply_to,
                                                         key_map=key_map, value_map=value_map),
                          coords=data.coords, attrs=data.attrs)


class MakeMask(Transformation):
    def __init__(self, mask_measurement_name, flags):
        self.mask_measurement_name = mask_measurement_name
        self.flags = flags

    def measurements(self, input_measurements):
        if self.mask_measurement_name not in input_measurements:
            raise VirtualProductException("required measurement {} not found"
                                          .format(self.mask_measurement_name))

        def worker(_, value):
            result = value.copy()
            result['dtype'] = 'bool'
            return Measurement(**result)

        return selective_apply_dict(input_measurements,
                                    apply_to=[self.mask_measurement_name], value_map=worker)

    def compute(self, data):
        def worker(_, value):
            return make_mask_prim(value, **self.flags)

        return selective_apply(data, apply_to=[self.mask_measurement_name], value_map=worker)


def make_mask(product, mask_measurement_name, flags):
    """
    Create a mask that would only keep pixels for which the measurement with `mask_measurement_name`
    of the `product` satisfies `flags`.
    """
    return transform(product, MakeMask(mask_measurement_name, flags))


class ApplyMask(Transformation):
    def __init__(self, mask_measurement_name, apply_to=None,
                 preserve_dtype=True, fallback_dtype='float32'):
        self.mask_measurement_name = mask_measurement_name
        self.apply_to = apply_to
        self.preserve_dtype = preserve_dtype
        self.fallback_dtype = fallback_dtype

    def measurements(self, input_measurements):
        rest = {key: value
                for key, value in input_measurements.items()
                if key != self.mask_measurement_name}

        def worker(_, value):
            if self.preserve_dtype:
                return value

            result = value.copy()
            result['dtype'] = self.fallback_dtype
            return Measurement(**result)

        return selective_apply_dict(rest, apply_to=self.apply_to, value_map=worker)

    def compute(self, data):
        mask = data[self.mask_measurement_name]
        rest = data.drop(self.mask_measurement_name)

        def worker(key, value):
            if self.preserve_dtype:
                if 'nodata' not in value.attrs:
                    raise VirtualProductException("measurement {} has no nodata value".format(key))
                return value.where(mask, value.attrs['nodata'])

            return value.where(mask).astype(self.fallback_dtype)

        return selective_apply(rest, apply_to=self.apply_to, value_map=worker)


def mask_by_flags(product, mask_measurement_name, flags,
                  apply_to=None, preserve_dtype=True, fallback_dtype='float32'):
    """
    Only keep pixels for which the measurement with `mask_measurement_name` of the
    `product` satisfies `flags`.
    """
    return transform(make_mask(product, mask_measurement_name, flags),
                     ApplyMask(mask_measurement_name, apply_to=apply_to,
                               preserve_dtype=preserve_dtype, fallback_dtype=fallback_dtype))


class ToFloat(Transformation):
    def __init__(self, apply_to=None, dtype='float32'):
        self.apply_to = apply_to
        self.dtype = dtype

    def measurements(self, input_measurements):
        def worker(_, value):
            result = value.copy()
            result['dtype'] = self.dtype
            return Measurement(**result)

        return selective_apply_dict(input_measurements, apply_to=self.apply_to, value_map=worker)

    def compute(self, data):
        def worker(_, value):
            if hasattr(value, 'dtype') and value.dtype == self.dtype:
                return value

            return mask_invalid_data_prim(value).astype(self.dtype)

        return selective_apply(data, apply_to=self.apply_to, value_map=worker)


def to_float(product, apply_to=None, dtype='float32'):
    """
    Convert the dataset to float. Replaces `nodata` values with `Nan`s.
    """
    return transform(product, ToFloat(apply_to=apply_to, dtype=dtype))


class Rename(Transformation):
    def __init__(self, name_dict):
        self.name_dict = name_dict

    def measurements(self, input_measurements):
        def key_map(key):
            return self.name_dict[key]

        def value_map(key, value):
            result = value.copy()
            result['name'] = self.name_dict[key]
            return Measurement(**result)

        return selective_apply_dict(input_measurements, apply_to=self.name_dict,
                                    key_map=key_map, value_map=value_map)

    def compute(self, data):
        return data.rename(self.name_dict)


def rename(product, name_dict):
    return transform(product, Rename(name_dict))
