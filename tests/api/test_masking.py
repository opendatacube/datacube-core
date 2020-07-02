# coding=utf-8
import yaml
import pytest
from xarray import DataArray, Dataset
import numpy as np

from datacube.utils.masking import (
    list_flag_names,
    create_mask_value,
    describe_variable_flags,
    mask_to_dict,
    mask_invalid_data,
    valid_data_mask,
)


@pytest.fixture
def simple_var():
    flags = SimpleVariableWithFlagsDef().flags_definition
    return DataArray(np.zeros((2, 3)),
                     dims=('y', 'x'),
                     name='simple_var',
                     attrs={'flags_definition': flags})


def test_list_flag_names(simple_var):
    flags = list_flag_names(simple_var)
    for flag_name in simple_var.flags_definition.keys():
        assert flag_name in flags

    with pytest.raises(ValueError):
        list_flag_names(([], {}))


def test_create_mask_value(simple_var):
    bits_def = simple_var.flags_definition

    assert create_mask_value(bits_def, contiguous=True) == (256, 256)
    assert create_mask_value(bits_def, contiguous=False) == (256, 0)
    assert create_mask_value(bits_def, contiguous=True, land_sea='land') == (
        768, 768)
    assert create_mask_value(bits_def, contiguous=False, land_sea='land') == (768, 512)


def test_create_multi_mask_value():
    multi_var = VariableWithMultiBitFlags()
    multi_flags_def = multi_var.flags_definition

    assert create_mask_value(multi_flags_def, filled=True) == (1, 1)
    assert create_mask_value(multi_flags_def, water_confidence='water') == (0b011000, 0b011000)

    assert create_mask_value(multi_flags_def, water_confidence='water', filled=True) == (
        0b011001, 0b011001)
    assert create_mask_value(multi_flags_def, water_confidence='not_determined') == (0b011000, 0b0)
    assert create_mask_value(multi_flags_def, water_confidence='no_water') == (0b11000, 0b01000)
    assert create_mask_value(multi_flags_def, veg_confidence='maybe_veg') == (
        0b110000000, 0b100000000)
    assert create_mask_value(multi_flags_def,
                             veg_confidence='maybe_veg',
                             water_confidence='water') == (0b110011000, 0b100011000)
    assert create_mask_value(multi_flags_def,
                             veg_confidence='maybe_veg',
                             water_confidence='water', filled=True) == (0b110011001, 0b100011001)

    assert create_mask_value(multi_flags_def, water_confidence='maybe_water') == (0b011000, 0b10000)

    with pytest.raises(ValueError):
        create_mask_value(multi_flags_def, this_flag_doesnot_exist=9)

    with pytest.raises(ValueError):
        create_mask_value(multi_flags_def, water_confidence='invalid enum value')


def test_ga_good_pixel(simple_var):
    bits_def = simple_var.flags_definition

    assert create_mask_value(bits_def, ga_good_pixel=True) == (16383, 16383)


def test_describe_flags(simple_var):
    describe_variable_flags(simple_var)
    describe_variable_flags(simple_var, with_pandas=False)

    describe_variable_flags(simple_var.to_dataset())
    describe_variable_flags(simple_var.to_dataset(), with_pandas=False)


class SimpleVariableWithFlagsDef(object):
    bits_def_yaml = """
        cloud_shadow_fmask:
          bits: 13
          description: Cloud Shadow (Fmask)
          values:
            0: cloud_shadow
            1: no_cloud_shadow
        cloud_shadow_acca:
          bits: 12
          description: Cloud Shadow (ACCA)
          values:
            0: cloud_shadow
            1: no_cloud_shadow
        cloud_fmask:
          bits: 11
          description: Cloud (Fmask)
          values:
            0: cloud
            1: no_cloud
        cloud_acca:
          bits: 10
          description: Cloud Shadow (ACCA)
          values:
            0: cloud
            1: no_cloud
        land_sea:
          bits: 9
          description: Land or Sea
          values:
            0: sea
            1: land
        contiguous:
          bits: 8
          description: All bands for this pixel contain non-null values
          values:
            0: false
            1: true
        swir2_saturated:
          bits: 7
          description: SWIR2 is saturated
          values:
            0: true
            1: false

        swir2_saturated:
          bits: 6
          description: SWIR2 band is saturated
          values:
            0: true
            1: false
        swir1_saturated:
          bits: 4
          description: SWIR1 band is saturated
          values:
            0: true
            1: false
        nir_saturated:
          bits: 3
          description: NIR band is saturated
          values:
            0: true
            1: false

        red_saturated:
          bits: 2
          description: Red band is saturated
          values:
            0: true
            1: false

        green_saturated:
          bits: 1
          description: Green band is saturated
          values:
            0: true
            1: false

        blue_saturated:
          bits: 0
          description: Blue band is saturated
          values:
            0: true
            1: false

        ga_good_pixel:
          bits: [13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
          description: Best Quality Pixel
          values:
            16383: true
        """

    flags_definition = yaml.safe_load(bits_def_yaml)


class VariableWithMultiBitFlags(object):
    bits_def_yaml = """
        cloud_confidence:
          bits: [13, 14]
          description: Cloud Confidence
          values:
            0: not_determined
            1: no_cloud
            2: maybe_cloud
            3: cloud
        cirrus_confidence:
          bits: [11, 12]
          description: Cirrus Confidence
          values:
            0: not_determined
            1: no_cirrus
            2: maybe_cirrus
            3: cirrus

        snowice_confidence:
          bits: [9, 10]
          description: Snow/Ice Confidence
          values:
            0: not_determined
            1: no_snowice
            2: maybe_snowice
            3: snowice

        veg_confidence:
          bits: [7, 8]
          description: Vegetation Confidence
          values:
            0: not_determined
            1: no_veg
            2: maybe_veg
            3: veg



        water_confidence:
          bits: [3, 4]
          description: Water Confidence
          values:
            0: not_determined
            1: no_water
            2: maybe_water
            3: water

        terrain_occluded:
          bits: 2
          description: Terrain occluded
          values:
            0: False
            1: True

        frame_dropped:
          bits: 1
          description: Frame dropped
          values:
            0: False
            1: True

        filled:
          bits: 0
          description: Filled
          values:
            0: False
            1: True
    """

    flags_definition = yaml.safe_load(bits_def_yaml)


bits_def_json = {
    'blue_saturated': {
        'bits': 0,
        'description': 'Blue band is saturated',
        'values': {'0': True, '1': False}},
    'cloud_acca': {
        'bits': 10,
        'description': 'Cloud Shadow (ACCA)',
        'values': {'0': 'cloud', '1': 'no_cloud'}},
    'cloud_fmask': {
        'bits': 11,
        'description': 'Cloud (Fmask)',
        'values': {'0': 'cloud', '1': 'no_cloud'}},
    'cloud_shadow_acca': {
        'bits': 12,
        'description': 'Cloud Shadow (ACCA)',
        'values': {'0': 'cloud_shadow', '1': 'no_cloud_shadow'}},
    'cloud_shadow_fmask': {
        'bits': 13,
        'description': 'Cloud Shadow (Fmask)',
        'values': {'0': 'cloud_shadow', '1': 'no_cloud_shadow'}},
    'contiguous': {
        'bits': 8,
        'description': 'All bands for this pixel contain non-null values',
        'values': {'0': False, '1': True}},
    'ga_good_pixel': {
        'bits': [13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
        'description': 'Best Quality Pixel',
        'values': {'16383': True}},
    'green_saturated': {
        'bits': 1,
        'description': 'Green band is saturated',
        'values': {'0': True, '1': False}},
    'land_sea': {
        'bits': 9,
        'description': 'Land or Sea',
        'values': {'0': 'sea', '1': 'land'}},
    'nir_saturated': {
        'bits': 3,
        'description': 'NIR band is saturated',
        'values': {'0': True, '1': False}},
    'red_saturated': {
        'bits': 2,
        'description': 'Red band is saturated',
        'values': {'0': True, '1': False}},
    'swir1_saturated': {
        'bits': 4,
        'description': 'SWIR1 band is saturated',
        'values': {'0': True, '1': False}},
    'swir2_saturated': {
        'bits': 7,
        'description': 'SWIR2 band is saturated',
        'values': {'0': True, '1': False}},
    'tir_saturated': {
        'bits': 5,
        'description': 'Thermal Infrared band is saturated',
        'values': {'0': True, '1': False}}}


@pytest.mark.parametrize('bits_def', [SimpleVariableWithFlagsDef.flags_definition,
                                      bits_def_json])
def test_simple_mask_to_dict(bits_def):
    # All 0. Contiguous should be False, Saturated bits should be true
    test_dict = mask_to_dict(bits_def, 0)
    assert not test_dict['contiguous']
    assert test_dict['blue_saturated']

    # Only contiguous (bit 8) set
    test_dict = mask_to_dict(bits_def, 256)
    assert test_dict['contiguous']
    assert test_dict['blue_saturated']
    assert test_dict['land_sea'] == 'sea'

    # Contiguous and land_sea bits set to 1. (bits 7 and 8)
    test_dict = mask_to_dict(bits_def, 768)
    assert test_dict['contiguous']
    assert test_dict['land_sea'] == 'land'
    assert test_dict['blue_saturated']

    # All GA PQ bits set to 1
    test_dict = mask_to_dict(bits_def, 16383)
    assert test_dict['ga_good_pixel']
    assert test_dict['contiguous']
    assert test_dict['land_sea'] == 'land'
    assert not test_dict['blue_saturated']


def test_mask_valid_data():
    test_attrs = {
        'one': 1,
        'nodata': -999,
    }

    expected_data_array = DataArray(np.array([[1., np.nan, np.nan], [2, 3, np.nan], [np.nan, np.nan, np.nan]],
                                             dtype='float'),
                                    attrs=test_attrs, name='var_one')

    data_array = DataArray([[1, -999, -999], [2, 3, -999], [-999, -999, -999]], attrs=test_attrs)
    dataset = Dataset(data_vars={'var_one': data_array}, attrs={'ds_attr': 'still here'})

    # Make sure test is actually changing something
    assert not data_array.equals(expected_data_array)

    output_ds = mask_invalid_data(dataset, keep_attrs=True)
    assert output_ds.attrs['ds_attr'] == 'still here'
    assert output_ds.data_vars['var_one'].equals(expected_data_array)
    assert output_ds.data_vars['var_one'].attrs['one'] == 1

    output_da = mask_invalid_data(data_array, keep_attrs=True)
    assert output_da.equals(expected_data_array)
    assert output_da.attrs['one'] == 1

    missing_nodata = data_array.copy()
    del missing_nodata.attrs['nodata']
    assert not hasattr(missing_nodata, 'nodata')
    np.testing.assert_array_equal(missing_nodata, mask_invalid_data(missing_nodata))

    with pytest.raises(TypeError):
        mask_invalid_data({})


def test_valid_data_mask():
    attrs = {
        'nodata': -999,
    }

    expected_data_array = DataArray(np.array([[True, False, False], [True, True, False], [False, False, False]],
                                             dtype='bool'))

    data_array = DataArray([[1, -999, -999], [2, 3, -999], [-999, -999, -999]], attrs=attrs)
    dataset = Dataset(data_vars={'var_one': data_array})

    output_ds = valid_data_mask(dataset)
    assert output_ds.data_vars['var_one'].equals(expected_data_array)

    output_da = valid_data_mask(data_array)
    assert output_da.equals(expected_data_array)

    expected_data_array = DataArray(np.array([[True, True, True], [True, True, True], [True, True, True]],
                                             dtype='bool'))
    data_array = DataArray([[1, -999, -999], [2, 3, -999], [-999, -999, -999]])
    dataset = Dataset(data_vars={'var_one': data_array})

    output_ds = valid_data_mask(dataset)
    assert output_ds.data_vars['var_one'].equals(expected_data_array)

    output_da = valid_data_mask(data_array)
    assert output_da.equals(expected_data_array)

    expected_data_array = DataArray(np.array([[True, False, False], [True, True, False], [False, False, False]],
                                             dtype='bool'))

    data_array = DataArray([[1, -999, -999], [2, 3, -999], [-999, -999, float('nan')]], attrs=attrs)
    dataset = Dataset(data_vars={'var_one': data_array})

    output_ds = valid_data_mask(dataset)
    assert output_ds.data_vars['var_one'].equals(expected_data_array)

    output_da = valid_data_mask(data_array)
    assert output_da.equals(expected_data_array)

    expected_data_array = DataArray(np.array([[True, True, True], [True, True, True], [True, True, False]],
                                             dtype='bool'))

    data_array = DataArray([[1, -999, -999], [2, 3, -999], [-999, -999, float('nan')]])
    dataset = Dataset(data_vars={'var_one': data_array})

    output_ds = valid_data_mask(dataset)
    assert output_ds.data_vars['var_one'].equals(expected_data_array)

    output_da = valid_data_mask(data_array)
    assert output_da.equals(expected_data_array)

    with pytest.raises(TypeError):
        valid_data_mask(([], []))


def test_deprecation():
    from datacube.storage.masking import make_mask as a
    from datacube.utils.masking import make_mask as b
    assert a is b
