# coding=utf-8
import yaml

from datacube.storage.masking import list_flag_names, create_mask_value, describe_variable_flags
from datacube.storage.masking import mask_to_dict


def test_list_flag_names():
    simple_var = SimpleVariableWithFlagsDef()
    flags = list_flag_names(simple_var)
    for flag_name in simple_var.flags_definition.keys():
        assert flag_name in flags


def test_create_mask_value():
    simple_var = SimpleVariableWithFlagsDef()
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

    assert create_mask_value(multi_flags_def, water_confidence='water', filled=True) == (0b011001, 0b011001)
    assert create_mask_value(multi_flags_def, water_confidence='not_determined') == (0b011000, 0b0)
    assert create_mask_value(multi_flags_def, water_confidence='no_water') == (0b11000, 0b01000)
    assert create_mask_value(multi_flags_def, veg_confidence='maybe_veg') == (0b110000000, 0b100000000)
    assert create_mask_value(multi_flags_def,
                             veg_confidence='maybe_veg',
                             water_confidence='water') == (0b110011000, 0b100011000)
    assert create_mask_value(multi_flags_def,
                             veg_confidence='maybe_veg',
                             water_confidence='water', filled=True) == (0b110011001, 0b100011001)

    assert create_mask_value(multi_flags_def, water_confidence='maybe_water') == (0b011000, 0b10000)


def test_ga_good_pixel():
    simple_var = SimpleVariableWithFlagsDef()
    bits_def = simple_var.flags_definition

    assert create_mask_value(bits_def, ga_good_pixel=True) == (16383, 16383)


def test_describe_flags():
    simple_var = SimpleVariableWithFlagsDef()
    describe_variable_flags(simple_var)


def test_simple_mask_to_dict():
    simple_var = SimpleVariableWithFlagsDef()
    bits_def = simple_var.flags_definition

    contiguous_true = mask_to_dict(bits_def, 256)
    assert contiguous_true['contiguous']

    test_dict = mask_to_dict(bits_def, 768)
    assert test_dict['contiguous']
    assert test_dict['land_sea'] == 'land'

    test_dict = mask_to_dict(bits_def, 16383)
    assert test_dict['ga_good_pixel']


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
          bits: 1
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

    def __init__(self):
        self.flags_definition = yaml.load(self.bits_def_yaml)


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

    def __init__(self):
        self.flags_definition = yaml.load(self.bits_def_yaml)
