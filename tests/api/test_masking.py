# coding=utf-8
from datacube.api.masking import list_flag_names, create_mask_value


def test_list_flag_names():
    flags = list_flag_names(SimpleVariableWithFlagsDef)
    for flag_name in SimpleVariableWithFlagsDef.flags_definition.keys():
        assert flag_name in flags


def test_create_mask_value():
    flags_def = SimpleVariableWithFlagsDef.flags_definition

    assert create_mask_value(flags_def, contiguity=True) == (256, 256)
    assert create_mask_value(flags_def, contiguity=False) == (256, 0)
    assert create_mask_value(flags_def, contiguity=False, sea_obs=False) == (768, 512)

    multi_flags_def = VariableWithMultiBitFlags.flags_definition

    assert create_mask_value(multi_flags_def, filled=True) == (1, 1)
    assert create_mask_value(multi_flags_def, water=True) == (0b011000, 0b011000)

    assert create_mask_value(multi_flags_def, water=True, filled=True) == (0b011001, 0b011001)
    assert create_mask_value(multi_flags_def, undetermined_water=True) == (0b011000, 0b0)
    assert create_mask_value(multi_flags_def, no_water=True) == (0b11000, 0b01000)
    assert create_mask_value(multi_flags_def, maybe_veg=True) == (0b110000000, 0b100000000)
    assert create_mask_value(multi_flags_def, maybe_veg=True, water=True) == (0b110011000, 0b100011000)
    assert create_mask_value(multi_flags_def,
                             maybe_veg=True,
                             water=True, filled=True) == (0b110011001, 0b100011001)


class SimpleVariableWithFlagsDef(object):
    flags_definition = {
        'band_1_saturated': {
            'bit_index': 0,
            'description': 'Band 1 is saturated',
            'value': 0},
        'band_2_saturated': {
            'bit_index': 1,
            'description': 'Band 2 is saturated',
            'value': 0},
        'band_3_saturated': {
            'bit_index': 2,
            'description': 'Band 3 is saturated',
            'value': 0},
        'band_4_saturated': {
            'bit_index': 3,
            'description': 'Band 4 is saturated',
            'value': 0},
        'band_5_saturated': {
            'bit_index': 4,
            'description': 'Band 5 is saturated',
            'value': 0},
        'band_6_1_saturated': {
            'bit_index': 5,
            'description': 'Band 6-1 is saturated',
            'value': 0},
        'band_6_2_saturated': {
            'bit_index': 6,
            'description': 'Band 6-2 is saturated',
            'value': 0},
        'band_7_saturated': {
            'bit_index': 7,
            'description': 'Band 7 is saturated',
            'value': 0},
        'cloud_acca': {
            'bit_index': 10, 'description': 'Cloud (ACCA)', 'value': 0},
        'cloud_fmask':
            {'bit_index': 11, 'description': 'Cloud (Fmask)', 'value': 0},
        'cloud_shadow_acca': {
            'bit_index': 12,
            'description': 'Cloud Shadow (ACCA)',
            'value': 0},
        'cloud_shadow_fmask': {
            'bit_index': 13,
            'description': 'Cloud Shadow (Fmask)',
            'value': 0},
        'contiguity': {
            'bit_index': 8,
            'description': 'All bands for this pixel contain non-null values',
            'value': 1},
        'land_obs': {'bit_index': 9, 'description': 'Land observation', 'value': 1},
        'sea_obs': {'bit_index': 9, 'description': 'Sea observation', 'value': 0}}


class VariableWithMultiBitFlags(object):
    flags_definition = {
        'cirrus': {'bits': [11, 12], 'description': 'Cirrus', 'value': 3},
        'cloud': {'bits': [13, 14], 'description': 'Cloud', 'value': 3},
        'filled': {'bit_index': 0, 'description': 'Filled', 'value': 1},
        'frame_dropped': {'bit_index': 1, 'description': 'Frame dropped', 'value': 1},
        'frame_not_dropped': {'bit_index': 1,
                              'description': 'Frame not dropped',
                              'value': 0},
        'maybe_cirrus': {'bits': [11, 12], 'description': 'Maybe cirrus', 'value': 2},
        'maybe_cloud': {'bits': [13, 14], 'description': 'Maybe cloud', 'value': 2},
        'maybe_snowice': {'bits': [9, 10],
                          'description': 'Maybe snow/ice',
                          'value': 2},
        'maybe_veg': {'bits': [7, 8], 'description': 'Maybe vegetation', 'value': 2},
        'maybe_water': {'bits': [3, 4], 'description': 'Maybe water', 'value': 2},
        'no_cirrus': {'bits': [11, 12], 'description': 'No cirrus', 'value': 1},
        'no_cloud': {'bits': [13, 14], 'description': 'No cloud', 'value': 1},
        'no_snowice': {'bits': [9, 10], 'description': 'No snow/ice', 'value': 1},
        'no_veg': {'bits': [7, 8], 'description': 'No vegetation', 'value': 1},
        'no_water': {'bits': [3, 4], 'description': 'No water', 'value': 1},
        'not_filled': {'bit_index': 0, 'description': 'Not filled', 'value': 0},
        'snowice': {'bits': [9, 10], 'description': 'Snow/ice', 'value': 3},
        'terrain_not_occluded': {'bit_index': 2,
                                 'description': 'Terrain occluded',
                                 'value': 1},
        'undetermined_cirrus': {'bits': [11, 12],
                                'description': 'Cirrus not determined',
                                'value': 0},
        'undetermined_cloud': {'bits': [13, 14],
                               'description': 'Cloud not determined',
                               'value': 0},
        'undetermined_snowice': {'bits': [9, 10],
                                 'description': 'Snow/ice not determined',
                                 'value': 0},
        'undetermined_veg': {'bits': [7, 8],
                             'description': 'Vegetation not determined',
                             'value': 0},
        'undetermined_water': {'bits': [3, 4],
                               'description': 'Water not determined',
                               'value': 0},
        'veg': {'bits': [7, 8], 'description': 'Vegetation', 'value': 3},
        'water': {'bits': [3, 4], 'description': 'Water', 'value': 3}}
