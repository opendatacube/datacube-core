# coding=utf-8
from datacube.api.masking import list_flag_names, _create_mask_value


def test_list_flag_names():
    flags = list_flag_names(SimpleVariableWithFlagsDef)
    for flag_name in SimpleVariableWithFlagsDef.flags_definition.keys():
        assert flag_name in flags


def test_create_mask_value():
    flags_def = SimpleVariableWithFlagsDef.flags_definition

    assert _create_mask_value(flags_def, contiguity=True) == (256, 256)
    assert _create_mask_value(flags_def, contiguity=False) == (256, 0)
    assert _create_mask_value(flags_def, contiguity=False, sea_obs=False) == (768, 512)


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
