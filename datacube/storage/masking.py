"""
Tools for masking data based on a bit-mask variable with attached definition.

The main functions are `make_mask(variable)` `describe_flags(variable)`
"""
import collections

from datacube.utils import generate_table

FLAGS_ATTR_NAME = 'flags_definition'


def list_flag_names(variable):
    """
    Return the available masking flags for the variable

    :param variable: Masking xarray.Dataset or xarray.DataArray
    :return: list
    """
    flags_def = get_flags_def(variable)
    return sorted(list(flags_def.keys()))


def describe_variable_flags(variable):
    """
    Return a string describing the available flags for a masking variable

    Interprets the `flags_definition` attribute on the provided variable and returns
    a string like:

    `
    Bits are listed from the MSB (bit 13) to the LSB (bit 0)
    Bit     Value   Flag Name            Description
    13      0       cloud_shadow_fmask   Cloud Shadow (Fmask)
    12      0       cloud_shadow_acca    Cloud Shadow (ACCA)
    11      0       cloud_fmask          Cloud (Fmask)
    10      0       cloud_acca           Cloud (ACCA)
    `

    :param variable: Masking xarray.Dataset or xarray.DataArray
    :return: str
    """
    flags_def = get_flags_def(variable)

    return describe_flags_def(flags_def)


def describe_flags_def(flags_def):
    return '\n'.join(generate_table(list(_table_contents(flags_def))))


def _table_contents(flags_def):
    yield 'Flag name', 'Description', 'Bit. No', 'Value', 'Meaning'
    for name, defn in sorted(flags_def.items(), key=_order_bitdefs_by_bits):
        name, desc = name, defn['description']
        for value, meaning in defn['values'].items():
            yield name, desc, str(defn['bits']), str(value), str(meaning)
            name, desc = '', ''


def _order_bitdefs_by_bits(bitdef):
    name, defn = bitdef
    try:
        return min(defn['bits'])
    except TypeError:
        return defn['bits']


def make_mask(variable, **flags):
    """
    Return a mask array, based on provided flags

    For example:

    make_mask(pqa, cloud_acca=False, cloud_fmask=False, land_obs=True)

    OR

    make_mask(pqa, **GOOD_PIXEL_FLAGS)

    where GOOD_PIXEL_FLAGS is a dict of flag_name to True/False

    :param variable:
    :type variable: xarray.Dataset or xarray.DataArray
    :param flags: list of boolean flags
    :return:
    """
    flags_def = get_flags_def(variable)

    mask, mask_value = create_mask_value(flags_def, **flags)

    return variable & mask == mask_value


def create_mask_value(bits_def, **flags):
    mask = 0
    value = 0

    for flag_name, flag_ref in flags.items():
        defn = bits_def[flag_name]

        try:
            [flag_value] = (bit_val
                            for bit_val, val_ref in defn['values'].items()
                            if val_ref == flag_ref)
            flag_value = int(flag_value)  # Might be string if coming from DB
        except ValueError:
            raise ValueError('Unknown value %s specified for flag %s' %
                             (flag_ref, flag_name))

        if isinstance(defn['bits'], collections.Iterable):  # Multi-bit flag
            # Set mask
            for bit in defn['bits']:
                mask = set_value_at_index(mask, bit, True)

            shift = min(defn['bits'])
            real_val = flag_value << shift

            value |= real_val

        else:
            bit = defn['bits']
            mask = set_value_at_index(mask, bit, True)
            value = set_value_at_index(value, bit, flag_value)

    return mask, value


def mask_to_dict(bits_def, mask_value):
    """
    Describe which flags are set for a mask value

    :param bits_def:
    :param mask_value:
    :return: Mapping of flag_name -> set_value
    :rtype: dict
    """
    return_dict = {}
    for flag_name, flag_defn in bits_def.items():
        shift = _get_minimum_bit(flag_defn['bits'])

        for flag_value, value in flag_defn['values'].items():
            shifted_value = int(flag_value) << shift
            if mask_value & shifted_value == shifted_value:
                return_dict[flag_name] = value
    return return_dict


def _get_minimum_bit(bit_or_bits):
    try:
        return min(bit_or_bits)
    except TypeError:
        return bit_or_bits


def get_flags_def(variable):
    try:
        return getattr(variable, FLAGS_ATTR_NAME)
    except AttributeError:
        # Maybe we have a DataSet, not a DataArray
        for var in variable.data_vars.values():
            if _is_data_var(var):
                try:
                    return getattr(var, FLAGS_ATTR_NAME)
                except AttributeError:
                    pass

        raise ValueError('No masking variable found')


def _is_data_var(variable):
    return variable.name != 'crs' and len(variable.coords) > 1


def set_value_at_index(bitmask, index, value):
    """
    Set a bit value onto an integer bitmask

    eg. set bits 2 and 4 to True
    >>> mask = 0
    >>> mask = set_value_at_index(mask, 2, True)
    >>> mask = set_value_at_index(mask, 4, True)
    >>> print(bin(mask))
    0b10100
    >>> mask = set_value_at_index(mask, 2, False)
    >>> print(bin(mask))
    0b10000

    :param bitmask: existing int bitmask to alter
    :type bitmask: int
    :type index: int
    :type value: bool
    """
    bit_val = 2 ** index
    if value:
        bitmask |= bit_val
    else:
        bitmask &= (~bit_val)
    return bitmask
