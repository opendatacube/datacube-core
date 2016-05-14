"""
Tools for masking data based on a bit-mask variable with attached definition.

The main functions are `make_mask(variable)` `describe_flags(variable)`
"""
FLAGS_ATTR_NAME = 'flags_definition'


def list_flag_names(variable):
    """
    Return the available masking flags for the variable

    :param variable: Masking xarray.Dataset or xarray.DataArray
    :return: list
    """
    variable = _ensure_masking_variable(variable)
    return sorted(list(getattr(variable, FLAGS_ATTR_NAME).keys()))


def describe_flags(variable):
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
    variable = _ensure_masking_variable(variable)
    column_spacing = 3

    def gen_human_readable(flags_def):
        bit_value_flagname_desc = [
            (bitdef['bit_index'], bitdef['value'], flagname, bitdef['description'])
            for flagname, bitdef in flags_def.items()]
        max_bit, _, _, _ = max(bit_value_flagname_desc)
        min_bit, _, _, _ = min(bit_value_flagname_desc)

        widest_flagname = len(max(list(zip(*bit_value_flagname_desc))[2], key=len))

        yield "Bits are listed from the MSB (bit {}) to the LSB (bit {})".format(max_bit, min_bit)
        yield "{:<8}{:<8}{:<{flagname_width}}{}".format('Bit', 'Value', 'Flag Name', 'Description',
                                                        flagname_width=widest_flagname + column_spacing)
        for bit, value, flagname, desc in sorted(bit_value_flagname_desc, reverse=True):
            yield "{:<8d}{:<8d}{:<{flagname_width}}{}".format(bit, value, flagname, desc,
                                                              flagname_width=widest_flagname + column_spacing)

    return '\n'.join(gen_human_readable(getattr(variable, FLAGS_ATTR_NAME)))


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
    variable = _ensure_masking_variable(variable)

    mask, mask_value = create_mask_value(getattr(variable, FLAGS_ATTR_NAME), **flags)

    return variable & mask == mask_value


def create_mask_value(flag_defs, **flags):
    mask = 0
    value = 0

    for flag_name, flag in flags.items():
        defn = flag_defs[flag_name]

        if 'bit_index' in defn:

            mask = set_value_at_index(mask, defn['bit_index'], True)
            value = set_value_at_index(value, defn['bit_index'], bool(flag) == bool(defn['value']))
        elif 'bits' in defn:
            # Set mask
            for bit in defn['bits']:
                mask = set_value_at_index(mask, bit, True)

            shift = min(defn['bits'])
            real_val = defn['value'] << shift

            value |= real_val

    return mask, value


def _ensure_masking_variable(variable):
    if hasattr(variable, FLAGS_ATTR_NAME):
        return variable
    else:
        return _get_masking_array(variable)


def _get_masking_array(dataset):
    for var in dataset.data_vars.values():
        if _is_data_var(var) and hasattr(var, FLAGS_ATTR_NAME):
            return var

    raise ValueError('No masking variable found')


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


def set_value_at_mask(bitmask, new_val, old_value):
    pass


def _is_data_var(variable):
    return variable.name != 'crs' and len(variable.coords) > 1


def is_set(x, n):
    return x & 2 ** n != 0


def all_set(max_bit):
    return (2 << max_bit + 1) - 1
