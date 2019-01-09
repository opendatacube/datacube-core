"""
Tools for masking data based on a bit-mask variable with attached definition.

The main functions are `make_mask(variable)` `describe_flags(variable)`
"""

import collections
import warnings
import pandas

from xarray import DataArray, Dataset

FLAGS_ATTR_NAME = 'flags_definition'


def list_flag_names(variable):
    """
    Returns the available masking flags for the variable

    :param variable: Masking xarray.Dataset or xarray.DataArray
    :return: list
    """
    flags_def = get_flags_def(variable)
    return sorted(list(flags_def.keys()))


def describe_variable_flags(variable, with_pandas=True):
    """
    Returns either a Pandas Dataframe (with_pandas=True - default) or a string
    (with_pandas=False) describing the available flags for a masking variable

    Interprets the `flags_definition` attribute on the provided variable and
    returns a Pandas Dataframe or string like::

        Bits are listed from the MSB (bit 13) to the LSB (bit 0)
        Bit     Value   Flag Name            Description
        13      0       cloud_shadow_fmask   Cloud Shadow (Fmask)
        12      0       cloud_shadow_acca    Cloud Shadow (ACCA)
        11      0       cloud_fmask          Cloud (Fmask)
        10      0       cloud_acca           Cloud (ACCA)

    :param variable: Masking xarray.Dataset or xarray.DataArray
    :return: Pandas Dataframe or str
    """
    flags_def = get_flags_def(variable)

    if not with_pandas:
        return describe_flags_def(flags_def)

    return pandas.DataFrame.from_dict(flags_def, orient='index')


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
    Returns a mask array, based on provided flags

    When multiple flags are provided, they will be combined in a logical AND fashion.

    For example:

    >>> make_mask(pqa, cloud_acca=False, cloud_fmask=False, land_obs=True) # doctest: +SKIP

    OR

    >>> make_mask(pqa, **GOOD_PIXEL_FLAGS) # doctest: +SKIP

    where `GOOD_PIXEL_FLAGS` is a dict of flag_name to True/False

    :param variable:
    :type variable: xarray.Dataset or xarray.DataArray
    :param flags: list of boolean flags
    :return: boolean xarray.DataArray or xarray.Dataset
    """
    flags_def = get_flags_def(variable)

    mask, mask_value = create_mask_value(flags_def, **flags)

    return variable & mask == mask_value


def valid_data_mask(data):
    """
    Returns bool arrays where the data is not `nodata`

    :param Dataset or DataArray data:
    :return: Dataset or DataArray
    """
    if isinstance(data, Dataset):
        return data.apply(valid_data_mask)

    if isinstance(data, DataArray):
        if 'nodata' not in data.attrs:
            return True
        return data != data.nodata

    raise TypeError('valid_data_mask not supported for type {}'.format(type(data)))


def mask_valid_data(data, keep_attrs=True):
    """
    Deprecated. This function was poorly named. It is now available as `mask_invalid_data`.
    """
    warnings.warn("`mask_valid_data` has been renamed to `mask_invalid_data`. Please use that instead.",
                  DeprecationWarning)
    return mask_invalid_data(data, keep_attrs=keep_attrs)


def mask_invalid_data(data, keep_attrs=True):
    """
    Sets all `nodata` values to ``nan``.

    This will convert numeric data to type `float`.

    :param Dataset or DataArray data:
    :param bool keep_attrs: If the attributes of the data should be included in the returned .
    :return: Dataset or DataArray
    """
    if isinstance(data, Dataset):
        # Pass keep_attrs as a positional arg to the DataArray func
        return data.apply(mask_invalid_data, keep_attrs=keep_attrs, args=(keep_attrs,))

    if isinstance(data, DataArray):
        if 'nodata' not in data.attrs:
            return data
        out_data_array = data.where(data != data.nodata)
        if keep_attrs:
            out_data_array.attrs = data.attrs
        return out_data_array

    raise TypeError('mask_invalid_data not supported for type {}'.format(type(data)))


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
    Describes which flags are set for a mask value

    :param bits_def:
    :param mask_value:
    :return: Mapping of flag_name -> set_value
    :rtype: dict
    """
    return_dict = {}
    for flag_name, flag_defn in bits_def.items():

        # Make bits a list, even if there is only one
        flag_bits = flag_defn['bits']
        if not isinstance(flag_defn['bits'], list):
            flag_bits = [flag_bits]

        # The amount to shift flag_value to line up with mask_value
        flag_shift = min(flag_bits)

        # Mask our mask_value, we are only interested in the bits for this flag
        flag_mask = 0
        for i in flag_bits:
            flag_mask |= (1 << i)
        masked_mask_value = mask_value & flag_mask

        for flag_value, value in flag_defn['values'].items():
            shifted_value = int(flag_value) << flag_shift
            if shifted_value == masked_mask_value:
                assert flag_name not in return_dict
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


def generate_table(rows):
    """
    Yield strings to print a table using the data in `rows`.

    TODO: Maybe replace with Pandas

    :param rows: A sequence of sequences with the 0th element being the table
                 header
    """

    # - figure out column widths
    widths = [len(max(columns, key=len)) for columns in zip(*rows)]

    # - print the header
    header, data = rows[0], rows[1:]
    yield (
        ' | '.join(format(title, "%ds" % width) for width, title in zip(widths, header))
    )

    # Print the separator
    first_col = ''
    # - print the data
    for row in data:
        if first_col == '' and row[0] != '':
            # - print the separator
            yield '-+-'.join('-' * width for width in widths)
        first_col = row[0]

        yield (
            " | ".join(format(cdata, "%ds" % width) for width, cdata in zip(widths, row))
        )
