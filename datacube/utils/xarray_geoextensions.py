"""
Add geometric extensions to :class:`xarray.Dataset` and :class:`xarray.DataArray` for use
with Data Cube by Monkey Patching those classes.

This extension is reliant on an `xarray` object having a `.crs` property of type
:class:`datacube.utils.geometry.CRS`. This is used to inspect the spatial dimensions of the
:class:`Dataset` or :class:`DataArray`, and provide new attributes for accessing a
:class:`datacube.utils.geometry.GeoBox`, affine transform and extent for the dataset as
`.geobox`, `.affine` and `.extent` respectively.

"""

import xarray

from datacube.utils import geometry, spatial_dims
from datacube.utils.math import affine_from_axis


def _norm_crs(crs):
    if crs is None or isinstance(crs, geometry.CRS):
        return crs
    elif isinstance(crs, str):
        return geometry.CRS(crs)
    else:
        raise ValueError('Can not interpret {} as CRS'.format(type(crs)))


def _get_crs_from_attrs(obj):
    """ Looks for attribute named `crs` containing CRS string
        1. Checks spatials coords attrs
        2. Checks data variable attrs
        3. Checks dataset attrs

        Returns
        =======
        Content for `.attrs[crs]` usually it's a string
        None if not present in any of the places listed above
    """
    if isinstance(obj, xarray.Dataset):
        if len(obj.data_vars) > 0:
            data_array = next(iter(obj.data_vars.values()))
        else:
            # fall back option
            return obj.attrs.get('crs', None)
    else:
        data_array = obj

    sdims = spatial_dims(data_array, relaxed=True)
    if sdims is not None:
        crs_set = set(data_array[d].attrs.get('crs', None) for d in sdims)
        crs = None
        if len(crs_set) > 1:
            raise ValueError('Spatial dimensions have different crs.')
        elif len(crs_set) == 1:
            crs = crs_set.pop()
    else:
        crs = None

    if crs is None:
        # fall back option
        crs = data_array.attrs.get('crs', None) or obj.attrs.get('crs', None)
    return crs


def _get_crs_from_coord(obj, mode='strict'):
    """ Looks for dimensionless coordinate with `spatial_ref` attribute.

        obj: Dataset | DataArray
        mode: strict|any|all
           strict -- raise Error if multiple candidates
           any    -- return first one
           all    -- return a list of all found CRSs

       Returns
       =======
       None     - if none found
       crs:str  - if found one
       crs:str  - if found several but mode is any

       (crs: str, crs: str) - if found several and mode=all
    """
    grid_mapping = obj.attrs.get('grid_mapping', None)

    # First check CF convention "pointer"
    if grid_mapping is not None and grid_mapping in obj.coords:
        coord = obj.coords[grid_mapping]
        spatial_ref = coord.attrs.get('spatial_ref', None)
        if spatial_ref is not None:
            return spatial_ref
        else:
            raise ValueError(f"Coordinate '{grid_mapping}' has no `spatial_ref` attribute")

    # No explicit `grid_mapping` find some "CRS" coordinate
    candidates = tuple(coord.attrs['spatial_ref'] for coord in obj.coords.values()
                       if coord.ndim == 0 and 'spatial_ref' in coord.attrs)

    if len(candidates) == 0:
        return None
    if len(candidates) == 1:
        return candidates[0]

    if mode == 'strict':
        raise ValueError("Too many candidates when looking for CRS")
    elif mode == 'all':
        return candidates
    elif mode == 'any':
        return candidates[0]
    else:
        raise ValueError(f"Mode needs to be: strict|any|all got {mode}")


def _xarray_affine(obj):
    sdims = spatial_dims(obj, relaxed=True)
    if sdims is None:
        return None

    yy, xx = (obj[dim] for dim in sdims)
    fallback_res = (coord.attrs.get('resolution', None) for coord in (xx, yy))

    return affine_from_axis(xx.values, yy.values, fallback_res)


def _xarray_extent(obj):
    geobox = obj.geobox
    return None if geobox is None else geobox.extent


def _xarray_geobox(obj):
    crs = None
    try:
        crs = _get_crs_from_coord(obj)
    except ValueError:
        pass

    if crs is None:
        try:
            crs = _get_crs_from_attrs(obj)
        except ValueError:
            pass

    if crs is None:
        return None

    try:
        crs = _norm_crs(crs)
    except ValueError:
        return None

    dims = crs.dimensions
    return geometry.GeoBox(obj[dims[1]].size, obj[dims[0]].size, obj.affine, crs)


xarray.Dataset.geobox = property(_xarray_geobox)   # type: ignore
xarray.Dataset.affine = property(_xarray_affine)   # type: ignore
xarray.Dataset.extent = property(_xarray_extent)   # type: ignore
xarray.DataArray.geobox = property(_xarray_geobox) # type: ignore
xarray.DataArray.affine = property(_xarray_affine) # type: ignore
xarray.DataArray.extent = property(_xarray_extent) # type: ignore
