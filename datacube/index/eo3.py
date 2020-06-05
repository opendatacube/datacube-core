""" Tools for working with EO3 metadata
"""
from types import SimpleNamespace
from affine import Affine
import toolz
from typing import Dict, Any, Optional

from datacube.utils.geometry import (
    SomeCRS,
    CRS,
    Geometry,
    polygon,
    bbox_union,
    CoordList,
    BoundingBox,
    lonlat_bounds,
)

EO3_SCHEMA = "https://schemas.opendatacube.org/dataset"


def _norm_grid(grid: Dict[str, Any]) -> Any:
    shape = grid.get('shape')
    transform = grid.get('transform')
    if shape is None or transform is None:
        raise ValueError("Each grid must have .shape and .transform")
    return SimpleNamespace(shape=shape,
                           transform=Affine(*transform[:6]))


def grid2points(grid: Dict[str, Any],
                ring: bool = False) -> CoordList:
    grid = _norm_grid(grid)

    ny, nx = (float(dim) for dim in grid.shape)
    transform = grid.transform
    pts = [(0.0, 0.0), (nx, 0.0), (nx, ny), (0.0, ny)]
    if ring:
        pts += pts[:1]
    return [transform*pt for pt in pts]


def grid2ref_points(grid: Dict[str, Any]) -> Dict[str, Any]:
    nn = ['ul', 'ur', 'lr', 'll']
    return {n: dict(x=x, y=y)
            for n, (x, y) in zip(nn, grid2points(grid))}


def grid2polygon(grid: Dict[str, Any], crs: SomeCRS) -> Geometry:
    return polygon(grid2points(grid, ring=True), crs)


def eo3_lonlat_bbox(doc: Dict[str, Any],
                    resolution: Optional[float] = None) -> BoundingBox:
    """ Compute bounding box in Lon/Lat for a given EO3 document.
    """
    crs = doc.get('crs')
    grids = doc.get('grids')

    if crs is None or grids is None:
        raise ValueError("Input must have crs and grids")

    crs = CRS(crs)
    geom = doc.get('geometry', None)
    if geom is not None:
        geom = Geometry(geom, crs)
        return lonlat_bounds(geom, resolution=resolution)

    bounds = [lonlat_bounds(grid2polygon(grid, crs), resolution=resolution)
              for grid in grids.values()]

    return bbox_union(bounds)


def eo3_grid_spatial(doc: Dict[str, Any],
                     resolution: Optional[float] = None) -> Dict[str, Any]:
    """Using doc[grids|crs|geometry] compute EO3 style grid spatial:

    Note that `geo_ref_points` are set to the 4 corners of the default grid
    only, while lon/lat bounds are computed using all the grids, unless tighter
    valid region is defined via `geometry` key, in which case it is used to
    determine lon/lat bounds instead.

    inputs:
    ```
    crs: "<:str>"
    geometry: <:GeoJSON object>  # optional
    grids:
       default:
          shape: [ny: int, nx: int]
          transform: [a0, a1, a2, a3, a4, a5, 0, 0, 1]
       <...> # optionally more grids
    ```

    Where transform is a linear mapping matrix from pixel space to projected
    space encoded in row-major order:

       [X]   [a0, a1, a2] [ Pixel]
       [Y] = [a3, a4, a5] [ Line ]
       [1]   [ 0,  0,  1] [  1   ]

    outputs:
    ```
      extent:
        lat: {begin=<>, end=<>}
        lon: {begin=<>, end=<>}

      grid_spatial:
        projection:
          spatial_reference: "<crs>"
          geo_ref_points: {ll: {x:<>, y:<>}, ...}
          valid_data: {...}
    ```

    """
    grid = toolz.get_in(['grids', 'default'], doc, None)
    crs = doc.get('crs', None)
    if crs is None or grid is None:
        raise ValueError("Input must have crs and grids.default")

    geometry = doc.get('geometry')

    if geometry is not None:
        valid_data = dict(valid_data=geometry)
    else:
        valid_data = {}

    oo = dict(grid_spatial=dict(projection={
        'spatial_reference': crs,
        'geo_ref_points': grid2ref_points(grid),
        **valid_data,
    }))

    x1, y1, x2, y2 = eo3_lonlat_bbox(doc, resolution=resolution)
    oo['extent'] = dict(lon=dict(begin=x1, end=x2),
                        lat=dict(begin=y1, end=y2))
    return oo


def add_eo3_parts(doc: Dict[str, Any],
                  resolution: Optional[float] = None) -> Dict[str, Any]:
    """Add spatial keys the DB requires to eo3 metadata
    """
    return dict(**doc,
                **eo3_grid_spatial(doc, resolution=resolution))


def is_doc_eo3(doc: Dict[str, Any]) -> bool:
    """ Is this document eo3?

    :param doc: Parsed ODC Dataset metadata document

    :returns:
        False if this document is a legacy dataset
        True if this document is eo3

    :raises ValueError: For an unsupported document
    """
    schema = doc.get('$schema')
    # All legacy documents had no schema at all.
    if schema is None:
        return False

    if schema == EO3_SCHEMA:
        return True

    # Otherwise it has an unknown schema.
    #
    # Reject it for now.
    # We don't want future documents (like Stac items, or "eo4") to be quietly
    # accepted as legacy eo.
    raise ValueError(f'Unsupported dataset schema: {schema!r}')


def prep_eo3(doc: Dict[str, Any],
             auto_skip: bool = False,
             resolution: Optional[float] = None) -> Dict[str, Any]:
    """ Modify spatial and lineage sections of eo3 metadata
    :param doc: input document
    :param auto_skip: If true check if dataset is EO3 and if not
                      silently return input dataset without modifications
    """
    if doc is None:
        return None

    if auto_skip:
        if not is_doc_eo3(doc):
            return doc

    doc = add_eo3_parts(doc, resolution=resolution)
    lineage = doc.pop('lineage', {})

    def remap_lineage(name, uuids) -> Dict[str, Any]:
        """ Turn name, [uuid] -> {name: {id: uuid}}
        """
        if len(uuids) == 0:
            return {}
        if len(uuids) == 1:
            return {name: {'id': uuids[0]}}

        out = {}
        for idx, uuid in enumerate(uuids, start=1):
            out[name+str(idx)] = {'id': uuid}
        return out

    sources = {}
    for name, uuids in lineage.items():
        sources.update(remap_lineage(name, uuids))

    doc['lineage'] = dict(source_datasets=sources)
    return doc
