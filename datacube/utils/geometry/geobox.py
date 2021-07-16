# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
""" Geometric operations on GeoBox class
"""

from typing import Optional, Tuple, Dict, Iterable
import itertools
import math
from affine import Affine

from . import Geometry, GeoBox, BoundingBox
from .tools import align_up
from datacube.utils.math import clamp

# pylint: disable=invalid-name
MaybeInt = Optional[int]
MaybeFloat = Optional[float]


def flipy(geobox: GeoBox) -> GeoBox:
    """
    :returns: GeoBox covering the same region but with Y-axis flipped
    """
    H, W = geobox.shape
    A = Affine.translation(0, H)*Affine.scale(1, -1)
    A = geobox.affine*A
    return GeoBox(W, H, A, geobox.crs)


def flipx(geobox: GeoBox) -> GeoBox:
    """
    :returns: GeoBox covering the same region but with X-axis flipped
    """
    H, W = geobox.shape
    A = Affine.translation(W, 0)*Affine.scale(-1, 1)
    A = geobox.affine*A
    return GeoBox(W, H, A, geobox.crs)


def translate_pix(geobox: GeoBox, tx: float, ty: float) -> GeoBox:
    """
    Shift GeoBox in pixel plane. (0,0) of the new GeoBox will be at the same
    location as pixel (tx, ty) in the original GeoBox.
    """
    H, W = geobox.shape
    A = geobox.affine*Affine.translation(tx, ty)
    return GeoBox(W, H, A, geobox.crs)


def pad(geobox: GeoBox, padx: int, pady: MaybeInt = None) -> GeoBox:
    """
    Expand GeoBox by fixed number of pixels on each side
    """
    pady = padx if pady is None else pady

    H, W = geobox.shape
    A = geobox.affine*Affine.translation(-padx, -pady)
    return GeoBox(W + padx*2, H + pady*2, A, geobox.crs)


def pad_wh(geobox: GeoBox,
           alignx: int = 16,
           aligny: MaybeInt = None) -> GeoBox:
    """
    Expand GeoBox such that width and height are multiples of supplied number.
    """
    aligny = alignx if aligny is None else aligny
    H, W = geobox.shape

    return GeoBox(align_up(W, alignx),
                  align_up(H, aligny),
                  geobox.affine, geobox.crs)


def zoom_out(geobox: GeoBox, factor: float) -> GeoBox:
    """
    factor > 1 --> smaller width/height, fewer but bigger pixels
    factor < 1 --> bigger width/height, more but smaller pixels

    :returns: GeoBox covering the same region but with bigger pixels (i.e. lower resolution)
    """
    from math import ceil

    H, W = (max(1, ceil(s/factor)) for s in geobox.shape)
    A = geobox.affine*Affine.scale(factor, factor)
    return GeoBox(W, H, A, geobox.crs)


def zoom_to(geobox: GeoBox, shape: Tuple[int, int]) -> GeoBox:
    """
    :returns: GeoBox covering the same region but with different number of pixels
              and therefore resolution.
    """
    H, W = geobox.shape
    h, w = shape

    sx, sy = W/float(w), H/float(h)
    A = geobox.affine*Affine.scale(sx, sy)
    return GeoBox(w, h, A, geobox.crs)


def rotate(geobox: GeoBox, deg: float) -> GeoBox:
    """
    Rotate GeoBox around the center.

    It's as if you stick a needle through the center of the GeoBox footprint
    and rotate it counter clock wise by supplied number of degrees.

    Note that from pixel point of view image rotates the other way. If you have
    source image with an arrow pointing right, and you rotate GeoBox 90 degree,
    in that view arrow should point down (this is assuming usual case of inverted
    y-axis)
    """
    h, w = geobox.shape
    c0 = geobox.transform*(w*0.5, h*0.5)
    A = Affine.rotation(deg, c0)*geobox.transform
    return GeoBox(w, h, A, geobox.crs)


def affine_transform_pix(geobox: GeoBox, transform: Affine) -> GeoBox:
    """
    Apply affine transform on pixel side.

    :param transform: Affine matrix mapping from new pixel coordinate space to
    pixel coordinate space of input geobox

    :returns: GeoBox of the same pixel shape but covering different region,
    pixels in the output geobox relate to input geobox via `transform`

    X_old_pix = transform * X_new_pix

    """
    H, W = geobox.shape
    A = geobox.affine*transform
    return GeoBox(W, H, A, geobox.crs)


class GeoboxTiles():
    """ Partition GeoBox into sub geoboxes
    """

    def __init__(self, box: GeoBox, tile_shape: Tuple[int, int]):
        """ Construct from a ``GeoBox``

        :param box: source :class:`datacube.utils.geometry.GeoBox`
        :param tile_shape: Shape of sub-tiles in pixels (rows, cols)
        """
        self._geobox = box
        self._tile_shape = tile_shape
        self._shape = tuple(math.ceil(float(N)/n)
                            for N, n in zip(box.shape, tile_shape))
        self._cache = {}  # type: Dict[Tuple[int, int], GeoBox]

    @property
    def base(self) -> GeoBox:
        return self._geobox

    @property
    def shape(self):
        """ Number of tiles along each dimension
        """
        return self._shape

    def _idx_to_slice(self, idx: Tuple[int, int]) -> Tuple[slice, slice]:
        def _slice(i, N, n) -> slice:
            _in = i*n
            if 0 <= _in < N:
                return slice(_in, min(_in + n, N))
            else:
                raise IndexError("Index ({},{})is out of range".format(*idx))

        ir, ic = (_slice(i, N, n)
                  for i, N, n in zip(idx, self._geobox.shape, self._tile_shape))
        return (ir, ic)

    def chunk_shape(self, idx: Tuple[int, int]) -> Tuple[int, int]:
        """ Chunk shape for a given chunk index.

            :param idx: (row, col) index
            :returns: (nrow, ncols) shape of a tile (edge tiles might be smaller)
            :raises: IndexError when index is outside of [(0,0) -> .shape)
        """
        def _sz(i: int, n: int, tile_sz: int, total_sz: int) -> int:
            if 0 <= i < n - 1:  # not edge tile
                return tile_sz
            elif i == n - 1:    # edge tile
                return total_sz - (i*tile_sz)
            else:               # out of index case
                raise IndexError("Index ({},{}) is out of range".format(*idx))

        n1, n2 = map(_sz, idx, self._shape, self._tile_shape, self._geobox.shape)
        return (n1, n2)

    def __getitem__(self, idx: Tuple[int, int]) -> GeoBox:
        """ Lookup tile by index, index is in matrix access order: (row, col)

            :param idx: (row, col) index
            :returns: GeoBox of a tile
            :raises: IndexError when index is outside of [(0,0) -> .shape)
        """
        sub_geobox = self._cache.get(idx, None)
        if sub_geobox is not None:
            return sub_geobox

        roi = self._idx_to_slice(idx)
        return self._cache.setdefault(idx, self._geobox[roi])

    def range_from_bbox(self, bbox: BoundingBox) -> Tuple[range, range]:
        """ Compute rows and columns overlapping with a given ``BoundingBox``
        """
        def clamped_range(v1: float, v2: float, N: int) -> range:
            _in = clamp(math.floor(v1), 0, N)
            _out = clamp(math.ceil(v2), 0, N)
            return range(_in, _out)

        sy, sx = self._tile_shape
        A = Affine.scale(1.0/sx, 1.0/sy)*(~self._geobox.transform)
        # A maps from X,Y in meters to chunk index
        bbox = bbox.transform(A)

        NY, NX = self.shape
        xx = clamped_range(bbox.left, bbox.right, NX)
        yy = clamped_range(bbox.bottom, bbox.top, NY)
        return (yy, xx)

    def tiles(self, polygon: Geometry) -> Iterable[Tuple[int, int]]:
        """ Return tile indexes overlapping with a given geometry.
        """
        poly = polygon.to_crs(self._geobox.crs)
        yy, xx = self.range_from_bbox(poly.boundingbox)
        for idx in itertools.product(yy, xx):
            geobox = self[idx]
            if geobox.extent.intersects(poly):
                yield idx
