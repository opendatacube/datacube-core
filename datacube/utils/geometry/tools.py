import numpy as np
import collections
from affine import Affine


def polygon_path(x, y=None):
    """A little bit like numpy.meshgrid, except returns only boundary values and
    limited to 2d case only.

    Examples:
      [0,1], [3,4] =>
      array([[0, 1, 1, 0, 0],
             [3, 3, 4, 4, 3]])

      [0,1] =>
      array([[0, 1, 1, 0, 0],
             [0, 0, 1, 1, 0]])
    """

    if y is None:
        y = x

    return np.vstack([
        np.vstack([x, np.full_like(x, y[0])]).T,
        np.vstack([np.full_like(y, x[-1]), y]).T[1:],
        np.vstack([x, np.full_like(x, y[-1])]).T[::-1][1:],
        np.vstack([np.full_like(y, x[0]), y]).T[::-1][1:]]).T


def gbox_boundary(gbox, pts_per_side=16):
    """Return points in pixel space along the perimeter of a GeoBox, or a 2d array.

    """
    H, W = gbox.shape[:2]
    xx = np.linspace(0, W, pts_per_side, dtype='float32')
    yy = np.linspace(0, H, pts_per_side, dtype='float32')

    return polygon_path(xx, yy).T[:-1]


def roi_boundary(roi, pts_per_side=2):
    """
    Get boundary points from a 2d roi.

    roi needs to be in the normalised form, i.e. no open-ended start/stop, see roi_normalise

    :returns: Nx2 float32 array of X,Y points on the perimeter of the envelope defined by `roi`
    """
    yy, xx = roi
    xx = np.linspace(xx.start, xx.stop, pts_per_side, dtype='float32')
    yy = np.linspace(yy.start, yy.stop, pts_per_side, dtype='float32')

    return polygon_path(xx, yy).T[:-1]


def align_down(x, align):
    return x - (x % align)


def align_up(x, align):
    return align_down(x+(align-1), align)


def scaled_down_roi(roi, scale: int):
    return tuple(slice(s.start//scale,
                       align_up(s.stop, scale)//scale) for s in roi)


def scaled_up_roi(roi, scale: int, shape=None):
    roi = tuple(slice(s.start*scale,
                      s.stop*scale) for s in roi)
    if shape is not None:
        roi = tuple(slice(min(dim, s.start),
                          min(dim, s.stop))
                    for s, dim in zip(roi, shape))
    return roi


def scaled_down_shape(shape, scale: int):
    return tuple(align_up(s, scale)//scale for s in shape)


def roi_shape(roi):
    def slice_dim(s):
        return s.stop if s.start is None else s.stop - s.start

    if isinstance(roi, slice):
        roi = (roi,)

    return tuple(slice_dim(s) for s in roi)


def roi_is_empty(roi):
    return any(d <= 0 for d in roi_shape(roi))


def roi_normalise(roi, shape):
    """
    Fill in missing .start/.stop, also deal with negative values, which are
    treated as offsets from the end.

    .step parameter is left unchanged.

    Example:
          np.s_[:3, 4:  ], (10, 20) -> np._s[0:3, 4:20]
          np.s_[:3,  :-3], (10, 20) -> np._s[0:3, 0:17]

    """

    def fill_if_none(x, val_if_none):
        return val_if_none if x is None else x

    def norm_slice(s, n):
        start = fill_if_none(s.start, 0)
        stop = fill_if_none(s.stop, n)
        start, stop = [x if x >= 0 else n+x for x in (start, stop)]
        return slice(start, stop, s.step)

    if not isinstance(shape, collections.Sequence):
        shape = (shape,)

    if isinstance(roi, slice):
        return norm_slice(roi, shape[0])

    return tuple([norm_slice(s, n) for s, n in zip(roi, shape)])


def decompose_rws(A):
    """
    Compute decomposition Affine matrix sans translation into Rotation, Shear and Scale.

    Note: that there are ambiguities for negative scales.

    Example: R(90)*S(1,1) == R(-90)*S(-1,-1),
    (R*(-I))*((-I)*S) == R*S

    A = R W S

    Where:

    R [ca -sa]  W [1, w]  S [sx,  0]
      [sa  ca]    [0, 1]    [ 0, sy]

    :return: Rotation, Sheer, Scale
    """
    # pylint: disable=invalid-name, too-many-locals

    from numpy.linalg import cholesky, det, inv

    if isinstance(A, Affine):
        def to_affine(m, t=(0, 0)):
            a, b, d, e = m.ravel()
            c, f = t
            return Affine(a, b, c,
                          d, e, f)

        (a, b, c,
         d, e, f,
         *_) = A
        R, W, S = decompose_rws(np.asarray([[a, b],
                                            [d, e]], dtype='float64'))

        return to_affine(R, (c, f)), to_affine(W), to_affine(S)

    assert A.shape == (2, 2)

    WS = cholesky(A.T @ A).T
    R = A @ inv(WS)

    if det(R) < 0:
        R[:, -1] *= -1
        WS[-1, :] *= -1

    ss = np.diag(WS)
    S = np.diag(ss)
    W = WS @ np.diag(1.0/ss)

    return R, W, S


def affine_from_pts(X, Y):
    """
    Given points X,Y compute A, such that: Y = A*X.

    Needs at least 3 points.

    :rtype: Affine
    """
    from numpy.linalg import lstsq

    assert len(X) == len(Y)
    assert len(X) >= 3

    n = len(X)

    XX = np.ones((n, 3), dtype='float64')
    YY = np.vstack(Y)
    for i, x in enumerate(X):
        XX[i, :2] = x

    mm, *_ = lstsq(XX, YY, rcond=-1)
    a, d, b, e, c, f = mm.ravel()

    return Affine(a, b, c,
                  d, e, f)


def get_scale_from_linear_transform(A):
    """
    Given a linear transform compute scale change.

    1. Y = A*X + t
    2. Extract scale components of A

    Returns (sx, sy), where sx > 0, sy > 0
    """
    _, _, S = decompose_rws(A)
    return abs(S.a), abs(S.e)


def get_scale_at_point(pt, tr, r=None):
    """
    Given an arbitrary locally linear transform estimate scale change around a point.

    1. Approximate Y = tr(X) as Y = A*X+t in the neighbourhood of pt, for X,Y in R2
    2. Extract scale components of A


    pt - estimate transform around this point
    r  - radius around the point (default 1)

    tr - List((x,y)) -> List((x,y))
         takes list of 2-d points on input and outputs same length list of 2d on output

    Returns (sx, sy), where sx > 0, sy > 0
    """
    pts0 = [(0, 0), (-1, 0), (0, -1), (1, 0), (0, 1)]
    x0, y0 = pt
    if r is None:
        XX = [(float(x+x0), float(y+y0)) for x, y in pts0]
    else:
        XX = [(float(x*r+x0), float(y*r+y0)) for x, y in pts0]
    YY = tr(XX)
    A = affine_from_pts(XX, YY)
    return get_scale_from_linear_transform(A)


def _same_crs_pix_transform(src, dst):
    assert src.crs == dst.crs

    def transorm(pts, A):
        return [A*pt[:2] for pt in pts]

    _fwd = (~dst.transform) * src.transform  # src -> dst
    _bwd = ~_fwd                             # dst -> src

    def pt_tr(pts):
        return transorm(pts, _fwd)
    pt_tr.back = lambda pts: transorm(pts, _bwd)
    pt_tr.back.back = pt_tr
    pt_tr.linear = _fwd
    pt_tr.back.linear = _bwd

    return pt_tr


def native_pix_transform(src, dst):
    """

    direction: from src to dst
    .back: goes the other way
    .linear: None|Affine linear transform src->dst if transform is linear (i.e. same CRS)
    """
    # pylint: disable=invalid-name

    from types import SimpleNamespace
    from ._base import mk_osr_point_transform

    # Special case CRS_in == CRS_out
    if src.crs == dst.crs:
        return _same_crs_pix_transform(src, dst)

    _in = SimpleNamespace(crs=src.crs, A=src.transform)
    _out = SimpleNamespace(crs=dst.crs, A=dst.transform)

    _fwd = mk_osr_point_transform(_in.crs, _out.crs)
    _bwd = mk_osr_point_transform(_out.crs, _in.crs)

    _fwd = (_in.A, _fwd, ~_out.A)
    _bwd = (_out.A, _bwd, ~_in.A)

    def transform(pts, params):
        A, f, B = params
        return [B*pt[:2] for pt in f.TransformPoints([A*pt[:2] for pt in pts])]

    def tr(pts):
        return transform(pts, _fwd)
    tr.back = lambda pts: transform(pts, _bwd)
    tr.back.back = tr
    tr.linear = None
    tr.back.linear = None

    return tr


def roi_intersect(a, b):
    """
    Compute intersection of two ROIs
    """
    def slice_intersect(a, b):
        if a.stop < b.start:
            return slice(a.stop, a.stop)
        elif a.start > b.stop:
            return slice(a.start, a.start)

        _in = max(a.start, b.start)
        _out = min(a.stop, b.stop)

        return slice(_in, _out)

    if isinstance(a, slice):
        if not isinstance(b, slice):
            b = b[0]
        return slice_intersect(a, b)

    b = (b,) if isinstance(b, slice) else b

    return tuple(slice_intersect(sa, sb) for sa, sb in zip(a, b))


def roi_center(roi):
    """ Return center point of roi
    """
    def slice_center(s):
        return (s.start + s.stop)*0.5

    if isinstance(roi, slice):
        return slice_center(roi)

    return tuple(slice_center(s) for s in roi)


def roi_from_points(xy, shape, padding=0, align=None):
    """
    Compute envelope around a bunch of points and return it as roi (tuple of
    row/col slices)

    Returned roi is clipped (0,0) --> shape, so it won't stick outside of the
    valid region.
    """
    def to_roi(*args):
        return tuple(slice(v[0], v[1]) for v in args)

    assert len(shape) == 2
    assert xy.ndim == 2 and xy.shape[1] == 2

    ny, nx = shape

    _in = np.floor(xy.min(axis=0)).astype('int32') - padding
    _out = np.ceil(xy.max(axis=0)).astype('int32') + padding

    if align is not None:
        _in = align_down(_in, align)
        _out = align_up(_out, align)

    xx = np.asarray([_in[0], _out[0]])
    yy = np.asarray([_in[1], _out[1]])

    xx = np.clip(xx, 0, nx, out=xx)
    yy = np.clip(yy, 0, ny, out=yy)

    return to_roi(yy, xx)


def compute_reproject_roi(src, dst, padding=1, align=None):
    """
    Given two GeoBoxes find the region within the source GeoBox that overlaps
    with the destination GeoBox, and also compute the scale factor (>1 means
    shrink). Scale is chosen such that if you apply it to the source image
    before reprojecting, then reproject will have roughly no scale component.

    So we breaking up reprojection into two stages:

    1. Scale in the native pixel CRS
    2. Reprojection (possibly non-linear with CRS change)

    - src[roi] -> scale      -> reproject -> dst  (using native pixels)
    - src(scale)[roi(scale)] -> reproject -> dst  (using overview image)

    Here roi is "minimal", padding is configurable though, so you only read what you need.
    Also scale can be used to pick the right kind of overview level to read.

    Applying reprojection in two steps allows us to use pre-computed overviews,
    particularly useful when shrink factor is large. But even for data sources
    without overviews there are advantages for shrinking source image before
    applying reprojection: mainly quality of the output (reduces aliasing for
    large shrink factors), improved efficiency of the computation is likely as
    well.

    Also compute and return ROI of the dst geobox that is affected by src.

    :returns: (roi_src: (slice, slice), scale: float, roi_dst: (slice, slice))

    """
    pts_per_side = 5

    tr = native_pix_transform(src, dst)

    if tr.linear is not None:
        pts_per_side = 2

    XY = np.vstack(tr.back(gbox_boundary(dst, pts_per_side)))
    roi_src = roi_from_points(XY, src.shape, padding, align=align)

    # project src roi back into dst and compute roi from that
    xy = np.vstack(tr(roi_boundary(roi_src, pts_per_side)))
    roi_dst = roi_from_points(xy, dst.shape, padding=0)  # no need to add padding twice

    if tr.linear is not None:
        scale = get_scale_from_linear_transform(tr.linear)
    else:
        center_pt = roi_center(roi_src)
        scale = get_scale_at_point(center_pt, tr)

    scale = min(1/s for s in scale)

    return roi_src, scale, roi_dst
