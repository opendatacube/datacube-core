# coding=utf-8
"""
Useful methods for tests (particularly: reading/writing and checking files)
"""
import atexit
import os
import shutil
import tempfile
import json
import uuid
import numpy as np
from datetime import datetime
from collections import Sequence, Mapping
import pathlib

from datacube.model import Dataset, DatasetType, MetadataType
from datacube.ui.common import get_metadata_path
from datacube.utils import read_documents, SimpleDocNav

from datacube.model.fields import parse_search_field

_DEFAULT = object()


def assert_file_structure(folder, expected_structure, root=''):
    """
    Assert that the contents of a folder (filenames and subfolder names recursively)
    match the given nested dictionary structure.

    :type folder: pathlib.Path
    :type expected_structure: dict[str,str|dict]
    """

    expected_filenames = set(expected_structure.keys())
    actual_filenames = {f.name for f in folder.iterdir()}

    if expected_filenames != actual_filenames:
        missing_files = expected_filenames - actual_filenames
        missing_text = 'Missing: %r' % (sorted(list(missing_files)))
        extra_files = actual_filenames - expected_filenames
        added_text = 'Extra  : %r' % (sorted(list(extra_files)))
        raise AssertionError('Folder mismatch of %r\n\t%s\n\t%s' % (root, missing_text, added_text))

    for k, v in expected_structure.items():
        id_ = '%s/%s' % (root, k) if root else k

        f = folder.joinpath(k)
        if isinstance(v, Mapping):
            assert f.is_dir(), "%s is not a dir" % (id_,)
            assert_file_structure(f, v, id_)
        elif isinstance(v, (str, Sequence)):
            assert f.is_file(), "%s is not a file" % (id_,)
        else:
            assert False, "Only strings|[strings] and dicts expected when defining a folder structure."


def write_files(file_dict):
    """
    Convenience method for writing a bunch of files to a temporary directory.

    Dict format is "filename": "text content"

    If content is another dict, it is created recursively in the same manner.

    writeFiles({'test.txt': 'contents of text file'})

    :type file_dict: dict
    :rtype: pathlib.Path
    :return: Created temporary directory path
    """
    containing_dir = tempfile.mkdtemp(suffix='neotestrun')
    _write_files_to_dir(containing_dir, file_dict)

    def remove_if_exists(path):
        if os.path.exists(path):
            shutil.rmtree(path)

    atexit.register(remove_if_exists, containing_dir)
    return pathlib.Path(containing_dir)


def _write_files_to_dir(directory_path, file_dict):
    """
    Convenience method for writing a bunch of files to a given directory.

    :type directory_path: str
    :type file_dict: dict
    """
    for filename, contents in file_dict.items():
        path = os.path.join(directory_path, filename)
        if isinstance(contents, Mapping):
            os.mkdir(path)
            _write_files_to_dir(path, contents)
        else:
            with open(path, 'w') as f:
                if isinstance(contents, str):
                    f.write(contents)
                elif isinstance(contents, Sequence):
                    f.writelines(contents)
                else:
                    raise ValueError('Unexpected file contents: %s' % type(contents))


def isclose(a, b, rel_tol=1e-09, abs_tol=0.0):
    """
    Testing aproximate equality for floats
    See https://docs.python.org/3/whatsnew/3.5.html#pep-485-a-function-for-testing-approximate-equality
    """
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def geobox_to_gridspatial(geobox):
    if geobox is None:
        return {}

    l, b, r, t = geobox.extent.boundingbox
    return {"grid_spatial": {
        "projection": {
            "geo_ref_points": {
                "ll": {"x": l, "y": b},
                "lr": {"x": r, "y": b},
                "ul": {"x": l, "y": t},
                "ur": {"x": r, "y": t}},
            "spatial_reference": str(geobox.crs)}}}


def mk_sample_product(name,
                      description='Sample',
                      measurements=('red', 'green', 'blue'),
                      with_grid_spec=False,
                      storage=None):

    if storage is None and with_grid_spec is True:
        storage = {'crs': 'EPSG:3577',
                   'resolution': {'x': 25, 'y': -25},
                   'tile_size': {'x': 100000.0, 'y': 100000.0}}

    eo_type = MetadataType({
        'name': 'eo',
        'description': 'Sample',
        'dataset': dict(
            id=['id'],
            label=['ga_label'],
            creation_time=['creation_dt'],
            measurements=['image', 'bands'],
            sources=['lineage', 'source_datasets'],
            format=['format', 'name'],
            grid_spatial=['grid_spatial', 'projection'],
        )
    }, dataset_search_fields={
        'time': parse_search_field({
            'type': 'datetime-range',
            'min_offset': [['time']],
            'max_offset': [['time']],
        }),
    })

    common = dict(dtype='int16',
                  nodata=-999,
                  units='1',
                  aliases=[])

    def mk_measurement(m):
        if isinstance(m, str):
            return dict(name=m, **common)
        elif isinstance(m, tuple):
            name, dtype, nodata = m
            m = common.copy()
            m.update(name=name, dtype=dtype, nodata=nodata)
            return m
        elif isinstance(m, dict):
            m_merged = common.copy()
            m_merged.update(m)
            return m_merged
        else:
            raise ValueError('Only support str|dict|(name, dtype, nodata)')

    measurements = [mk_measurement(m) for m in measurements]

    definition = dict(
        name=name,
        description=description,
        metadata_type='eo',
        metadata={},
        measurements=measurements
    )

    if storage is not None:
        definition['storage'] = storage

    return DatasetType(eo_type, definition)


def mk_sample_dataset(bands,
                      uri='file:///tmp',
                      product_name='sample',
                      format='GeoTiff',
                      timestamp=None,
                      id='3a1df9e0-8484-44fc-8102-79184eab85dd',
                      geobox=None,
                      product_opts=None):
    # pylint: disable=redefined-builtin
    image_bands_keys = 'path layer band'.split(' ')
    measurement_keys = 'dtype units nodata aliases name'.split(' ')

    def with_keys(d, keys):
        return dict((k, d[k]) for k in keys if k in d)

    measurements = [with_keys(m, measurement_keys) for m in bands]
    image_bands = dict((m['name'], with_keys(m, image_bands_keys)) for m in bands)

    if product_opts is None:
        product_opts = {}

    ds_type = mk_sample_product(product_name,
                                measurements=measurements,
                                **product_opts)

    if timestamp is None:
        timestamp = '2018-06-29'

    return Dataset(ds_type, {
        'id': id,
        'format': {'name': format},
        'image': {'bands': image_bands},
        'time': timestamp,
        **geobox_to_gridspatial(geobox),
    }, uris=[uri])


def make_graph_abcde(node):
    """
      A -> B
      |    |
      |    v
      +--> C -> D
      |
      +--> E
    """
    d = node('D')
    e = node('E')
    c = node('C', cd=d)
    b = node('B', bc=c)
    a = node('A', ab=b, ac=c, ae=e)
    return a, b, c, d, e


def dataset_maker(idx, t=None):
    """ Return function that generates "dataset documents"

    (name, sources={}, **kwargs) -> dict
    """
    ns = uuid.UUID('c0fefefe-2470-3b03-803f-e7599f39ceff')
    postfix = '' if idx is None else '{:04d}'.format(idx)

    if t is None:
        t = datetime.fromordinal(736637 + (0 if idx is None else idx))

    t = t.isoformat()

    def make(name, sources=_DEFAULT, **kwargs):
        if sources is _DEFAULT:
            sources = {}

        return dict(id=str(uuid.uuid5(ns, name + postfix)),
                    label=name+postfix,
                    creation_dt=t,
                    n=idx,
                    lineage=dict(source_datasets=sources),
                    **kwargs)

    return make


def gen_dataset_test_dag(idx, t=None, force_tree=False):
    """Build document suitable for consumption by dataset add

    when force_tree is True pump the object graph through json
    serialise->deserialise, this converts DAG to a tree (no object sharing,
    copies instead).
    """
    def node_maker(n, t):
        mk = dataset_maker(n, t)

        def node(name, **kwargs):
            return mk(name,
                      product_type=name,
                      sources=kwargs)

        return node

    def deref(a):
        return json.loads(json.dumps(a))

    root, *_ = make_graph_abcde(node_maker(idx, t))
    return deref(root) if force_tree else root


def load_dataset_definition(path):
    if not isinstance(path, pathlib.Path):
        path = pathlib.Path(path)

    fname = get_metadata_path(path)
    for _, doc in read_documents(fname):
        return SimpleDocNav(doc)


def mk_test_image(w, h,
                  dtype='int16',
                  nodata=-999,
                  nodata_width=4):
    """
    Create 2d ndarray where each pixel value is formed by packing x coordinate in
    to the upper half of the pixel value and y coordinate is in the lower part.

    So for uint16: im[y, x] == (x<<8) | y IF abs(x-y) >= nodata_width
                   im[y, x] == nodata     IF abs(x-y) < nodata_width

    really it's actually: im[y, x] == ((x & 0xFF ) <<8) | (y & 0xFF)

    If dtype is of floating point type:
       im[y, x] = (x + ((y%1024)/1024))

    Pixels along the diagonal are set to nodata values (to disable set nodata_width=0)
    """

    dtype = np.dtype(dtype)

    xx, yy = np.meshgrid(np.arange(w),
                         np.arange(h))
    if dtype.kind == 'f':
        aa = xx.astype(dtype) + (yy.astype(dtype) % 1024.0) / 1024.0
    else:
        nshift = dtype.itemsize*8//2
        mask = (1 << nshift) - 1
        aa = ((xx & mask) << nshift) | (yy & mask)
        aa = aa.astype(dtype)

    if nodata is not None:
        aa[abs(xx-yy) < nodata_width] = nodata
    return aa


def split_test_image(aa):
    """
    Separate image created by mk_test_image into x,y components
    """
    if aa.dtype.kind == 'f':
        y = np.round((aa % 1)*1024)
        x = np.floor(aa)
    else:
        nshift = (aa.dtype.itemsize*8)//2
        mask = (1 << nshift) - 1
        y = aa & mask
        x = aa >> nshift
    return x, y
