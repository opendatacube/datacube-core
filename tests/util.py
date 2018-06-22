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
from datetime import datetime

import pathlib

from osgeo import gdal

from datacube import compat
from datacube.model import Dataset, DatasetType, MetadataType


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
        if isinstance(v, dict):
            assert f.is_dir(), "%s is not a dir" % (id_,)
            assert_file_structure(f, v, id_)
        elif isinstance(v, compat.string_types):
            assert f.is_file(), "%s is not a file" % (id_,)
        else:
            assert False, "Only strings and dicts expected when defining a folder structure."


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
        if isinstance(contents, dict):
            os.mkdir(path)
            _write_files_to_dir(path, contents)
        else:
            with open(path, 'w') as f:
                if isinstance(contents, list):
                    f.writelines(contents)
                elif isinstance(contents, compat.string_types):
                    f.write(contents)
                else:
                    raise Exception('Unexpected file contents: %s' % type(contents))


def temp_dir():
    """
    Create and return a temporary directory that will be deleted automatically on exit.

    :rtype: pathlib.Path
    """
    return write_files({})


def temp_file(suffix=""):
    """
    Get a temporary file path that will be cleaned up on exit.

    Simpler than NamedTemporaryFile--- just a file path, no open mode or anything.
    :return:
    """
    f = tempfile.mktemp(suffix=suffix)

    def permissive_ignore(file_):
        if os.path.exists(file_):
            os.remove(file_)

    atexit.register(permissive_ignore, f)
    return f


def file_of_size(path, size_mb):
    """
    Create a blank file of the given size.
    """
    with open(path, "wb") as f:
        f.seek(size_mb * 1024 * 1024 - 1)
        f.write("\0")


def create_empty_dataset(src_filename, out_filename):
    """
    Create a new GDAL dataset based on an existing one, but with no data.

    Will contain the same projection, extents, etc, but have a very small filesize.

    These files can be used for automated testing without having to lug enormous files around.

    :param src_filename: Source Filename
    :param out_filename: Output Filename
    """
    inds = gdal.Open(src_filename)
    driver = inds.GetDriver()
    band = inds.GetRasterBand(1)

    out = driver.Create(out_filename,
                        inds.RasterXSize,
                        inds.RasterYSize,
                        inds.RasterCount,
                        band.DataType)
    out.SetGeoTransform(inds.GetGeoTransform())
    out.SetProjection(inds.GetProjection())
    out.FlushCache()


def isclose(a, b, rel_tol=1e-09, abs_tol=0.0):
    """
    Testing aproximate equality for floats
    See https://docs.python.org/3/whatsnew/3.5.html#pep-485-a-function-for-testing-approximate-equality
    """
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def mk_sample_product(name,
                      description='Sample',
                      measurements=['red', 'green', 'blue'],
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
        )
    }, dataset_search_fields={})

    common = dict(dtype='int16',
                  nodata=-999,
                  units='1',
                  aliases=[])

    def mk_measurement(m):
        if isinstance(m, str):
            return dict(name=m, **common)
        if isinstance(m, tuple):
            name, dtype, nodata = m
            m = common.copy()
            m.update(name=name, dtype=dtype, nodata=nodata)
            return m
        if isinstance(m, dict):
            m_merged = common.copy()
            m_merged.update(m)
            return m_merged

        assert False and 'Only support str|dict|(name, dtype, nodata)'

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
                      id='12345678123456781234567812345678'):
    image_bands_keys = 'path layer band'.split(' ')
    measurement_keys = 'dtype units nodata aliases name'.split(' ')

    def with_keys(d, keys):
        return dict((k, d[k]) for k in keys if k in d)

    measurements = [with_keys(m, measurement_keys) for m in bands]
    image_bands = dict((m['name'], with_keys(m, image_bands_keys)) for m in bands)

    ds_type = mk_sample_product(product_name,
                                measurements=measurements)

    return Dataset(ds_type, {
        'id': id,
        'format': {'name': format},
        'image': {'bands': image_bands}
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
    D = node('D')
    E = node('E')
    C = node('C', cd=D)
    B = node('B', bc=C)
    A = node('A', ab=B, ac=C, ae=E)
    return A, B, C, D, E


def gen_dataset_test_dag(idx, t=None, force_tree=False):
    """Build document suitable for consumption by dataset add

    when force_tree is True pump the object graph through json
    serialise->deserialise, this converts DAG to a tree (no object sharing,
    copies instead).
    """
    def node_maker(n, t):
        ns = uuid.UUID('c0fefefe-2470-3b03-803f-e7599f39ceff')
        postfix = '' if n is None else '{:04d}'.format(n)
        t = t.isoformat()

        def node(name, **kwargs):
            return dict(id=str(uuid.uuid5(ns, name + postfix)),
                        label=name+postfix,
                        creation_dt=t,
                        n=n,
                        product_type=name,
                        lineage=dict(source_datasets=kwargs))

        return node

    def deref(a):
        return json.loads(json.dumps(a))

    if t is None:
        t = datetime.fromordinal(736637 + (0 if idx is None else idx))

    root, *_ = make_graph_abcde(node_maker(idx, t))
    return deref(root) if force_tree else root
