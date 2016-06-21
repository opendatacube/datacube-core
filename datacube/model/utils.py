from __future__ import absolute_import, division, print_function

import collections
import datetime
import os
import platform
import sys
import uuid

import numpy
import xarray
from pandas import to_datetime

import datacube
from ..model import GeoPolygon, CRS, Dataset

import yaml
try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper


def machine_info():
    info = {
        'software_versions': {
            'python': sys.version,
            'datacube': datacube.__version__,
        },
        'hostname': platform.node(),
    }

    if hasattr(os, 'uname'):
        info['uname'] = ' '.join(os.uname())
    else:
        info['uname'] = ' '.join([platform.system(),
                                  platform.node(),
                                  platform.release(),
                                  platform.version(),
                                  platform.machine()])

    return {'machine': info}


def geobox_info(crs, extent):
    bb = extent.boundingbox
    gp = GeoPolygon([(bb.left, bb.top), (bb.right, bb.top), (bb.right, bb.bottom), (bb.left, bb.bottom)],
                    crs).to_crs(CRS('EPSG:4326'))
    doc = {
        'extent': {
            'coord': {
                'ul': {'lon': gp.points[0][0], 'lat': gp.points[0][1]},
                'ur': {'lon': gp.points[1][0], 'lat': gp.points[1][1]},
                'lr': {'lon': gp.points[2][0], 'lat': gp.points[2][1]},
                'll': {'lon': gp.points[3][0], 'lat': gp.points[3][1]},
            }
        },
        'grid_spatial': {
            'projection': {
                'spatial_reference': str(crs),
                'geo_ref_points': {
                    'ul': {'x': bb.left, 'y': bb.top},
                    'ur': {'x': bb.right, 'y': bb.top},
                    'll': {'x': bb.left, 'y': bb.bottom},
                    'lr': {'x': bb.right, 'y': bb.bottom},
                }
            }
        }
    }
    return doc


def new_dataset_info():
    doc = {
        'id': str(uuid.uuid4()),
        'creation_dt': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
    }
    return doc


def band_info(band_names):
    doc = {
        'image': {
            'bands': {name: {'path': '', 'layer': name} for name in band_names}
        }
    }
    return doc


def time_info(time):
    time = to_datetime(time)
    doc = {
        'extent': {
            'from_dt': '{:%Y-%m-%d %H:%M:%S}'.format(time),
            'to_dt': '{:%Y-%m-%d %H:%M:%S}'.format(time),
            'center_dt': '{:%Y-%m-%d %H:%M:%S}'.format(time),

        }
    }
    return doc


def source_info(source_datasets):
    doc = {
        'lineage': {
            'source_datasets': {str(idx): dataset.metadata_doc for idx, dataset in enumerate(source_datasets)}
        }
    }
    return doc


def datasets_to_doc(output_datasets):
    """
    Creates a yaml document version of every dataset

    :param output_datasets: An array of :class:`datacube.model.Dataset`
    :type output_datasets: :py:class:`xarray.DataArray`
    :return: An array of yaml document strings
    :rtype: :py:class:`xarray.DataArray`
    """
    def dataset_to_yaml(index, dataset):
        return yaml.dump(dataset.metadata_doc, Dumper=SafeDumper, encoding='utf-8')

    return xr_apply(output_datasets, dataset_to_yaml, dtype='O').astype('S')


def xr_iter(data_array):
    """
    Iterates over every element in an xarray, returning::

        * the numerical index eg ``(10, 1)``
        * the labeled index eg ``{'time': datetime(), 'band': 'red'}``
        * the element (same as ``da[10, 1].item()``)

    :param data_array: Array to iterate over
    :type data_array: xarray.DataArray
    :return: i-index, label-index, value of da element
    :rtype tuple, dict, da.dtype
    """
    for i in numpy.ndindex(data_array.shape):
        entry = data_array[i]
        index = {k: v.data for k, v in entry.coords.items()}
        yield i, index, entry.data


def xr_apply(data_array, func, dtype):
    """
    Applies a function to every element of an xarray
    :type data_array: xarray.DataArray
    :param func: function that takes a dict of labels and an element of the array,
        and returns a value of the given dtype
    :param dtype: The dtype of the returned array
    :return: The array with output of the function for every element
    :rtype: xarray.DataArray
    """
    data = numpy.empty(shape=data_array.shape, dtype=dtype)
    for i, index, entry in xr_iter(data_array):
        v = func(index, entry)
        data[i] = v
    return xarray.DataArray(data, coords=data_array.coords, dims=data_array.dims)


def generate_dataset(data, sources, dataset_type, uri, app_info):
    """
    Creates Datasets for the data
    :param data: The data to be used
    :type: :py:class:`xarray.Dataset`
    :param sources: an array of source datasets
    :type sources: :py:class:`xarray.DataArray`
    :param dataset_type:
    :type dataset_type: datacube.model.DatasetType
    :param uri: The uri of the file
    :type uri: str
    :param app_info: Additional metadata to be stored about the generation of the product
    :type app_info: dict
    :return: An array of Dataset objects
    :rtype: :py:class:`xarray.DataArray`
    """
    def make_dataset(index, source_datasets):
        document = {}
        merge(document, dataset_type.metadata)
        merge(document, new_dataset_info())
        merge(document, machine_info())
        merge(document, band_info(data.data_vars))
        merge(document, source_info(source_datasets))
        merge(document, geobox_info(data.crs, data.extent))
        if 'time' in index:
            merge(document, time_info(index['time']))
        merge(document, app_info)

        dataset = Dataset(dataset_type,
                          document,
                          local_uri=uri,
                          sources={str(idx): dataset for idx, dataset in enumerate(source_datasets)})
        return dataset

    output_datasets = xr_apply(sources, make_dataset, dtype='O')
    return output_datasets


def append_datasets_to_data(data, datasets):
    """
    :param data: The data to be output
    :type data: :py:class:`xarray.Dataset`
    :param datasets: The array containing the :class:`datacube.model.Dataset` objects.
        All dimensions of ``datasets`` must be the same length as those found in the ``data`` object
    :type datasets: :py:class:`xarray.DataArray`
    :return: Data with a new ``dataset`` variable
    """
    new_data = data.copy()
    new_data['dataset'] = datasets_to_doc(datasets)
    return new_data


def merge(a, b, path=None):
    """merges b into a
    http://stackoverflow.com/a/7205107/5262498
    :type a: dict
    :type b: dict
    :rtype: dict
    """
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a
