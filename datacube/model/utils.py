from __future__ import absolute_import, division, print_function

import datetime
import os
import platform
import sys
import uuid

import numpy
import xarray
from pandas import to_datetime

import datacube
from datacube.utils import geometry
from datacube.model import CRS, Dataset
import yaml
try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper


def machine_info():
    info = {
        'software_versions': {
            'python': {'version': sys.version},
            'datacube': {'version': datacube.__version__, 'repo_url': 'https://github.com/data-cube/agdc-v2.git'},
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

    return {'lineage': {'machine': info}}


def geobox_info(extent, valid_data=None):
    image_bounds = extent.boundingbox
    data_bounds = valid_data.boundingbox if valid_data else image_bounds
    ul = geometry.point(data_bounds.left, data_bounds.top, crs=extent.crs).to_crs(CRS('EPSG:4326'))
    ur = geometry.point(data_bounds.right, data_bounds.top, crs=extent.crs).to_crs(CRS('EPSG:4326'))
    lr = geometry.point(data_bounds.right, data_bounds.bottom, crs=extent.crs).to_crs(CRS('EPSG:4326'))
    ll = geometry.point(data_bounds.left, data_bounds.bottom, crs=extent.crs).to_crs(CRS('EPSG:4326'))
    doc = {
        'extent': {
            'coord': {
                'ul': {'lon': ul.points[0][0], 'lat': ul.points[0][1]},
                'ur': {'lon': ur.points[0][0], 'lat': ur.points[0][1]},
                'lr': {'lon': lr.points[0][0], 'lat': lr.points[0][1]},
                'll': {'lon': ll.points[0][0], 'lat': ll.points[0][1]},
            }
        },
        'grid_spatial': {
            'projection': {
                'spatial_reference': str(extent.crs),
                'geo_ref_points': {
                    'ul': {'x': image_bounds.left, 'y': image_bounds.top},
                    'ur': {'x': image_bounds.right, 'y': image_bounds.top},
                    'll': {'x': image_bounds.left, 'y': image_bounds.bottom},
                    'lr': {'x': image_bounds.right, 'y': image_bounds.bottom},
                }
            }
        }
    }
    if valid_data:
        doc['grid_spatial']['projection']['valid_data'] = valid_data.__geo_interface__
    return doc


def new_dataset_info():
    return {
        'id': str(uuid.uuid4()),
        'creation_dt': datetime.datetime.utcnow().isoformat(),
    }


def band_info(band_names):
    return {
        'image': {
            'bands': {name: {'path': '', 'layer': name} for name in band_names}
        }
    }


def time_info(time):
    time_str = to_datetime(time).isoformat()
    return {
        'extent': {
            'from_dt': time_str,
            'to_dt': time_str,
            'center_dt': time_str,

        }
    }


def source_info(source_datasets):
    return {
        'lineage': {
            'source_datasets': {str(idx): dataset.metadata_doc for idx, dataset in enumerate(source_datasets)}
        }
    }


def datasets_to_doc(output_datasets):
    """
    Create a yaml document version of every dataset

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
    Iterate over every element in an xarray, returning::

        * the numerical index eg ``(10, 1)``
        * the labeled index eg ``{'time': datetime(), 'band': 'red'}``
        * the element (same as ``da[10, 1].item()``)

    :param data_array: Array to iterate over
    :type data_array: xarray.DataArray
    :return: i-index, label-index, value of da element
    :rtype tuple, dict, da.dtype
    """
    values = data_array.values
    coords = {coord_name: v.values for coord_name, v in data_array.coords.items()}
    for i in numpy.ndindex(data_array.shape):
        entry = values[i]
        index = {coord_name: v[i] for coord_name, v in coords.items()}
        yield i, index, entry


def xr_apply(data_array, func, dtype):
    """
    Apply a function to every element of a :class:`xarray.DataArray`

    :type data_array: xarray.DataArray
    :param func: function that takes a dict of labels and an element of the array,
        and returns a value of the given dtype
    :param dtype: The dtype of the returned array
    :return: The array with output of the function for every element.
    :rtype: xarray.DataArray
    """
    data = numpy.empty(shape=data_array.shape, dtype=dtype)
    for i, index, entry in xr_iter(data_array):
        v = func(index, entry)
        data[i] = v
    return xarray.DataArray(data, coords=data_array.coords, dims=data_array.dims)


def make_dataset(product, sources, extent, center_time, valid_data=None, uri=None, app_info=None):
    """
    Create :class:`datacube.model.Dataset` for the data

    :param DatasetType product: Product the dataset is part of
    :param list[:class:`Dataset`] sources: datasets used to produce the dataset
    :param Geometry extent: extent of the dataset
    :param Geometry valid_data: extent of the valid data
    :param center_time: time of the central point of the dataset
    :param str uri: The uri of the dataset
    :param dict app_info: Additional metadata to be stored about the generation of the product
    :rtype: class:`Dataset`
    """
    document = {}
    merge(document, product.metadata_doc)
    merge(document, new_dataset_info())
    merge(document, machine_info())
    merge(document, band_info(product.measurements.keys()))
    merge(document, source_info(sources))
    merge(document, geobox_info(extent, valid_data))
    merge(document, time_info(center_time))
    merge(document, app_info or {})

    return Dataset(product,
                   document,
                   local_uri=uri,
                   sources={str(idx): dataset for idx, dataset in enumerate(sources)})


def merge(a, b, path=None):
    """
    Merge dictionary `b` into dictionary `a`

    See: http://stackoverflow.com/a/7205107/5262498

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
