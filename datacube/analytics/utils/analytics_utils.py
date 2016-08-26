# ------------------------------------------------------------------------------
# Name:       analytics_utils.py
# Purpose:    Helper utilities
#
# Author:     Peter Wang
#
# Created:    14 July 2015
# Copyright:  2015 Commonwealth Scientific and Industrial Research Organisation
#             (CSIRO)
#             Adapted get_pqa_mask function from stacker.py by Josh Sixsmith &
#             Alex IP of Geoscience Australia
#             https://github.com/GeoscienceAustralia/agdc/blob/master/src/stacker.py
# License:    This software is open source under the Apache v2.0 License
#             as provided in the accompanying LICENSE file or available from
#             https://github.com/data-cube/agdc-v2/blob/master/LICENSE
#             By continuing, you acknowledge that you have read and you accept
#             and will abide by the terms of the License.
#
# Updates:
# 7/10/2015:  Initial Version.
#
# ------------------------------------------------------------------------------

# pylint: disable=consider-using-enumerate
# Use of optional matplotlib
# pylint: disable=import-error

from __future__ import absolute_import
from __future__ import print_function
import math
import csv
import numpy as np
from scipy import ndimage
from scipy.io import netcdf
from osgeo import gdal, osr
import xarray


def plot(array_result):
    '''
    Wrapper to Plot a 1D, 2D and 3D array
    Parameters:
        array_result: computed array as a result of execution
    '''

    dims = len(array_result['array_result'].values()[0].shape)

    if dims == 1:
        plot_1d(array_result)
    elif dims == 2:
        plot_2d(array_result)
    elif dims == 3:
        plot_3d(array_result)


def plot_1d(array_result):
    '''
    Plot a 1D array
    Parameters:
        array_result: computed array as a result of execution
    '''
    print('plot1D')
    img = array_result['array_result'].values()[0]

    no_data_value = array_result['array_output']['no_data_value']
    import matplotlib.pyplot as plt
    ticks = np.arange(0, len(img), 1.0)
    plt.plot(ticks, img)
    plt.ylabel('Value')
    plt.xlabel(array_result['array_output']['dimensions_order'][0])
    plt.title(array_result.keys()[0])
    plt.xticks(ticks)
    plt.show()


def plot_2d(array_result):
    '''
    Plot a 2D array
    Parameters:
        array_result: computed array as a result of execution
    '''
    print('plot2D')
    import matplotlib.pyplot as plt
    img = array_result['array_result'].values()[0]
    fig = plt.figure(1)
    fig.clf()
    data = img
    ax = fig.add_subplot(1, 1, 1)
    cax = ax.imshow(data, interpolation='nearest', aspect='equal')
    fig.colorbar(cax)
    plt.title("%s %d" % (array_result.keys()[0], 1))
    plt.xlabel(array_result['array_output']['dimensions_order'][0])
    plt.ylabel(array_result['array_output']['dimensions_order'][1])
    fig.tight_layout()
    plt.show()


def plot_3d(array_result):
    '''
    Plot a 3D array
    Parameters:
        array_result: computed array as a result of execution
    '''
    print('plot3D')
    import matplotlib.pyplot as plt
    img = array_result['array_result'].values()[0]
    num_t = img.shape[0]
    num_rowcol = math.ceil(math.sqrt(num_t))
    fig = plt.figure(1)
    fig.clf()
    plot_count = 1
    for i in range(img.shape[0]):
        data = img[i]
        ax = fig.add_subplot(num_rowcol, num_rowcol, plot_count)
        cax = ax.imshow(data, interpolation='nearest', aspect='equal')
# TODO: including the color bar is causing crashes in formatting on some system (left > right reported by matplotlib)
#       fig.colorbar(cax)
        plt.title("%s %d" % (array_result.keys()[0], plot_count))
        plt.xlabel(array_result['array_output']['dimensions_order'][1])
        plt.ylabel(array_result['array_output']['dimensions_order'][2])
        plot_count += 1
    fig.tight_layout()
    plt.subplots_adjust(wspace=0.5, hspace=0.5)
    plt.show()


def get_pqa_mask(pqa_ndarray):
    '''
    create pqa_mask from a ndarray

    Parameters:
        pqa_ndarray: input pqa array
        good_pixel_masks: known good pixel values
        dilation: amount of dilation to apply
    '''

    good_pixel_masks = [32767, 16383, 2457]
    dilation = 3
    pqa_mask = np.zeros(pqa_ndarray.shape, dtype=np.bool)
    for i in range(len(pqa_ndarray)):
        pqa_array = pqa_ndarray[i]
        # Ignore bit 6 (saturation for band 62) - always 0 for Landsat 5
        pqa_array = pqa_array | 64

        # Dilating both the cloud and cloud shadow masks
        s = [[1, 1, 1], [1, 1, 1], [1, 1, 1]]
        acca = (pqa_array & 1024) >> 10
        erode = ndimage.binary_erosion(acca, s, iterations=dilation, border_value=1)
        dif = erode - acca
        dif[dif < 0] = 1
        pqa_array += (dif << 10)
        del acca
        fmask = (pqa_array & 2048) >> 11
        erode = ndimage.binary_erosion(fmask, s, iterations=dilation, border_value=1)
        dif = erode - fmask
        dif[dif < 0] = 1
        pqa_array += (dif << 11)
        del fmask
        acca_shad = (pqa_array & 4096) >> 12
        erode = ndimage.binary_erosion(acca_shad, s, iterations=dilation, border_value=1)
        dif = erode - acca_shad
        dif[dif < 0] = 1
        pqa_array += (dif << 12)
        del acca_shad
        fmask_shad = (pqa_array & 8192) >> 13
        erode = ndimage.binary_erosion(fmask_shad, s, iterations=dilation, border_value=1)
        dif = erode - fmask_shad
        dif[dif < 0] = 1
        pqa_array += (dif << 13)

        for good_pixel_mask in good_pixel_masks:
            pqa_mask[i][pqa_array == good_pixel_mask] = True
    return pqa_mask
