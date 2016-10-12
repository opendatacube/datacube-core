# ------------------------------------------------------------------------------
# Name:       execution_engine.py
# Purpose:    Execution Engine
#
# Author:     Peter Wang
#
# Created:    14 July 2015
# Copyright:  2015 Commonwealth Scientific and Industrial Research Organisation
#             (CSIRO)
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

# pylint: disable=deprecated-method

from __future__ import absolute_import
from __future__ import print_function
import sys
import copy
import inspect
import logging
from pprint import pprint
import numpy as np
import numexpr as ne
import gdal
import osr
import xarray as xr
from xarray import ufuncs

from datacube.api import API
from datacube.analytics.analytics_engine import OperationType
from datacube.analytics.utils.analytics_utils import get_pqa_mask
from datacube.ndexpr import NDexpr


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


class ExecutionEngine(object):

    REDUCTION_FNS = {"all": xr.DataArray.all,
                     "any": xr.DataArray.any,
                     "argmax": xr.DataArray.argmax,
                     "argmin": xr.DataArray.argmin,
                     "max": xr.DataArray.max,
                     "mean": xr.DataArray.mean,
                     "median": xr.DataArray.median,
                     "min": xr.DataArray.min,
                     "prod": xr.DataArray.prod,
                     "sum": xr.DataArray.sum,
                     "std": xr.DataArray.std,
                     "var": xr.DataArray.var}

    def __init__(self, api=None, index=None):
        LOG.info('Initialise Execution Module.')
        self.cache = {}
        self.nd = NDexpr()
        self.nd.set_ae(True)

        self.api = api or API(index=index)
        self.udfuncs = {}

    def add_function(self, name, func):
        self.udfuncs[name] = func

    def execute_plan(self, plan):

        for task in plan:
            function = next(iter(task.values()))['orig_function']
            op_type = next(iter(task.values()))['operation_type']

            if op_type == OperationType.Get_Data:
                self.execute_get_data(task)
            elif op_type == OperationType.Expression:
                self.execute_expression(task)
            elif op_type == OperationType.Cloud_Mask:
                self.execute_cloud_mask(task)
            elif op_type == OperationType.Reduction and \
                    len([s for s in self.REDUCTION_FNS.keys() if s in function]) > 0:
                self.execute_reduction(task)
            elif op_type == OperationType.Bandmath:
                self.execute_bandmath(task)

    def execute_get_data(self, task):

        value = next(iter(task.values()))

        array_input_value = next(iter(value['array_input'][0].values()))

        data_request_param = {}
        data_request_param['dimensions'] = array_input_value['dimensions']
        if 'storage_type' in array_input_value:
            data_request_param['storage_type'] = array_input_value['storage_type']
        else:
            data_request_param['product'] = array_input_value['product']
            data_request_param['platform'] = array_input_value['platform']
        data_request_param['variables'] = ()

        for array in value['array_input']:
            data_request_param['variables'] += (next(iter(array.values()))['variable'],)

        data_response = self.api.get_data(data_request_param)

        no_data_value = value['array_output']['no_data_value']

        if no_data_value is not None:
            for k, v in data_response['arrays'].items():
                data_response['arrays'][k] = data_response['arrays'][k].where(v != no_data_value)

        key = next(iter(task.keys()))
        self.cache[key] = {}
        self.cache[key]['array_result'] = copy.deepcopy(data_response['arrays'])
        self.cache[key]['array_indices'] = copy.deepcopy(data_response['indices'])
        self.cache[key]['array_dimensions'] = copy.deepcopy(data_response['dimensions'])
        self.cache[key]['array_output'] = copy.deepcopy(value['array_output'])
        self.cache[key]['crs'] = copy.deepcopy(data_response['coordinate_reference_systems'])

        del data_request_param
        del data_response

        return self.cache[key]

    def execute_cloud_mask(self, task):

        key = next(iter(task.keys()))
        value = next(iter(task.values()))

        data_key = value['array_input'][0]
        mask_key = value['array_mask']
        no_data_value = value['array_output']['no_data_value']

        array_desc = self.cache[value['array_input'][0]]

        data_array = next(iter(self.cache[data_key]['array_result'].values()))
        mask_array = next(iter(self.cache[mask_key]['array_result'].values()))

        pqa_mask = get_pqa_mask(mask_array.values)

        masked_array = xr.DataArray.where(data_array, pqa_mask)
        #masked_array = masked_array.fillna(no_data_value)

        self.cache[key] = {}

        self.cache[key]['array_result'] = {}
        self.cache[key]['array_result'][key] = masked_array
        self.cache[key]['array_indices'] = copy.deepcopy(array_desc['array_indices'])
        self.cache[key]['array_dimensions'] = copy.deepcopy(array_desc['array_dimensions'])
        self.cache[key]['array_output'] = copy.deepcopy(value['array_output'])
        self.cache[key]['crs'] = copy.deepcopy(array_desc['crs'])

    def execute_expression(self, task):

        key = next(iter(task.keys()))
        value = next(iter(task.values()))

        no_data_value = value['array_output']['no_data_value']

        arrays = {}
        for task_name in value['array_input']:
            arrays[task_name] = next(iter(self.cache[task_name]['array_result'].values()))

        for i in arrays:
            if arrays[i].dtype == type(no_data_value):
                arrays[i] = arrays[i].where(arrays[i] != no_data_value)

        array_result = {}
        array_result['array_result'] = {}
        array_result['array_result'][key] = self.nd.evaluate(value['function'],
                                                             local_dict=arrays,
                                                             user_functions=self.udfuncs)
        #array_result['array_result'][key] = array_result['array_result'][key].fillna(no_data_value)

        array_desc = self.cache[value['array_input'][0]]

        array_result['array_indices'] = copy.deepcopy(array_desc['array_indices'])
        array_result['array_dimensions'] = copy.deepcopy(array_desc['array_dimensions'])
        array_result['array_output'] = copy.deepcopy(value['array_output'])
        array_result['crs'] = copy.deepcopy(array_desc['crs'])

        self.cache[key] = array_result

        return self.cache[key]

    def execute_bandmath(self, task):

        key = next(iter(task.keys()))
        value = next(iter(task.values()))

        arrays = {}
        for task_name in value['array_input']:
            #for k, v in self.cache[task_name]['array_result'].items():
            #    arrays[k] = v.astype(float).values
            arrays.update(self.cache[task_name]['array_result'])

        array_result = {}
        array_result['array_result'] = {}
        array_result['array_result'][key] = xr.DataArray(ne.evaluate(value['function'], arrays))
        #array_result['array_result'][key] = self.nd.evaluate(value['function'],  arrays)

        array_desc = self.cache[value['array_input'][0]]

        array_result['array_indices'] = copy.deepcopy(array_desc['array_indices'])
        array_result['array_dimensions'] = copy.deepcopy(array_desc['array_dimensions'])
        array_result['array_output'] = copy.deepcopy(value['array_output'])
        array_result['crs'] = copy.deepcopy(array_desc['crs'])

        self.cache[key] = array_result
        return self.cache[key]

    def execute_reduction(self, task):

        key = next(iter(task.keys()))
        value = next(iter(task.values()))

        function_name = value['orig_function'].replace(")", " ").replace("(", " ").split()[0]
        func = self.REDUCTION_FNS[function_name]

        data_key = value['array_input'][0]

        data = self.cache[data_key]['array_dimensions']

        no_data_value = value['array_output']['no_data_value']

        array_data = next(iter(self.cache[data_key]['array_result'].values()))

        array_desc = self.cache[value['array_input'][0]]

        array_result = {}
        array_result['array_result'] = {}
        array_result['array_output'] = copy.deepcopy(value['array_output'])

        dims = tuple((self.cache[data_key]['array_dimensions'].index(p) for p in value['dimension']))

        args = {}
        if function_name == 'argmax' or function_name == 'argmin':
            if len(dims) != 1:
                args['axis'] = dims[0]
        else:
            args['axis'] = dims

        if sys.version_info >= (3, 0):
            if 'skipna' in list(inspect.signature(self.REDUCTION_FNS[function_name]).parameters.keys()) and \
               function_name != 'prod':
                args['skipna'] = True
        else:
            if 'skipna' in inspect.getargspec(self.REDUCTION_FNS[function_name])[0] and \
               function_name != 'prod':
                args['skipna'] = True

        array_result['array_result'][key] = func(xr.DataArray(array_data), **args)
        array_result['array_indices'] = copy.deepcopy(array_desc['array_indices'])
        array_result['array_dimensions'] = copy.deepcopy(array_result['array_output']['dimensions_order'])
        array_result['crs'] = copy.deepcopy(array_desc['crs'])

        self.cache[key] = array_result
        return self.cache[key]
