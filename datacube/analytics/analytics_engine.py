# ------------------------------------------------------------------------------
# Name:       analytics_engine.py
# Purpose:    Analytical Engine
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

from __future__ import absolute_import
from __future__ import print_function

# pylint: disable=redefined-builtin

import sys
import copy
import logging
from pprint import pprint

import numpy as np
import numexpr as ne

from datacube.api import API

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


class OperationType(object):
    Get_Data, Expression, Cloud_Mask, Bandmath, Reduction = range(5)


class AnalyticsEngine(object):

    SUPPORTED_OUTPUT_TYPES = ['netcdf-cf', 'geotiff']

    OPERATORS_SENSOR_SPECIFIC_BANDMATH = \
        {
            'ndvi':
            {
                'sensors':
                {
                    'LANDSAT_5': {'input': ['nir', 'red'], 'function': 'ndvi'},
                    'LANDSAT_7': {'input': ['nir', 'red'], 'function': 'ndvi'},
                },
                'functions':
                {
                    'ndvi': '((array1 - array2) / (array1 + array2))'
                }
            }
        }
    OPERATORS_REDUCTION = \
        {
            'median': 'median(array1)'
        }

    def __init__(self, api=None, index=None):
        LOG.info('Initialise Analytics Module.')
        self.plan = []
        self.plan_dict = {}

        self.api = api or API(index=index)
        self.api_descriptors = {}
        self.api_products = []

    def task(self, name):
        """Retrieve a task"""

        return self.plan[self.plan_dict[name]]

    def add_to_plan(self, name, task):
        """Add the task to the plan"""

        self.plan.append({name: task})
        size = len(self.plan)
        self.plan_dict[name] = size-1

        return self.plan[size-1]

    def create_array(self, storage_type, variables, dimensions, name):
        """Creates an array descriptor with metadata about what the data will look like"""

        try:
            basestring
        except NameError:
            basestring = str

        # construct query descriptor
        query_parameters = {}
        if isinstance(storage_type, basestring):
            query_parameters['storage_type'] = storage_type
        else:
            query_parameters['platform'] = storage_type[0]
            query_parameters['product'] = storage_type[1]

        query_parameters['dimensions'] = dimensions
        query_parameters['variables'] = ()

        for array in variables:
            query_parameters['variables'] += (array,)

        array_descriptors = self.api.get_descriptor(query_parameters)

        storage_type_key = next(iter(array_descriptors.keys()))

        # stopgap until storage_units are filtered based on descriptors
        array_descriptors[storage_type_key]['storage_units'] = {}

        array_results = []

        for variable in variables:
            if variable not in array_descriptors[storage_type_key]['variables']:
                raise AssertionError(variable, "not present in", storage_type_key, "descriptor")

            LOG.debug('variable = %s', variable)

            array_result = {}
            # storage_type_key
            if isinstance(storage_type, basestring):
                array_result['storage_type'] = storage_type
            else:
                array_result['platform'] = storage_type[0]
                array_result['product'] = storage_type[1]
            array_result['variable'] = variable
            array_result['dimensions_order'] = array_descriptors[storage_type_key]['dimensions']
            array_result['dimensions'] = dimensions
            array_result['result_max'] = array_descriptors[storage_type_key]['result_max']
            array_result['result_min'] = array_descriptors[storage_type_key]['result_min']
            array_result['shape'] = array_descriptors[storage_type_key]['result_shape']
            array_result['data_type'] = array_descriptors[storage_type_key]['variables'][variable]['datatype_name']
            array_result['no_data_value'] = array_descriptors[storage_type_key]['variables'][variable]['nodata_value']

            array_results.append({variable: array_result})

        task = {}
        task['array_input'] = array_results
        task['array_output'] = copy.deepcopy(array_result)
        task['array_output']['variable'] = name
        task['function'] = 'get_data'
        task['orig_function'] = 'get_data'
        task['expression'] = 'none'
        task['operation_type'] = OperationType.Get_Data

        return self.add_to_plan(name, task)

    def apply_cloud_mask(self, arrays, mask, name):
        """
        Create a task descriptor of performing cloud masking
        Will be deprecated - currently kept for backwards compatibility
        """

        size = len(arrays)
        if size == 0:
            raise AssertionError("Input array is empty")

        task = {}
        task['array_input'] = []
        for array in arrays.keys():
            task['array_input'].append(array)

        task['array_mask'] = next(iter(mask.keys()))
        task['orig_function'] = 'apply_cloud_mask'
        task['function'] = 'apply_cloud_mask'
        task['expression'] = 'none'
        task['array_output'] = copy.deepcopy(next(iter(arrays.values()))['array_output'])
        task['array_output']['variable'] = name
        task['operation_type'] = OperationType.Cloud_Mask

        return self.add_to_plan(name, task)

    def apply_expression(self, arrays, function, name):
        """Create a task descriptor of performing an expression"""

        # Arrays is a list
        # output Array is same shape as input array
        size = len(arrays)
        if size == 0:
            raise AssertionError("Input array is empty")

        variables = []

        if not isinstance(arrays, list):
            arrays = [arrays]
        for array in arrays:
            variables.append(next(iter(array.keys())))

        orig_function = function
        LOG.debug('function before = %s', function)
        count = 1
        for variable in variables:
            function = function.replace('array'+str(count), variable)
            count += 1
        LOG.debug('function after = %s', function)

        task = {}

        task['array_input'] = []

        for array in arrays:
            task['array_input'].append(next(iter(array.keys())))

        task['orig_function'] = orig_function
        task['function'] = function
        task['expression'] = function
        task['array_output'] = copy.deepcopy(next(iter(arrays[0].values()))['array_output'])
        task['array_output']['variable'] = name
        task['operation_type'] = OperationType.Expression

        return self.add_to_plan(name, task)

    # generic version
    def apply_bandmath(self, arrays, function, name):
        """
        Create a task descriptor of performing band math
        Will be deprecated - currently kept for backwards compatibility
        """

        # Arrays is a list
        # output Array is same shape as input array
        size = len(arrays)
        if size == 0:
            raise AssertionError("Input array is empty")

        variables = []
        if size == 1:  # 1 input
            array_value = next(iter(arrays.values()))
            if array_value['function'] == 'get_data':

                # check shape is same for all input arrays
                shape = next(iter(array_value['array_input'][0].values()))['shape']
                for variable in array_value['array_input']:
                    if shape != next(iter(variable.values()))['shape']:
                        raise AssertionError("Shape is different")
                    variables.append(next(iter(variable.keys())))
            else:
                variables.append(next(iter(arrays.keys())))

            orig_function = function
            LOG.debug('function before = %s', function)
            count = 1
            for variable in variables:
                function = function.replace('array'+str(count), variable)
                count += 1
            LOG.debug('function after = %s', function)

            task = {}

            task['array_input'] = []
            task['array_input'].append(next(iter(arrays.keys())))
            task['orig_function'] = orig_function
            task['function'] = function
            task['expression'] = 'none'
            task['array_output'] = copy.deepcopy(array_value['array_output'])
            task['array_output']['variable'] = name
            task['operation_type'] = OperationType.Bandmath

            return self.add_to_plan(name, task)

        else:  # multi-dependencies
            for array in arrays:
                variables.append(next(iter(array.keys())))

            orig_function = function
            LOG.debug('function before = %s', function)
            count = 1
            for variable in variables:
                function = function.replace('array'+str(count), variable)
                count += 1
            LOG.debug('function after = %s', function)

            task = {}

            task['array_input'] = []

            for array in arrays:
                task['array_input'].append(next(iter(array.keys())))

            task['orig_function'] = orig_function
            task['function'] = function
            task['expression'] = 'none'
            task['array_output'] = copy.deepcopy(next(iter(arrays[0].values()))['array_output'])
            task['array_output']['variable'] = name
            task['operation_type'] = OperationType.Bandmath

            return self.add_to_plan(name, task)

    # sensor specific version
    def apply_sensor_specific_bandmath(self, storage_types, product, function, dimensions, name_data, name_result):
        """
        Create a task descriptor of performing sensor specific bandmath
        """

        variables = self.get_predefined_inputs(storage_types, function)
        function_text = self.get_predefined_function(storage_types, function)

        arrays = self.create_array((storage_types, product), variables, dimensions, name_data)

        return self.apply_bandmath(arrays, function_text, name_result)

    def apply_reduction(self, array1, dimensions, function, name):
        """
        Create a task descriptor of performing dimension reduction
        Will be deprecated - currently kept for backwards compatibility
        """

        function = self.OPERATORS_REDUCTION.get(function)
        return self.apply_generic_reduction(array1, dimensions, function, name)

    def apply_generic_reduction(self, arrays, dimensions, function, name):
        """
        Create a task descriptor of performing generic dimension reduction
        Will be deprecated - currently kept for backwards compatibility
        """

        size = len(arrays)
        if size != 1:
            raise AssertionError("Input array should be 1")

        if next(iter(arrays.values()))['function'] == 'get_data':
            array_value = next(iter(arrays.values()))
            variable = next(iter(array_value['array_input'][0].values()))['variable']
            orig_function = function
            function = function.replace('array1', variable)

            task = {}

            task['array_input'] = []
            task['array_input'].append(next(iter(arrays.keys())))
            task['orig_function'] = orig_function
            task['function'] = function
            task['expression'] = 'none'
            task['dimension'] = copy.deepcopy(dimensions)

            task['array_output'] = copy.deepcopy(array_value['array_output'])
            task['array_output']['variable'] = name
            task['array_output']['dimensions_order'] = \
                self.diff_list(next(iter(array_value['array_input'][0].values()))['dimensions_order'], dimensions)

            for item in task['dimension']:
                if item in task['array_output']['dimensions']:
                    del task['array_output']['dimensions'][item]
            result = ()
            for value in task['array_output']['dimensions_order']:
                input_task = self.task(task['array_input'][0])
                index = next(iter(input_task.values()))['array_output']['dimensions_order'].index(value)
                value = next(iter(input_task.values()))['array_output']['shape'][index]
                result += (value,)
            task['array_output']['shape'] = result
            task['operation_type'] = OperationType.Reduction

            return self.add_to_plan(name, task)
        else:
            variable = next(iter(arrays.keys()))
            orig_function = function
            function = function.replace('array1', variable)

            task = {}

            task['array_input'] = []
            task['array_input'].append(variable)
            task['orig_function'] = orig_function
            task['function'] = function
            task['expression'] = 'none'
            task['dimension'] = copy.deepcopy(dimensions)

            task['array_output'] = copy.deepcopy(next(iter(arrays.values()))['array_output'])
            task['array_output']['variable'] = name
            task['array_output']['dimensions_order'] = \
                self.diff_list(next(iter(arrays.values()))['array_output']['dimensions_order'], dimensions)

            result = ()
            for value in task['array_output']['dimensions_order']:
                input_task = self.task(task['array_input'][0])
                index = next(iter(input_task.values()))['array_output']['dimensions_order'].index(value)
                value = next(iter(input_task.values()))['array_output']['shape'][index]
                result += (value,)
            task['array_output']['shape'] = result
            task['operation_type'] = OperationType.Reduction

            return self.add_to_plan(name, task)

    def diff_list(self, list1, list2):
        """find items in list1 that are not in list2"""
        list2 = set(list2)
        return [result for result in list1 if result not in list2]

    def get_predefined_inputs(self, storage_type, function):
        """Helper function to retrieve predefined inputs from look up table"""
        return self.OPERATORS_SENSOR_SPECIFIC_BANDMATH.get(function).get('sensors').get(storage_type).get('input')

    def get_predefined_function(self, storage_type, function):
        """Helper function to retrieve predefined functions from look up table"""
        function_id = self.OPERATORS_SENSOR_SPECIFIC_BANDMATH.get(function) \
            .get('sensors').get(storage_type).get('function')
        return self.OPERATORS_SENSOR_SPECIFIC_BANDMATH.get(function).get('functions').get(function_id)

    def list_searchables(self):
        """List searchable parameters for use in get_descriptor and get_data"""
        items = {}
        if len(self.api_descriptors) == 0:
            self.api_descriptors = self.api.get_descriptor()
        for storage_type in self.api_descriptors.keys():
            item = {}
            item['dimensions'] = \
                zip([str(x) for x in self.api_descriptors[storage_type]['dimensions']],
                    [str(x) for x in self.api_descriptors[storage_type]['result_min']],
                    [str(x) for x in self.api_descriptors[storage_type]['result_max']])
            item['storage_type'] = str(storage_type)
            items[str(storage_type)] = item

        if len(self.api_products) == 0:
            self.api_products = self.api.list_products()
        for product in self.api_products:
            storage_type = str(product['name'])
            if storage_type not in self.api_descriptors.keys():
                continue
            items[storage_type]['platform'] = str(product['platform'])
            items[storage_type]['product_type'] = str(product['product_type'])
            items[storage_type]['instrument'] = str(product['instrument'])
        for variable in self.api.list_variables():
            storage_type = str(variable['product'])
            if storage_type not in self.api_descriptors.keys():
                continue
            if 'bands' not in items[storage_type]:
                items[storage_type]['bands'] = []
            items[storage_type]['bands'].append(variable['measurement'])
            items[storage_type]['bands'].sort()
        return items
