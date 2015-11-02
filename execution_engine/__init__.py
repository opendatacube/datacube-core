#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import print_function
import copy
from pprint import pprint
import logging

import numpy as np
import numexpr as ne

from gdf import GDF
from analytics_utils import get_pqa_mask

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Logging level for this module


class ExecutionEngine(object):
    SUPPORTED_REDUCTION_OPERATORS = ['min', 'max', 'amin', 'amax', 'nanmin', 'nanmax', 'ptp',
                                     'median', 'average', 'mean', 'std', 'var', 'nanmean', 'nanstd', 'nanvar',
                                     'argmax', 'argmin', 'sum', 'prod', 'all', 'any'
                                     ]

    def __init__(self):
        logger.debug('Initialise Execution Module.')
        self.gdf = GDF()
        self.gdf.debug = False
        self.cache = {}

    def executePlan(self, plan):

        for task in plan:
            function = task.values()[0]['orig_function']
            print('function =', function)
            if function == 'get_data':  # get data
                self.executeGetData(task)
            elif function == 'apply_cloud_mask':  # apply cloud mask
                self.executeCloudMask(task)
            elif len([s for s in self.SUPPORTED_REDUCTION_OPERATORS if s in function]) > 0:  # reduction operator
                self.executeReduction(task)
            else:  # bandmath
                self.executeBandmath(task)

    def executeGetData(self, task):

        data_request_param = {}
        data_request_param['dimensions'] = task.values()[0]['array_input'][0].values()[0]['dimensions']
        data_request_param['storage_type'] = task.values()[0]['array_input'][0].values()[0]['storage_type']
        data_request_param['variables'] = ()

        for array in task.values()[0]['array_input']:
            data_request_param['variables'] += (array.values()[0]['variable'],)

        data_response = self.gdf.get_data(data_request_param)

        key = task.keys()[0]
        self.cache[key] = {}
        self.cache[key]['array_result'] = copy.deepcopy(data_response['arrays'])
        self.cache[key]['array_indices'] = copy.deepcopy(data_response['indices'])
        self.cache[key]['array_dimensions'] = copy.deepcopy(data_response['dimensions'])
        self.cache[key]['array_output'] = copy.deepcopy(task.values()[0]['array_output'])

        del data_request_param
        del data_response

        return self.cache[key]

    def executeCloudMask(self, task):

        key = task.keys()[0]
        data_key = task.values()[0]['array_input'][0]
        mask_key = task.values()[0]['array_mask']
        no_data_value = task.values()[0]['array_output']['no_data_value']
        print('key =', key)
        print('data key =', data_key)
        print('data mask_key =', mask_key)
        print('no_data_value =', no_data_value)

        array_desc = self.cache[task.values()[0]['array_input'][0]]

        data_array = self.cache[data_key]['array_result'].values()[0]
        mask_array = self.cache[mask_key]['array_result'].values()[0]

        pqa_mask = get_pqa_mask(mask_array)

        masked_array = copy.deepcopy(data_array)
        masked_array[~pqa_mask] = no_data_value
        self.cache[key] = {}

        self.cache[key]['array_result'] = {}
        self.cache[key]['array_result'][key] = masked_array
        self.cache[key]['array_indices'] = copy.deepcopy(array_desc['array_indices'])
        self.cache[key]['array_dimensions'] = copy.deepcopy(array_desc['array_dimensions'])
        self.cache[key]['array_output'] = copy.deepcopy(task.values()[0]['array_output'])

    def executeBandmath(self, task):

        key = task.keys()[0]

        # TODO: check all input arrays are the same shape and parameters

        arrays = {}
        for task_name in task.values()[0]['array_input']:
            arrays.update(self.cache[task_name]['array_result'])

        array_result = {}
        array_result['array_result'] = {}
        array_result['array_result'][key] = ne.evaluate(task.values()[0]['function'], arrays)

        no_data_value = task.values()[0]['array_output']['no_data_value']

        array_desc = self.cache[task.values()[0]['array_input'][0]]

        for array in array_desc['array_result'].values():
            array_result['array_result'][key][array == no_data_value] = no_data_value

        array_result['array_indices'] = copy.deepcopy(array_desc['array_indices'])
        array_result['array_dimensions'] = copy.deepcopy(array_desc['array_dimensions'])
        array_result['array_output'] = copy.deepcopy(task.values()[0]['array_output'])

        self.cache[key] = array_result
        return self.cache[key]

    def executeReduction(self, task):

        function_name = task.values()[0]['orig_function'].replace(")", " ").replace("(", " ").split()[0]
        func = getattr(np, function_name)

        key = task.keys()[0]
        data_key = task.values()[0]['array_input'][0]
        print('key =', key)
        print('data key =', data_key)

        data = self.cache[data_key]['array_dimensions']

        no_data_value = task.values()[0]['array_output']['no_data_value']

        array_data = self.cache[data_key]['array_result'].values()[0]

        array_desc = self.cache[task.values()[0]['array_input'][0]]

        array_result = {}
        array_result['array_result'] = {}
        array_result['array_output'] = copy.deepcopy(task.values()[0]['array_output'])

        if len(task.values()[0]['dimension']) == 1:  # 3D -> 2D reduction
            pprint(self.cache[data_key]['array_dimensions'])
            dim = self.cache[data_key]['array_dimensions'].index(task.values()[0]['dimension'][0])

            array_result['array_result'][key] = np.apply_along_axis(lambda x: func(x[x != no_data_value]), dim,
                                                                   array_data)

            array_result['array_indices'] = copy.deepcopy(array_desc['array_indices'])
            array_result['array_dimensions'] = copy.deepcopy(array_result['array_output']['dimensions_order'])

            for index in array_desc['array_indices']:
                if index not in array_result['array_dimensions'] and index in array_result['array_indices']:
                    del array_result['array_indices'][index]
        elif len(task.values()[0]['dimension']) == 2:  # 3D -> 1D reduction
            size = task.values()[0]['array_output']['shape'][0]
            print('size =', size)
            out = np.empty([size])
            dim = self.cache[data_key]['array_dimensions'].index(
                task.values()[0]['array_output']['dimensions_order'][0])
            print('dim =', dim)

            # to fix bug in gdf
            # size = self.cache[data_key]['array_result'].values()[0].shape[dim]
            # print 'size =', size
            # out = np.empty([size])

            for i in range(size):
                if dim == 0:
                    out[i] = func(array_data[i, :, :][array_data[i, :, :] != no_data_value])
                elif dim == 1:
                    out[i] = func(array_data[:, i, :][array_data[:, i, :] != no_data_value])
                elif dim == 2:
                    out[i] = func(array_data[:, :, i][array_data[:, :, i] != no_data_value])

                if np.isnan(out[i]):
                    out[i] = no_data_value

            array_result['array_result'][key] = out
            array_result['array_indices'] = copy.deepcopy(array_desc['array_indices'])
            array_result['array_dimensions'] = copy.deepcopy(array_result['array_output']['dimensions_order'])

            for index in array_desc['array_indices']:
                if index not in array_result['array_dimensions'] and index in array_result['array_indices']:
                    del array_result['array_indices'][index]

        self.cache[key] = array_result
        return self.cache[key]
