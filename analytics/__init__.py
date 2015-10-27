#!/usr/bin/env python

import sys
import numpy as np
import numexpr as ne
import copy
from pprint import pprint

import logging

from gdf import GDF

logger = logging.getLogger(__name__)


class Analytics(object):

	SUPPORTED_OUTPUT_TYPES = ['netcdf-cf', 'geotiff']

	OPERATORS_SENSOR_SPECIFIC_BANDMATH = \
	{
		'ndvi': \
		{
			'sensors': \
			{
				'LS5TM' : { 'input': ['B40', 'B30'], 'function': 'ndvi'},
				'LS7ETM' : { 'input': ['B40', 'B30'], 'function': 'ndvi'},
				'LS8OLI' : { 'input': ['B5', 'B4'], 'function': 'ndvi'},
				'LS8OLITIRS' : { 'input': ['B5', 'B4'], 'function': 'ndvi'},
			},
			'functions': \
			{
				'ndvi' : '((array1 - array2) / (array1 + array2))'
			}
		}
	}

	OPERATORS_REDUCTION = \
	{
		'median': 'median(array1)'
	}

	def __init__(self):
		logger.debug('Initialise Analytics Module.')
		self.gdf = GDF()
		self.gdf.debug = False
		self.plan = []
		self.planDict = {}

	def task(self, name):

		return self.plan[self.planDict[name]]

	def add_to_plan(self, name, task):
		
		self.plan.append( { name : task })
		size = len(self.plan)
		self.planDict[name] = size-1

		return self.plan[size-1]

	def createArray(self, storage_type, variables, dimensions, name):
		query_parameters = {}
		query_parameters['storage_type'] = storage_type
		query_parameters['dimensions'] = dimensions
		query_parameters['variables'] = ()

		for array in variables:
			query_parameters['variables'] += (array,)

		arrayDescriptors = self.gdf.get_descriptor(query_parameters)

		if storage_type not in arrayDescriptors.keys():
			raise AssertionError(storage_type, "not present in descriptor")

		logger.debug('storage_type = %s', storage_type)

		arrayResults = []

		for variable in variables:
			if variable not in arrayDescriptors[storage_type]['variables']:
				raise AssertionError(variable, "not present in", storage_type, "descriptor")

			logger.debug('variable = %s', variable)

			arrayResult = {}
			arrayResult['storage_type'] = storage_type
			arrayResult['variable'] = variable
			arrayResult['dimensions_order'] = arrayDescriptors[storage_type]['dimensions']
			arrayResult['dimensions'] = dimensions
			arrayResult['shape'] = arrayDescriptors[storage_type]['result_shape']
			arrayResult['data_type'] = arrayDescriptors[storage_type]['variables'][variable]['numpy_datatype_name']
			arrayResult['no_data_value'] = arrayDescriptors[storage_type]['variables'][variable]['nodata_value']

			arrayResults.append({variable : arrayResult})

		task = {}
		task['array_input'] = arrayResults
		task['array_output'] = copy.deepcopy(arrayResult)
		task['array_output']['variable'] = 'data'
		task['function'] = 'get_data'
		task['orig_function'] = 'get_data'

		return self.add_to_plan(name, task)

	def applyCloudMask(self, arrays, mask, name):
		size = len(arrays)
		if size == 0:
			raise AssertionError("Input array is empty")

		task = {}
		task['array_input'] = []
		for array in arrays.keys():
			task['array_input'].append(array)

		task['array_mask'] = mask.keys()[0]
		task['orig_function'] = 'apply_cloud_mask'
		task['function'] = 'apply_cloud_mask'
		task['array_output'] = copy.deepcopy(arrays.values()[0]['array_output'])
		task['array_output']['variable'] = name

		return self.add_to_plan(name, task)

	# generic version
	def applyBandMath(self, arrays, function, name):
		# Arrays is a list
		# output Array is same shape as input array
		size = len(arrays)
		if size == 0:
			raise AssertionError("Input array is empty")

		variables = []
		if size == 1: # 1 input
			if arrays.values()[0]['function'] == 'get_data':

				# check shape is same for all input arrays
				shape = arrays.values()[0]['array_input'][0].values()[0]['shape']
				for variable in arrays.values()[0]['array_input']:
					if shape != variable.values()[0]['shape']:
						raise AssertionError("Shape is different")
					variables.append(variable.keys()[0])
			else:
				variables.append(arrays.keys()[0])

			orig_function = function
			logger.debug('function before = %s', function)
			count = 1
			for variable in variables:
				function = function.replace('array'+str(count), variable)
				count += 1
			logger.debug('function after = %s', function)

			task = {}

			task['array_input'] = []
			task['array_input'].append(arrays.keys()[0])
			task['orig_function'] = orig_function
			task['function'] = function
			task['array_output'] = copy.deepcopy(arrays.values()[0]['array_output'])
			task['array_output']['variable'] = name

			return self.add_to_plan(name, task)

		else: # multi-dependencies
			pprint(arrays)
			for array in arrays:
				variables.append(array.keys()[0])

			orig_function = function
			logger.debug('function before = %s', function)
			count = 1
			for variable in variables:
				function = function.replace('array'+str(count), variable)
				count += 1
			logger.debug('function after = %s', function)

			task = {}

			task['array_input'] = []

			for array in arrays:
				task['array_input'].append(array.keys()[0])

			task['orig_function'] = orig_function
			task['function'] = function
			task['array_output'] = copy.deepcopy(arrays[0].values()[0]['array_output'])
			task['array_output']['variable'] = name

			return self.add_to_plan(name, task)

	# sensor specific version
	def applySensorSpecificBandMath(self, storage_types, function, dimensions, name_data, name_result):
		variables = self.getPredefinedInputs(storage_types, function)
		arrays = self.createArray(storage_types, variables, dimensions, name_data)

		function_text = self.getPredefinedFunction(storage_types, function)
		return self.applyBandMath(arrays, function_text, name_result)

	def applyReduction(self, array1, dimensions, function, name):

		function = self.OPERATORS_REDUCTION.get(function)
		return self.applyGenericReductionFunction(array1, dimensions, function, name)

	def applyGenericReduction(self, arrays, dimensions, function, name):
		
		size = len(arrays)
		if size != 1:
			raise AssertionError("Input array should be 1")

		if arrays.values()[0]['function'] == 'get_data':

			variable = arrays.values()[0]['array_input'][0].values()[0]['variable']
			orig_function = function
			logger.debug('function before = %s', function)
			function = function.replace('array1', variable)
			logger.debug('function after = %s', function)

			task = {}

			task['array_input'] = []
			task['array_input'].append(arrays.keys()[0])
			task['orig_function'] = orig_function
			task['function'] = function
			task['dimension'] = copy.deepcopy(dimensions)

			task['array_output'] = copy.deepcopy(arrays.values()[0]['array_output'])
			task['array_output']['variable'] = name
			task['array_output']['dimensions_order'] = self.diffList(arrays.values()[0]['array_input'][0].values()[0]['dimensions_order'], dimensions)

			result = ()
			for value in task['array_output']['dimensions_order']:
				input_task = self.task(task['array_input'][0])
				index = input_task.values()[0]['array_output']['dimensions_order'].index(value)
				value = input_task.values()[0]['array_output']['shape'][index]
				result += (value,)
			task['array_output']['shape'] = result

			return self.add_to_plan(name, task)
		else:
			variable = arrays.keys()[0]
			orig_function = function
			logger.debug('function before = %s', function)
			function = function.replace('array1', variable)
			logger.debug('function after = %s', function)

			task = {}
			
			task['array_input'] = []
			task['array_input'].append(variable)
			task['orig_function'] = orig_function
			task['function'] = function
			task['dimension'] = copy.deepcopy(dimensions)

			task['array_output'] = copy.deepcopy(arrays.values()[0]['array_output'])
			task['array_output']['variable'] = name
			task['array_output']['dimensions_order'] = self.diffList(arrays.values()[0]['array_output']['dimensions_order'], dimensions)

			result = ()
			for value in task['array_output']['dimensions_order']:
				input_task = self.task(task['array_input'][0])
				pprint(input_task)
				index = input_task.values()[0]['array_output']['dimensions_order'].index(value)
				value = input_task.values()[0]['array_output']['shape'][index]
				result += (value,)
			task['array_output']['shape'] = result

			return self.add_to_plan(name, task)

	def diffList(self, list1, list2):
		list2 = set(list2)
		return [result for result in list1 if result not in list2]

	def getPredefinedInputs(self, storage_type, function):
		return self.OPERATORS_SENSOR_SPECIFIC_BANDMATH.get(function).get('sensors').get(storage_type).get('input')

	def getPredefinedFunction(self, storage_type, function):
		function_id = self.OPERATORS_SENSOR_SPECIFIC_BANDMATH.get(function).get('sensors').get(storage_type).get('function')
		return self.OPERATORS_SENSOR_SPECIFIC_BANDMATH.get(function).get('functions').get(function_id)

