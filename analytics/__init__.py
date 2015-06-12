#!/usr/bin/env python

import sys
import numpy as np
import numexpr as ne
import copy
from pprint import pprint

import logging

from gdf import GDF

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Logging level for this module

'''
Example 1 - create array

from pprint import pprint
from datetime import datetime
from gdf import GDF, dt2secs
from analytics import Analytics
a = Analytics()
g = GDF()

dimensions = { 'x': { 'range': (140, 142), 'crs': 'EPSG:4326' }, 'y': { 'range': (-36, -34), 'crs': 'EPSG:4326' }, 't': { 'range': (1288569600, 1296518400), 'crs': 'SSE', 'grouping_function': g.solar_days_since_epoch } }
array = a.createArray('LS5TM', ['B40'], dimensions)
pprint(array)

Example 2 - sensor specific ndvi

from pprint import pprint
from datetime import datetime
from gdf import GDF, dt2secs
from analytics import Analytics
a = Analytics()
g = GDF()

dimensions = { 'x': { 'range': (140, 142), 'crs': 'EPSG:4326' }, 'y': { 'range': (-36, -34), 'crs': 'EPSG:4326' }, 't': { 'range': (1288569600, 1296518400), 'crs': 'SSE', 'grouping_function': g.solar_days_since_epoch } }
storage_types = ['LS5TM']

ndvi = a.applySensorSpecificBandMathFunction(storage_types, 'ndvi', dimensions, 'ndvi-result', 'netcdf-cf')
pprint(ndvi)

Example 3 - generic ndvi

from pprint import pprint
from datetime import datetime
from gdf import GDF, dt2secs
from analytics import Analytics
a = Analytics()
g = GDF()

dimensions = { 'x': { 'range': (140, 142), 'crs': 'EPSG:4326' }, 'y': { 'range': (-36, -34), 'crs': 'EPSG:4326' }, 't': { 'range': (1288569600, 1296518400), 'crs': 'SSE', 'grouping_function': g.solar_days_since_epoch } }
storage_types = ['LS5TM']
variables = ['B40', 'B30']
arrays = a.createArray(storage_types[0], variables, dimensions)

ndvi_array = a.applyBandMathFunction([arrays[variables[0]], arrays[variables[1]]], '((array1 - array2) / (array1 + array2))', 'ndvi-result', 'netcdf-cf')
pprint(ndvi_array)

Example 4 - median

from pprint import pprint
from datetime import datetime
from gdf import GDF, dt2secs
from analytics import Analytics
a = Analytics()
g = GDF()

dimensions = { 'x': { 'range': (140, 142), 'crs': 'EPSG:4326' }, 'y': { 'range': (-36, -34), 'crs': 'EPSG:4326' }, 't': { 'range': (1288569600, 1296518400), 'crs': 'SSE', 'grouping_function': g.solar_days_since_epoch } }
arrays = a.createArray('LS5TM', ['B40'], dimensions)

median_t_array = a.applyReductionFunction(arrays['B40'], ['T'], 'median', 'medianResult', 'netcdf-cf')
pprint(median_t_array)

median_xy_array = a.applyReductionFunction(arrays['B40'], ['X', 'Y'], 'median', 'medianResult', 'netcdf-cf')
pprint(median_xy_array)

'''

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

	def createArray(self, storage_type, variables, dimensions):
		# stub to call GDF to get Array Descriptors and construct Arrayself.
		query_parameters = {'storage_types': [storage_type]	, 'dimensions': dimensions}
		arrayDescriptors = self.gdf.get_descriptor(query_parameters)

		if storage_type not in arrayDescriptors.keys():
			raise AssertionError(storage_type, "not present in descriptor")

		logger.debug('storage_type = %s', storage_type)

		arrayResults = {}

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

			arrayResults[variable] = arrayResult
			
		return arrayResults

	# generic version
	def applyBandMathFunction(self, arrays, function, name, output_format):
		# Arrays is a list
		# output Array is same shape as input array
		
		logger.debug('function before = %s', function)
		count = 1
		for array in arrays:
			function = function.replace('array'+str(count), arrays[count-1]['variable'])
			count += 1
		logger.debug('function after = %s', function)

		functionResult = {}

		functionResult['array_input'] = []
		count = 1
		for array in arrays:
			functionResult['array_input'].append({ arrays[count-1]['variable'] : copy.deepcopy(arrays[count-1]) })
			count += 1
		functionResult['function'] = function
		functionResult['array_output'] = { name : copy.deepcopy(arrays[0]) }
		functionResult['array_output'][name]['variable'] = name

		functionResult['output_format'] = output_format

		return functionResult

	# sensor specific version
	def applySensorSpecificBandMathFunction(self, storage_types, function, dimensions, name, output_format):
		variables = self.getPredefinedInputs(storage_types[0], function)
		arrays = self.createArray(storage_types[0], variables, dimensions)

		array_list = []
		for variable in variables:
			array_list.append(arrays[variable]) 

		function_text = self.getPredefinedFunction(storage_types, function)
		band_math_function = self.applyBandMathFunction(array_list, function_text, name, output_format)

		return band_math_function

	def applyReductionFunction(self, array1, dimensions, function, name, output_format):

		function = self.OPERATORS_REDUCTION.get(function)
		logger.debug('function before = %s', function)
		function = function.replace('array1', array1['variable'])
		logger.debug('function after = %s', function)
		functionResult = {}
		functionResult['array_input'] = []
		functionResult['array_input'].append({ array1['variable'] : copy.deepcopy(array1) })
		functionResult['Function'] = function
		functionResult['dimension'] = copy.deepcopy(dimensions)
		functionResult['array_output'] = copy.deepcopy(array1)
		functionResult['array_output']['variable'] = name
		functionResult['array_output']['dimensions_order'] = self.diffList(array1['dimensions_order'], dimensions)

		result = ()
		for value in functionResult['array_output']['dimensions_order']:
			index = functionResult['array_input'][0][array1['variable']]['dimensions_order'].index(value)
			value = functionResult['array_input'][0][array1['variable']]['shape'][index]
			result += (value,)
		functionResult['array_output']['shape'] = result
		
		functionResult['output_format'] = output_format

		return functionResult

	def diffList(self, list1, list2):
		list2 = set(list2)
		return [result for result in list1 if result not in list2]

	def getPredefinedInputs(self, storage_type, function):
		return self.OPERATORS_SENSOR_SPECIFIC_BANDMATH.get(function).get('sensors').get(storage_type).get('input')

	def getPredefinedFunction(self, storage_type, function):
		function_id = self.OPERATORS_SENSOR_SPECIFIC_BANDMATH.get(function).get('sensors').get(storage_type[0]).get('function')
		return self.OPERATORS_SENSOR_SPECIFIC_BANDMATH.get(function).get('functions').get(function_id)
