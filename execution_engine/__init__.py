#!/usr/bin/env python

import sys
import numpy as np
import numexpr as ne
import copy
from pprint import pprint
import matplotlib.pyplot as plt
import math
import gdal
import osr

import csv

import logging

from gdf import GDF

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Logging level for this module

class ExecutionEngine(object):

	SUPPORTED_REDUCTION_OPERATORS = [ 'min', 'max', 'amin', 'amax', 'nanmin', 'nanmax', 'ptp',
									  'median', 'average', 'mean', 'std', 'var', 'nanmean', 'nanstd', 'nanvar',
									  'argmax', 'argmin', 'sum', 'prod', 'all', 'any' 
									]

	def __init__(self):
		logger.debug('Initialise Execution Module.')
		self.gdf = GDF()
		self.gdf.debug = False

	def executePlan(self, plan):

		is_bandmath = True
		function = None
		for op in self.SUPPORTED_REDUCTION_OPERATORS:
			if op in plan['orig_function']:
				is_bandmath = False
				break

		if is_bandmath:
			return self.executePlanBandmath(plan)
		else:
			return self.executePlanReduction(plan)

	def executePlanBandmath(self, plan):
		
		pprint(plan)
		#build query
		data_request_param = {}
		data_request_param['dimensions'] = plan['array_input'].values()[0]['dimensions']
		data_request_param['storage_type'] = plan['array_input'].values()[0]['storage_type']
		data_request_param['variables'] = ()

		for array in plan['array_input'].values():
			data_request_param['variables'] += (array['variable'],)
		
		#pprint(data_request_param)

		#response = self.gdf.get_data(data_request_param)
		#input_arrays = response['arrays']

		b30 = np.random.rand(5,1024,1024) * 255
		b40 = np.random.rand(5,1024,1024) * 255
		pq = np.random.rand(5,1024,1024)

		for k in range(b30.shape[0]):
			for i in range(1024):
				for j in range(1024):
					b30[k][i][j] = float(i)/1024.0 * 255.0

		data_response = \
		{
		'dimensions': ['T', 'X', 'Y'],
		'arrays': {
		     'B30': b30,
		     'B40': b40,
		     'PQ': pq
		     },
		'indices': [# These will be the actual x, y & t (long, lat & time) values for each array index
		'<numpy array of x indices>', 
		'<numpy array of y indices>', 
		'<numpy array of t indices>'
		] 
		}
		#pprint(data_response)
		
		functionResult = {}
		functionResult['plan'] = copy.deepcopy(plan)
		functionResult['array_result'] = ne.evaluate(plan['function'],  data_response['arrays'])

		return functionResult
	
	def executePlanReduction(self, plan):
		if 'array_input' in plan['array_input'].values()[0] and 'array_output' in plan['array_input'].values()[0] and 'function' in plan['array_input'].values()[0] and 'orig_function' in plan['array_input'].values()[0]:
			return self.executePlanReductionNested(plan)
		else:
			return self.executePlanReductionFlat(plan)
	
	def executePlanReductionNested(self, plan):

		is_nested = False

		if 'array_input' in plan['array_input'].values()[0] and 'array_output' in plan['array_input'].values()[0] and 'function' in plan['array_input'].values()[0] and 'orig_function' in plan['array_input'].values()[0]:
			is_nested = True

		result = None
		if is_nested:
			result = self.executePlanBandmath(plan['array_input'].values()[0])
			
			function_name = plan['orig_function'].replace(")"," ").replace("("," ").split()[0]

			func = getattr(np, function_name)

			#functionResult = copy.deepcopy(plan['array_output'])
			functionResult = {}
			functionResult['plan'] = copy.deepcopy(plan)

			#name = functionResult['plan']['array_output'].keys()[0]
			name2 = result['plan']['array_output'].keys()[0]
			#name2 = result['plan'['array_output'].keys()[0]

			if len(plan['dimension']) == 1 :
				print "here 1"
				dim = result['plan']['array_output'].values()[0]['dimensions_order'].index(plan['dimension'][0])
				functionResult['array_result'] = func(result['array_result'], axis=dim)
			elif len(plan['dimension']) == 2 :
				print "here 2"
				dim = result['plan']['array_output'].values()[0]['dimensions_order'].index(plan['array_output'].values()[0]['dimensions_order'][0])
				size = result['array_result'].shape[dim]
				out = np.empty([size])
				for i in range(size):
					out[i] = func(result['array_result'][i])
				functionResult['array_result'] = out

			return functionResult
		else:
			return self.executePlanReduction(plan)
			
	def executePlanReductionFlat(self, plan):

		pprint(plan)
		#build query
		data_request_param = {}
		data_request_param['dimensions'] = plan['array_input'].values()[0]['dimensions']
		data_request_param['storage_type'] = plan['array_input'].values()[0]['storage_type']
		data_request_param['variables'] = ()

		for array in plan['array_input'].values():
			data_request_param['variables'] += (array['variable'],)

		#pprint(data_request_param)

		#response = self.gdf.get_data(data_request_param)
		#input_arrays = response['arrays']

		b30 = np.random.rand(6,1024,1024) * 255

		data_response = \
		{
		'dimensions': ['T', 'X', 'Y'],
		'arrays': { 'B30': b30 },
		'indices': [# These will be the actual x, y & t (long, lat & time) values for each array index
		'<numpy array of x indices>', 
		'<numpy array of y indices>', 
		'<numpy array of t indices>'
		] 
		}
		#pprint(data_response)
		
		'''
		function_name = None
		for op in self.SUPPORTED_REDUCTION_OPERATORS:
			if op in plan['orig_function']:
				function_name = op
				break
		'''
		function_name = plan['orig_function'].replace(")"," ").replace("("," ").split()[0]

		func = getattr(np, function_name)

		functionResult = {}
		functionResult['plan'] = copy.deepcopy(plan)
		#functionResult['array_result'] = ne.evaluate(plan['function'],  data_response['arrays'])

		if len(plan['dimension']) == 1 :
			dim = data_response['dimensions'].index(plan['dimension'][0])
			functionResult['array_result'] = func(data_response['arrays'].values()[0], axis=dim)
		elif len(plan['dimension']) == 2 :
			dim = data_response['dimensions'].index(plan['array_output'].values()[0]['dimensions_order'][0])
			size = data_response['arrays'].values()[0].shape[dim]
			out = np.empty([size])
			for i in range(size):
				out[i] = func(data_response['arrays'].values()[0][i])
			functionResult['array_result'] = out

		return functionResult

	def plotNDVIImage(self, functionResult):

		img = functionResult['array_result']
		num_t = img.shape[0]
		num_rowcol = math.ceil(math.sqrt(num_t))
		fig = plt.figure(1)
		fig.clf()
		plot_count = 1
		for i in range(img.shape[0]):
			data = img[i]
			ax = fig.add_subplot(num_rowcol,num_rowcol,plot_count)
			cax = ax.imshow(data, interpolation='nearest', vmin=-1, vmax=1, aspect = 'equal')
			fig.colorbar(cax)
			plt.title("%s %d" % (functionResult.keys()[0], plot_count))
			plt.ylabel(functionResult['plan']['array_output'].values()[0]['dimensions_order'][2])
			plt.xlabel(functionResult['plan']['array_output'].values()[0]['dimensions_order'][1])
			plot_count += 1
		fig.tight_layout()
		plt.show()
	
	def plotMedianT(self, functionResult):

		img = functionResult['array_result']
		fig = plt.figure(1)
		fig.clf()
		plot_count = 1
		data = img
		ax = fig.add_subplot(1,1,plot_count)
		cax = ax.imshow(data, interpolation='nearest', aspect = 'equal')
		fig.colorbar(cax)
		plt.title("%s %d" % (functionResult.keys()[0], plot_count))
		plt.ylabel(functionResult['plan']['array_output'].values()[0]['dimensions_order'][1])
		plt.xlabel(functionResult['plan']['array_output'].values()[0]['dimensions_order'][0])
		plot_count += 1
		fig.tight_layout()
		plt.show()


	def plotMedianXY(self, functionResult):

		img = functionResult['array_result']

		ticks = range(len(img))
		plt.plot(ticks, img)
		plt.xlabel('Value')
		plt.xlabel(functionResult['plan']['array_output'].values()[0]['dimensions_order'][0])
		plt.title(functionResult.keys()[0])
		plt.xticks(ticks)
		#plt.ylim([125,130])
		plt.show()

	def writeNDVI2GeoTiff(self, functionResult, filename):

		num_t = functionResult['array_result'].shape[0]
		rows = functionResult['array_result'].shape[1]
		cols = functionResult['array_result'].shape[2]

		driver = gdal.GetDriverByName('GTiff')
		dataset = driver.Create(filename, rows, cols, num_t, gdal.GDT_Int16)
		
		# set projection

		proj = osr.SpatialReference()
		srs = functionResult['plan']['array_output'].values()[0]['dimensions']['x']['crs']
		proj.SetWellKnownGeogCS(srs)  
		dataset.SetProjection(proj.ExportToWkt())
		
		# set geo transform
		xmin = functionResult['plan']['array_output'].values()[0]['dimensions']['x']['range'][0] 
		ymax = functionResult['plan']['array_output'].values()[0]['dimensions']['y']['range'][1]
		pixel_size = 0.00025
		geotransform = (xmin, pixel_size, 0, ymax,0, -pixel_size)  
		dataset.SetGeoTransform(geotransform)

		for i in range(num_t):
			band = dataset.GetRasterBand(i+1)
			band.WriteArray(functionResult['array_result'][i])
			band.FlushCache()

	def writeToCSV(self, functionResult, filename):
		with open(filename, 'w') as fp:
			writer = csv.writer(fp, delimiter=',')
			for i in range(functionResult['array_result'].shape[0]):
				data = functionResult['array_result'][i].tolist()
				if len(functionResult['array_result'].shape) == 1:
					writer.writerow([data])
				else:
					writer.writerow(data)
