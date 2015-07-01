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
from scipy import ndimage
from scipy.io import netcdf

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
		
		#pprint(plan)
		#build query
		data_request_param = {}
		data_request_param['dimensions'] = plan['array_input'].values()[0]['dimensions']
		data_request_param['storage_type'] = plan['array_input'].values()[0]['storage_type']
		data_request_param['variables'] = ()

		for array in plan['array_input'].values():
			data_request_param['variables'] += (array['variable'],)
		
		pprint(data_request_param)

		data_response = self.gdf.get_data(data_request_param)
		pprint(data_response)
		#input_arrays = response['arrays']
		'''
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
		'''
		
		functionResult = {}
		functionResult['plan'] = copy.deepcopy(plan)
		functionResult['array_result'] = ne.evaluate(plan['function'],  data_response['arrays'])
		functionResult['array_indices'] = data_response['indices']

		no_data_value = plan['array_output'].values()[0]['no_data_value']
		for array in data_response['arrays'].values():
			functionResult['array_result'][array == no_data_value] = no_data_value
		
		
		#Apply Cloud mask

		data_request_param = {}
		data_request_param['dimensions'] = plan['array_input'].values()[0]['dimensions']
		data_request_param['storage_type'] = plan['array_input'].values()[0]['storage_type']+'PQ'
		data_request_param['variables'] = ("PQ",)
		
		pprint(data_request_param)
		try:
			data_response = self.gdf.get_data(data_request_param)
			pprint(data_response)

			pqa_mask = self.get_pqa_mask(data_response['arrays'][data_request_param['variables'][0]])
			functionResult['pq'] = pqa_mask
			print "PQA"
			pprint(pqa_mask)

			functionResult['array_result'][~pqa_mask] = no_data_value
		except:
			print "no cloud information found"
			pass
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

			no_data_value = functionResult['plan']['array_output'].values()[0]['no_data_value']

			if len(plan['dimension']) == 1 :
				print "here 1"
				dim = result['plan']['array_output'].values()[0]['dimensions_order'].index(plan['dimension'][0])
				functionResult['array_result'] = func(result['array_result'], axis=dim)
				functionResult['array_result'][functionResult['array_result'] == no_data_value] = 0
				#masked_array = np.ma.masked_array(result['array_result'], result['array_result'] == no_data_value)
				#functionResult['array_result'] = func(masked_array,axis=dim).filled(no_data_value)				
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

		data_response = self.gdf.get_data(data_request_param)
		pprint(data_response)
		#input_arrays = response['arrays']
		'''
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

		no_data_value = functionResult['plan']['array_output'].values()[0]['no_data_value']
		print no_data_value
		if len(plan['dimension']) == 1 :
			print "here1"
			dim = data_response['dimensions'].index(plan['dimension'][0])
			#functionResult['array_result'] = func(data_response['arrays'].values()[0], axis=dim)

			masked_array = np.ma.masked_array(data_response['arrays'].values()[0],data_response['arrays'].values()[0] == no_data_value)
			functionResult['array_result'] = func(masked_array,axis=dim).filled(no_data_value)

			#functionResult['array_result'] = func(data_response['arrays'].values()[0], axis=dim)
		elif len(plan['dimension']) == 2 :
			print "here2"

			dim = data_response['dimensions'].index(plan['array_output'].values()[0]['dimensions_order'][0])
			size = data_response['arrays'].values()[0].shape[dim]
			out = np.empty([size])
			for i in range(size):
#				out[i] = func(data_response['arrays'].values()[0][i])
				masked_array = np.ma.masked_array(data_response['arrays'].values()[0][i], data_response['arrays'].values()[0][i] == no_data_value)
				out[i] = func(masked_array)
				if np.isnan(out[i]):
					out[i] = no_data_value

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

		no_data_value = functionResult['plan']['array_output'].values()[0]['no_data_value']

		num_t = functionResult['array_result'].shape[0]
		rows = functionResult['array_result'].shape[1]
		cols = functionResult['array_result'].shape[2]

		driver = gdal.GetDriverByName('GTiff')
		dataset = driver.Create(filename, rows, cols, num_t, gdal.GDT_Int16)
		
		# set projection

		proj = osr.SpatialReference()
		#srs = functionResult['plan']['array_output'].values()[0]['dimensions']['X']['crs']
		srs = 'EPSG:4326'
		proj.SetWellKnownGeogCS(srs)  
		dataset.SetProjection(proj.ExportToWkt())
		
		# set geo transform
		xmin = functionResult['plan']['array_output'].values()[0]['dimensions']['X']['range'][0] 
		ymax = functionResult['plan']['array_output'].values()[0]['dimensions']['Y']['range'][1]
		pixel_size = 0.00025
		geotransform = (xmin, pixel_size, 0, ymax,0, -pixel_size)  
		dataset.SetGeoTransform(geotransform)

		for i in range(num_t):
			band = dataset.GetRasterBand(i+1)
			band.WriteArray(functionResult['array_result'][i])
			band.SetNoDataValue(no_data_value)
			band.FlushCache()
	
	def writeNDVI2NetCDF(self, functionResult, filename):

		no_data_value = functionResult['plan']['array_output'].values()[0]['no_data_value']

		num_t = functionResult['array_result'].shape[0]
		rows = functionResult['array_result'].shape[1]
		cols = functionResult['array_result'].shape[2]

		pixel_size = 0.00025
		grid_size = rows * pixel_size

		pprint(functionResult)

		f = netcdf.netcdf_file(filename, 'w')
		f.createDimension('time', num_t)
		f.createDimension('longitude', rows)
		f.createDimension('latitude', cols)

		time = f.createVariable('time', 'f8', ('time',))
		time[:] = functionResult['array_indices']['T']
		time.long_name = 'time'
		time.calendar = 'gregorian'
		time.standard_name = 'time'
		time.axis = 'T'
		time.units = 'seconds since 1970-01-01'

		longitude = f.createVariable('longitude', 'f8', ('longitude',))
		longitude[:] = functionResult['array_indices']['X']
		longitude.units = 'degrees_east'
		longitude.long_name = 'longitude'
		longitude.standard_name = 'longitude'
		longitude.axis = 'X'

		latitude = f.createVariable('latitude', 'f8', ('latitude',))
		latitude[:] = functionResult['array_indices']['Y']
		latitude.units = 'degrees_north'
		latitude.long_name = 'latitude'
		latitude.standard_name = 'latitude'
		latitude.axis = 'Y'

		result = f.createVariable('result', 'f8', ('time','longitude', 'latitude'))
 		#short B10(time, longitude, latitude) ;
		result[:] = functionResult['array_result']
		result._FillValue = no_data_value
		result.name = 'result'
		result.coordinates = 'lat lon'
		result.grid_mapping = 'crs'


		f.history = 'AnalyticsEngine test output file.'
		f.license = 'Result file'
		f.spatial_coverage = `grid_size` + ' degrees grid'
		f.featureType = 'grid'
		f.geospatial_lat_min = min(functionResult['array_indices']['Y'])
		f.geospatial_lat_max = max(functionResult['array_indices']['Y'])
		f.geospatial_lat_units = 'degrees_north'
		f.geospatial_lat_resolution = -pixel_size
		f.geospatial_lon_min = min(functionResult['array_indices']['X'])
		f.geospatial_lon_max = max(functionResult['array_indices']['X'])
		f.geospatial_lon_units = 'degrees_east'
		f.geospatial_lon_resolution = pixel_size

		f.close()

	def writeToCSV(self, functionResult, filename):
		with open(filename, 'w') as fp:
			writer = csv.writer(fp, delimiter=',')
			for i in range(functionResult['array_result'].shape[0]):
				data = functionResult['array_result'][i].tolist()
				if len(functionResult['array_result'].shape) == 1:
					writer.writerow([data])
				else:
					writer.writerow(data)

	def get_pqa_mask(self, pqa_ndarray, good_pixel_masks=[32767,16383,2457], dilation=3):

		pqa_mask = np.zeros(pqa_ndarray.shape, dtype=np.bool)
		for i in range(len(pqa_ndarray)):
			pqa_array = pqa_ndarray[i]
			# Ignore bit 6 (saturation for band 62) - always 0 for Landsat 5
			pqa_array = pqa_array | 64

			# Dilating both the cloud and cloud shadow masks 
			s = [[1,1,1],[1,1,1],[1,1,1]]
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

			#=======================================================================
			# pqa_mask = ma.getmask(ma.masked_equal(pqa_array, int(good_pixel_masks[0])))
			# for good_pixel_mask in good_pixel_masks[1:]:
			#    pqa_mask = ma.mask_or(pqa_mask, ma.getmask(ma.masked_equal(pqa_array, int(good_pixel_mask))))
			#=======================================================================
			#pqa_mask[i] = np.zeros(pqa_array.shape, dtype=np.bool)
			for good_pixel_mask in good_pixel_masks:
				pqa_mask[i][pqa_array == good_pixel_mask] = True
		return pqa_mask