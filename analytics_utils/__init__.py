#!/usr/bin/env python

# Adapted get_pqa_mask function from stacker.py by Josh Sixsmith & Alex IP of Geoscience Australia
# https://github.com/GeoscienceAustralia/agdc/blob/master/src/stacker.py

import math
import matplotlib.pyplot as plt
import numpy as np
from scipy import ndimage
from scipy.io import netcdf
import csv
from osgeo import gdal, osr

'''
Utils class for:
- plotting
- exporting to GeoTiff & netcdf
- computing landsat cloud masks

'''

def plot(array_result):
	'''
	Wrapper to Plot a 1D, 2D and 3D array
	Parameters:
		array_result: computed array as a result of execution
	'''

	#TODO: make it work for multiple arrays

	dims = len(array_result['array_result'].values()[0].shape)

	if dims == 1:
		plot1D(array_result)
	elif dims == 2:
		plot2D(array_result)
	elif dims == 3:
		plot3D(array_result)

def plot1D(array_result):
	'''
	Plot a 1D array
	Parameters:
		array_result: computed array as a result of execution
	'''
	print 'plot1D'
	img = array_result['array_result'].values()[0]

	no_data_value = array_result['array_output']['no_data_value']
	#img = np.array(filter(lambda x: x > -no_data_value, img))

	ticks = range(len(img))
	plt.plot(ticks, img)
	plt.ylabel('Value')
	plt.xlabel(array_result['array_output']['dimensions_order'][0])
	plt.title(array_result.keys()[0])
	plt.xticks(ticks)
	plt.show()

def plot2D(array_result):
	'''
	Plot a 2D array
	Parameters:
		array_result: computed array as a result of execution
	'''
	print 'plot2D'

	img = array_result['array_result'].values()[0]
	fig = plt.figure(1)
	fig.clf()
	data = img
	data[data == -999] = 0
	ax = fig.add_subplot(1,1,1)
	cax = ax.imshow(data, interpolation='nearest', aspect = 'equal')
	fig.colorbar(cax)
	plt.title("%s %d" % (array_result.keys()[0], 1))
	plt.xlabel(array_result['array_output']['dimensions_order'][0])
	plt.ylabel(array_result['array_output']['dimensions_order'][1])
	fig.tight_layout()
	plt.show()

def plot3D(array_result):
	'''
	Plot a 3D array
	Parameters:
		array_result: computed array as a result of execution
	'''
	print 'plot3D'

	img = array_result['array_result'].values()[0]
	num_t = img.shape[0]
	num_rowcol = math.ceil(math.sqrt(num_t))
	fig = plt.figure(1)
	fig.clf()
	plot_count = 1
	for i in range(img.shape[0]):
		data = img[i]
		data[data == -999] = 0
		ax = fig.add_subplot(num_rowcol,num_rowcol,plot_count)
		cax = ax.imshow(data, interpolation='nearest', aspect = 'equal')
# TODO: including the color bar is causing crashes in formatting on some system (left > right reported by matplotlib)
#		fig.colorbar(cax)
		plt.title("%s %d" % (array_result.keys()[0], plot_count))
		plt.xlabel(array_result['array_output']['dimensions_order'][1])
		plt.ylabel(array_result['array_output']['dimensions_order'][2])
		plot_count += 1
	fig.tight_layout()
	plt.subplots_adjust(wspace=0.5, hspace=0.5)
	plt.show()

def writeTXY_to_GeoTiff(array_result, filename):
	'''
	Export TXY/TYX to GeoTiff

	Parameters:
		array_result: computed array as a result of execution
		filename: name of output GeoTiff file
	'''

	no_data_value = array_result['array_output']['no_data_value']

	dims = array_result['array_output']['shape']
	dim_order = array_result['array_output']['dimensions_order']

	num_t = dims[0]
	rows = int(dims[1])
	cols = int(dims[2])

	driver = gdal.GetDriverByName('GTiff')
	dataset = driver.Create(filename, rows, cols, num_t, gdal.GDT_Int16)
	
	# set projection

	proj = osr.SpatialReference()
	#srs = array_result['plan']['array_output'].values()[0]['dimensions']['X']['crs']
	srs = 'EPSG:4326'
	proj.SetWellKnownGeogCS(srs)  
	dataset.SetProjection(proj.ExportToWkt())
	
	# set geo transform
	xmin = array_result['array_output']['dimensions']['X']['range'][0]
	ymax = array_result['array_output']['dimensions']['Y']['range'][1]
	pixel_size = 0.00025
	geotransform = (xmin, pixel_size, 0, ymax,0, -pixel_size)  
	dataset.SetGeoTransform(geotransform)

	for i in range(num_t):
		band = dataset.GetRasterBand(i+1)
		band.WriteArray(array_result['array_result'].values()[0][i])
		band.SetNoDataValue(no_data_value)
		band.FlushCache()

def writeNDVI2NetCDF(array_result, filename):
	'''
	Export TXY/TYX to NetCDF

	Parameters:
		array_result: computed array as a result of execution
		filename: name of output NetCDF file
	'''

	no_data_value = array_result['array_output']['no_data_value']
	dims = array_result['array_output']['shape']
	num_t = dims[0]
	rows = int(dims[1])
	cols = int(dims[2])

	pixel_size = 0.00025
	grid_size = rows * pixel_size

	pprint(array_result)

	f = netcdf.netcdf_file(filename, 'w')
	f.createDimension('time', num_t)
	f.createDimension('longitude', rows)
	f.createDimension('latitude', cols)

	time = f.createVariable('time', 'f8', ('time',))
	time[:] = array_result['array_indices']['T']
	time.long_name = 'time'
	time.calendar = 'gregorian'
	time.standard_name = 'time'
	time.axis = 'T'
	time.units = 'seconds since 1970-01-01'

	latitude = f.createVariable('latitude', 'f8', ('latitude',))
	latitude[:] = array_result['array_indices']['Y']
	latitude.units = 'degrees_north'
	latitude.long_name = 'latitude'
	latitude.standard_name = 'latitude'
	latitude.axis = 'Y'

	longitude = f.createVariable('longitude', 'f8', ('longitude',))
	longitude[:] = array_result['array_indices']['X']
	longitude.units = 'degrees_east'
	longitude.long_name = 'longitude'
	longitude.standard_name = 'longitude'
	longitude.axis = 'X'

	result = f.createVariable('result', 'f8', ('time', 'latitude', 'longitude'))
		#short B10(time, longitude, latitude) ;
	result[:] = array_result['array_result']
	result._FillValue = no_data_value
	result.name = 'result'
	result.coordinates = 'lat lon'
	result.grid_mapping = 'crs'


	f.history = 'AnalyticsEngine test output file.'
	f.license = 'Result file'
	f.spatial_coverage = `grid_size` + ' degrees grid'
	f.featureType = 'grid'
	f.geospatial_lat_min = min(array_result['array_indices']['Y'])
	f.geospatial_lat_max = max(array_result['array_indices']['Y'])
	f.geospatial_lat_units = 'degrees_north'
	f.geospatial_lat_resolution = -pixel_size
	f.geospatial_lon_min = min(array_result['array_indices']['X'])
	f.geospatial_lon_max = max(array_result['array_indices']['X'])
	f.geospatial_lon_units = 'degrees_east'
	f.geospatial_lon_resolution = pixel_size

	f.close()

def writeToCSV(array_result, filename):
	'''
	Export 1D/2D array to CSV file

	Parameters:
		array_result: computed array as a result of execution
		filename: name of output CSV file
	'''

	with open(filename, 'w') as fp:
		writer = csv.writer(fp, delimiter=',')
		for i in range(int(array_result['array_output']['shape'][0])):
			data = array_result['array_result'].values()[0][i].tolist()
			if len(array_result['array_result'].values()[0].shape) == 1:
				writer.writerow([data])
			else:
				writer.writerow(data)

def get_pqa_mask(pqa_ndarray, good_pixel_masks=[32767,16383,2457], dilation=3):
	'''
	create pqa_mask from a ndarray

	Parameters:
		pqa_ndarray: input pqa array
		good_pixel_masks: known good pixel values
		dilation: amount of dilation to apply
	'''

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

		for good_pixel_mask in good_pixel_masks:
			pqa_mask[i][pqa_array == good_pixel_mask] = True
	return pqa_mask
