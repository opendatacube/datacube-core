# coding=utf-8
"""
Ingest data from the command-line.
"""
from __future__ import absolute_import
import uuid
import logging
from xml.etree import ElementTree
from pathlib import Path
import yaml

import click
from osgeo import osr
import os
os.environ["CPL_ZIP_ENCODING"] = "UTF-8"
# image boundary imports
import rasterio
from rasterio.errors import RasterioIOError
import rasterio.features
import shapely.affinity
import shapely.geometry
import shapely.ops
import zipfile

import hashlib
from datetime import datetime

# IMAGE BOUNDARY CODE


def safe_valid_region(images, mask_value=None):
    try:
        return valid_region(images, mask_value)
    except (OSError, RasterioIOError):
        return None


def valid_region(images, mask_value=None):
    mask = None
    for fname in images:
        logging.info("Valid regions for %s", fname)
        # ensure formats match
        with rasterio.open(str(fname), 'r') as ds:
            transform = ds.transform
            img = ds.read(1)

            if mask_value is not None:
                new_mask = img & mask_value == mask_value
            else:
                # new_mask = img != ds.nodata
                new_mask = img != 0
            if mask is None:
                mask = new_mask
            else:
                mask |= new_mask
            

    shapes = rasterio.features.shapes(mask.astype('uint8'), mask=mask)
    shape = shapely.ops.unary_union([shapely.geometry.shape(shape) for shape, val in shapes if val == 1])
    type(shapes)
    # convex hull
    geom = shape.convex_hull

    # buffer by 1 pixel
    geom = geom.buffer(1, join_style=3, cap_style=3)

    # simplify with 1 pixel radius
    geom = geom.simplify(1)

    # intersect with image bounding box
    geom = geom.intersection(shapely.geometry.box(0, 0, mask.shape[1], mask.shape[0]))

    # transform from pixel space into CRS space
    geom = shapely.affinity.affine_transform(geom, (transform.a, transform.b, transform.d,
                                                    transform.e, transform.xoff, transform.yoff))

    return geom



def _to_lists(x):
    """
    Returns lists of lists when given tuples of tuples
    """
    if isinstance(x, tuple):
        return [_to_lists(el) for el in x]

    return x


def get_geo_ref_points(root):
    nrows = int(root.findall('./*/Tile_Geocoding/Size[@resolution="10"]/NROWS')[0].text)
    ncols = int(root.findall('./*/Tile_Geocoding/Size[@resolution="10"]/NCOLS')[0].text)

    ulx = int(root.findall('./*/Tile_Geocoding/Geoposition[@resolution="10"]/ULX')[0].text)
    uly = int(root.findall('./*/Tile_Geocoding/Geoposition[@resolution="10"]/ULY')[0].text)

    xdim = int(root.findall('./*/Tile_Geocoding/Geoposition[@resolution="10"]/XDIM')[0].text)
    ydim = int(root.findall('./*/Tile_Geocoding/Geoposition[@resolution="10"]/YDIM')[0].text)

    return {
        'ul': {'x': ulx, 'y': uly},
        'ur': {'x': ulx + ncols * abs(xdim), 'y': uly},
        'll': {'x': ulx, 'y': uly - nrows * abs(ydim)},
        'lr': {'x': ulx + ncols * abs(xdim), 'y': uly - nrows * abs(ydim)},
    }


def get_coords(geo_ref_points, spatial_ref):
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        lon, lat, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}

    return {key: transform(p) for key, p in geo_ref_points.items()}


def prepare_dataset(path):
    if path.suffix == '.zip':
        zipfile.ZipFile(str(path))
        z = zipfile.ZipFile(str(path))
        # find the auxilliary metadata
        datastrip_auxilliary = [s for s in z.namelist() if "DATASTRIP" in s]
        for i in datastrip_auxilliary:
            if 'xml' in i:
                datastrip_metadata = i
        
        xmlzipfiles = [s for s in z.namelist() if "MTD_MSIL1C.xml" in s]
        if xmlzipfiles == []:
            pattern = str(path.name)
            pattern = pattern.replace('PRD_MSIL1C', 'MTD_SAFL1C')
            pattern = pattern.replace('.zip','.xml')
            xmlzipfiles = [s for s in z.namelist() if pattern in s]
            
        mtd_xml = z.read(xmlzipfiles[0])
        root = ElementTree.XML(mtd_xml)
        checksum_sha1 = hashlib.sha1(open(path, 'rb').read()).hexdigest()
        size_bytes = os.path.getsize(path)
        
    else:
        root = ElementTree.parse(str(path)).getroot()
    
    product_start_time = root.findall('./*/Product_Info/PRODUCT_START_TIME')[0].text
    product_stop_time = root.findall('./*/Product_Info/PRODUCT_STOP_TIME')[0].text
    # Looks like sometimes the stop time is before the start time....maybe just set them to be the same
    start_time = datetime.strptime(product_start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    stop_time = datetime.strptime(product_stop_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    
    if start_time > stop_time:
        dummy = product_stop_time
        product_stop_time = product_start_time
        product_start_time = dummy
    
    product_uri = Path(root.findall('./*/Product_Info/PRODUCT_URI')[0].text)
    level = root.findall('./*/Product_Info/PROCESSING_LEVEL')[0].text
    
    product_type = root.findall('./*/Product_Info/PRODUCT_TYPE')[0].text
    processing_baseline = root.findall('./*/Product_Info/PROCESSING_BASELINE')[0].text
    ct_time = root.findall('./*/Product_Info/GENERATION_TIME')[0].text
    datatake_id = root.findall('./*/Product_Info/Datatake')[0].attrib
    platform = root.findall('./*/Product_Info/*/SPACECRAFT_NAME')[0].text
    datatake_type = root.findall('./*/Product_Info/*/DATATAKE_TYPE')[0].text
    datatake_sensing_start = root.findall('./*/Product_Info/*/DATATAKE_SENSING_START')[0].text
    orbit = root.findall('./*/Product_Info/*/SENSING_ORBIT_NUMBER')[0].text
    orbit_direction = root.findall('./*/Product_Info/*/SENSING_ORBIT_DIRECTION')[0].text
    product_format = root.findall('./*/Product_Info/*/PRODUCT_FORMAT')[0].text
    
    if product_format == 'SAFE':
        product_format = 'SAFE_COMPACT'
    
    null = root.findall('./*/Product_Image_Characteristics/Special_Values/SPECIAL_VALUE_INDEX')[0].text
    saturated = root.findall('./*/Product_Image_Characteristics/Special_Values/SPECIAL_VALUE_INDEX')[1].text
    reflectance_conversion = root.findall('./*/Product_Image_Characteristics/Reflectance_Conversion/U')[0].text
    solar_irradiance = []
    for irradiance in root.iter('SOLAR_IRRADIANCE'):
        band_irradiance = irradiance.attrib
        band_irradiance['value'] = irradiance.text
        solar_irradiance.append(band_irradiance)
    
    cloud_coverage = root.findall('./*/Cloud_Coverage_Assessment')[0].text
    degraded_anc_data_percentage = root.findall('./*/Technical_Quality_Assessment/DEGRADED_ANC_DATA_PERCENTAGE')[0].text
    degraded_msi_data_percentage = root.findall('./*/Technical_Quality_Assessment/DEGRADED_MSI_DATA_PERCENTAGE')[0].text
    sensor_quality_flag = root.findall('./*/Quality_Control_Checks/Quality_Inspections/SENSOR_QUALITY_FLAG')[0].text
    geometric_quality_flag = root.findall('./*/Quality_Control_Checks/Quality_Inspections/GEOMETRIC_QUALITY_FLAG')[0].text
    general_quality_flag = root.findall('./*/Quality_Control_Checks/Quality_Inspections/GENERAL_QUALITY_FLAG')[0].text
    format_quality_flag = root.findall('./*/Quality_Control_Checks/Quality_Inspections/FORMAT_CORRECTNESS_FLAG')[0].text
    radiometric_quality_flag = root.findall('./*/Quality_Control_Checks/Quality_Inspections/RADIOMETRIC_QUALITY_FLAG')[0].text
    
    #Assume multiple granules
    single_granule_archive = False   
    
    granules = {granule.get('granuleIdentifier'): [imid.text for imid in granule.findall('IMAGE_ID')]
                for granule in root.findall('./*/Product_Info/Product_Organisation/Granule_List/Granules')}
 
    if not granules:
        
        single_granule_archive = True
        granules = {granule.get('granuleIdentifier'): [imid.text for imid in granule.findall('IMAGE_FILE')]
                for granule in root.findall('./*/Product_Info/Product_Organisation/Granule_List/Granule')}
        
        if not [] in granules.values():
            single_granule_archive = True
        else:
            # the dreaded third variant that looks like a single granule archive but has multiple granules...
            granules = {granule.get('granuleIdentifier'): [imid.text for imid in granule.findall('IMAGE_ID')]
                for granule in root.findall('./*/Product_Info/Product_Organisation/Granule_List/Granule')} 
            single_granule_archive = False
    
    documents = []
    
      
    
    for granule_id, images in granules.items():
        images_ten_list = []
        images_twenty_list = []
        images_sixty_list = []
        
        # Not required for Zip method - uses granule metadata
        img_data_path = str(path.parent.joinpath('GRANULE', granule_id, 'IMG_DATA'))
        
        if not path.suffix == '.zip':
            gran_path = str(path.parent.joinpath('GRANULE', granule_id, granule_id[:-7].replace('MSI', 'MTD') + '.xml'))
            root = ElementTree.parse(gran_path).getroot()
            
        else:
            
            xmlzipfiles = [s for s in z.namelist() if 'MTD_TL.xml' in s]
            if xmlzipfiles == []:

                pattern = granule_id.replace('MSI', 'MTD')
                pattern = pattern.replace('_N'+processing_baseline, '.xml')

                xmlzipfiles = [s for s in z.namelist() if pattern in s]
            mtd_xml = z.read(xmlzipfiles[0])
            root = ElementTree.XML(mtd_xml)
            img_data_path = str(path)+'!'

            img_data_path = 'zip:'+img_data_path+str(z.namelist()[0])
            
            #for earlier versions of zip archive - use GRANULES
            if single_granule_archive is False: 
                
                img_data_path = img_data_path+str(Path('GRANULE').joinpath(granule_id, 'IMG_DATA'))
                
                
        sensing_time = root.findall('./*/SENSING_TIME')[0].text
              
        # Add the QA band
        qi_band = root.findall('./*/PVI_FILENAME')[0].text
        qi_band = qi_band.replace('.jp2', '')
        
        images.append(qi_band)
        
        for image in images:
            ten_list = ['B02', 'B03', 'B04', 'B08']
            twenty_list = ['B05', 'B06', 'B07', 'B11', 'B12', 'B8A']
            sixty_list = ['B01', 'B09', 'B10']

            for item in ten_list:
                if item in image:
                    images_ten_list.append(os.path.join(img_data_path, image + ".jp2"))
            for item in twenty_list:
                if item in image:
                    images_twenty_list.append(os.path.join(img_data_path, image + ".jp2"))
            for item in sixty_list:
                if item in image:
                    images_sixty_list.append(os.path.join(img_data_path, image + ".jp2"))
                    
        
        
        tile_id = root.findall('./*/TILE_ID')[0].text
        mgrs_reference = tile_id.split('_')[9]
        datastrip_id = root.findall('./*/DATASTRIP_ID')[0].text
        downlink_priority = root.findall('./*/DOWNLINK_PRIORITY')[0].text

        sensing_time = root.findall('./*/SENSING_TIME')[0].text
        station = root.findall('./*/Archiving_Info/ARCHIVING_CENTRE')[0].text
        archiving_time = root.findall('./*/Archiving_Info/ARCHIVING_TIME')[0].text
        
        
        sun_zenith_angle = root.findall('./*/Tile_Angles/Mean_Sun_Angle/ZENITH_ANGLE')[0].text
        sun_azimuth_angle = root.findall('./*/Tile_Angles/Mean_Sun_Angle/AZIMUTH_ANGLE')[0].text

        viewing_zenith_azimuth_angle = []

        for viewing_incidence in root.iter('Mean_Viewing_Incidence_Angle'):
            view_incidence = viewing_incidence.attrib
            zenith_value = viewing_incidence.find('ZENITH_ANGLE').text
            azimuth_value = viewing_incidence.find('AZIMUTH_ANGLE').text
            view_incidence.update({'unit': 'degree', 'measurement':{'zenith': {'value': zenith_value}, 'azimith': {'value': azimuth_value}}})
            viewing_zenith_azimuth_angle.append(view_incidence)

        cs_code = root.findall('./*/Tile_Geocoding/HORIZONTAL_CS_CODE')[0].text
        spatial_ref = osr.SpatialReference()
        spatial_ref.SetFromUserInput(cs_code)

        geo_ref_points = get_geo_ref_points(root)
        img_dict = {}
        # fix image paths
        for image in images:
                        
            if image[-3:] in ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12', 'TCI']:
                #img_path = img_data_path+'/'+image + '.jp2'
                img_path = os.path.join(img_data_path, image + ".jp2")
                band_label = image[-3:]
            else:
                img_path = os.path.join(img_data_path, image + ".jp2")
                img_path = img_path.replace('IMG_DATA', 'QI_DATA')
                #img_path = img_data_path.replace('IMG_DATA', 'QI_DATA')+'/'+image+'.jp2'
                band_label = 'PVI'
                
            img_dict[band_label]= {'path': img_path, 'layer': 1}              

        documents.append({
            'id': str(uuid.uuid4()),
            'processing_level': level, #level.replace('Level-', 'L'),
            'product_type': product_type,
            'processing_baseline': processing_baseline,
            'datatake_id': datatake_id,
            'datatake_type': datatake_type,
            'datatake_sensing_start' : datatake_sensing_start,
            'orbit': orbit,
            'orbit_direction': orbit_direction,
            'creation_dt': ct_time,
            'size_bytes': size_bytes,
            'checksum_sha1': checksum_sha1,
            'platform': {'code': platform},
            'instrument': {'name': 'MSI'},
            'product_format': {'name': product_format},
            'product_uri': str(product_uri),
            'format': {'name': 'JPEG2000'},
            'tile_id': tile_id,
            'datastrip_id': datastrip_id,
            'datastrip_metadata': datastrip_metadata,
            'downlink_priority': downlink_priority,
            'archiving_time': archiving_time,
            'acquisition': {'groundstation': {'code': station}},
            
            'extent': {
                'from_dt': product_start_time,
                'to_dt': product_stop_time,
                'center_dt': sensing_time,
                'coord': get_coords(geo_ref_points, spatial_ref),
            },
            
            'grid_spatial': {
                'projection': {
                    'geo_ref_points': geo_ref_points,
                    'spatial_reference': spatial_ref.ExportToWkt(),
                    'valid_data': {
                        'coordinates': _to_lists(
                            shapely.geometry.mapping(
                                shapely.ops.unary_union([
                                    safe_valid_region(images_sixty_list)

                                ])
                            )['coordinates']),
                        'type': "Polygon"}
                }
            },
            'image': {
                'tile_reference': mgrs_reference,
                'cloud_cover_percentage': cloud_coverage,
                'sun_azimuth': sun_azimuth_angle,
                'sun_elevation': sun_zenith_angle,
                'viewing_angles': viewing_zenith_azimuth_angle,
                'degraded_anc_data_percentage': degraded_anc_data_percentage, 
                'degraded_msi_data_percentage': degraded_msi_data_percentage,
                'sensor_quality_flag': sensor_quality_flag,
                'geometric_quality_flag': geometric_quality_flag,
                'general_quality_flag': general_quality_flag,
                'format_quality_flag': format_quality_flag,
                'radiometric_quality_flag': radiometric_quality_flag,
                'null_value': null,
                'saturated': saturated,
                'reflectance_conversion': reflectance_conversion,
                'solar_irradiance': solar_irradiance,                
                'bands': img_dict
            },

            'lineage': {'source_datasets': {}},
        })
    return documents

def absolutify_paths(doc, path):
    for band in doc['image']['bands'].values():
        band['path'] = str(path / band['path'])
    return doc

@click.command(help="Prepare Sentinel 2 dataset for ingestion into the Data Cube.")
@click.option('--output', help="Write datasets into this directory",
              type=click.Path(exists=False, writable=True, dir_okay=True))
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@click.option('--checksum/--no-checksum', default=False)


def main(output, datasets, checksum):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    for dataset in datasets:
        path = Path(dataset)
        
        
        if path.is_dir():
            path = Path(path.joinpath(path.stem.replace('PRD_MSIL1C', 'MTD_SAFL1C') + '.xml'))
        if path.suffix not in ['.xml', '.zip']:#!= '.xml':
            raise RuntimeError('want xml or zipped archive')

        logging.info("Processing %s", path)

        output_path = Path(output)
        yaml_path = output_path.joinpath(path.name+'.yaml')
        logging.info("Output %s", yaml_path)
        if os.path.exists(yaml_path):
            logging.info("Output already exists %s", yaml_path)
            with open(yaml_path) as f:
                if checksum:
                    logging.info("Running checksum comparison")
                    dataMap = yaml.load_all(f)
                    for data in dataMap:
                        yaml_sha1 = data['checksum_sha1']
                        checksum_sha1 = hashlib.sha1(open(path, 'rb').read()).hexdigest()
                
                    if checksum_sha1 == yaml_sha1:
                        logging.info("Dataset preparation already done...SKIPPING")            
                        continue 
                else:
                   logging.info("Dataset preparation already done...SKIPPING")            
                   continue 
            
        documents = prepare_dataset(path)
        if documents:
            logging.info("Writing %s dataset(s) into %s", len(documents), yaml_path)
            with open(yaml_path, 'w') as stream:
                yaml.dump_all(documents, stream)
        else:
            logging.info("No datasets discovered. Bye!")


if __name__ == "__main__":
    main()
