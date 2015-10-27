#!/usr/bin/env python

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither Geoscience Australia nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# ===============================================================================

"""
Fixer for uncatalogued mosaics in AGDC database

Created on Jun 23, 2015

@author: Alex Ip
"""
import os
import sys
import logging
from osgeo import gdal

from gdf import Database
from gdf. _gdfutils import make_dir

# Set handler for root logger to standard output
console_handler = logging.StreamHandler(sys.stdout)
# console_handler.setLevel(logging.INFO)
console_handler.setLevel(logging.DEBUG)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)
logging.root.addHandler(console_handler)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Initial logging level for this module

# DB_HOST = '130.56.244.224'
DB_HOST = '130.56.244.228'
DB_PORT = 6432
# DB_NAME = 'hypercube_v0'
DB_NAME = 'agdc_snapshot_20150622'
DB_USER = 'cube_admin'
DB_PASSWORD = 'GAcube!'


def create_mosaic_file(record):
    mosaic_tile_path = record['tile_pathname']
    tile_pathname1 = record['tile_pathname1']
    assert os.path.exists(tile_pathname1), 'First source tile %s does not exist' % tile_pathname1
    tile_pathname2 = record['tile_pathname2']
    assert os.path.exists(tile_pathname2), 'Second source tile %s does not exist' % tile_pathname2

    make_dir(os.path.dirname(mosaic_tile_path))

    raise Exception('create_mosaic_file not implemented yet')

    logger.info('Creating Dataset %s', mosaic_tile_path)

    # TODO: Finish this
    if record['level_name'] == 'PQA':
        # Make bitwise-and composite
        pass
    else:
        # Make VRT
        pass


def write_mosaic_records(database, record):
    sql = '''-- Insert record for existing mosaic tile
insert into tile (
  x_index,
  y_index,
  tile_type_id,
  tile_pathname,
  dataset_id,
  tile_class_id,
  tile_size,
  )
values (
  %(x_index)s,
  %(y_index)s,
  %(tile_type_id)s,
  %(tile_pathname)s, -- Mosaic tile path
  %(dataset_id1)s,
  4, -- Mosaic class
  0, -- Change this to file size later
  )

update tile
set tile_class_id = 3
where tile_class_id = 1
and
tile_id = in (%(tile_id1)s, %(tile_id1)s);
'''
    database.submit_query(sql, record)


def main():
    database = Database(db_ref='agdc', host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)

    sql = '''-- Find all overlap tiles
SELECT * from ztmp.temp; # Here's one I prepared earlier...
/*
SELECT
    level_name,
    t1.tile_type_id,
    t1.x_index,
    t1.y_index,
    a1.start_datetime,
    a2.end_datetime,
    a1.acquisition_id AS acquisition_id1,
    d1.dataset_id AS dataset_id1,
    t1.tile_id AS tile_id1,
    a2.acquisition_id AS acquisition_id2,
    d2.dataset_id AS dataset_id2,
    t2.tile_id AS tile_id2,
    regexp_replace(regexp_replace(t1.tile_pathname, '(/(-)*\d{3}_(-)*\d{3}/\d{4}/)(.*)\.\w+$'::text, '\1mosaic_cache/\4.vrt'::text), '(.*_PQA_.*)\.vrt$'::text, '\1.tif'::text) AS tile_pathname,
    a1.x_ref as path,
    a1.y_ref as row1,
    a2.y_ref as row2,
    a1.end_datetime as first_end_datetime,
    a2.start_datetime as second_start_datetime,
    d1.dataset_path as dataset_path1,
    d2.dataset_path as dataset_path2,
    t1.tile_pathname as tile_pathname1,
    t2.tile_pathname as tile_pathname2
   FROM acquisition a1
     JOIN dataset d1 ON d1.acquisition_id = a1.acquisition_id
     JOIN tile t1 ON t1.dataset_id = d1.dataset_id AND (t1.tile_class_id = 1 OR t1.tile_class_id = 3)
     JOIN acquisition a2 ON a1.satellite_id = a2.satellite_id AND a1.sensor_id = a2.sensor_id AND a1.x_ref = a2.x_ref AND a1.y_ref = (a2.y_ref - 1) AND a1.end_datetime - a2.start_datetime between interval '-12 seconds' and interval '12 seconds' AND a1.acquisition_id <> a2.acquisition_id
     JOIN dataset d2 ON d2.acquisition_id = a2.acquisition_id AND d1.level_id = d2.level_id
     JOIN tile t2 ON t2.dataset_id = d2.dataset_id AND t1.tile_type_id = t2.tile_type_id AND (t2.tile_class_id = 1 OR t2.tile_class_id = 3) AND t1.x_index = t2.x_index AND t1.y_index = t2.y_index
     JOIN processing_level on d1.level_id = processing_level.level_id
     LEFT JOIN tile mt ON mt.tile_class_id = 4 AND mt.dataset_id = t1.dataset_id AND mt.tile_type_id = t1.tile_type_id AND mt.x_index = t1.x_index AND mt.y_index = t1.y_index
where t1.tile_type_id = 1
and t1.tile_class_id in (1,3) --Non-overlapped & overlapped
and t2.tile_class_id in (1,3) --Non-overlapped & overlapped
and mt.tile_id is null -- Uncatalogued
order by t1.x_index, t1.y_index, level_name, a1.end_datetime;
*/
'''

    uncatalogued_mosaics = database.submit_query(sql)

    for record in uncatalogued_mosaics:
        mosaic_file_ok = False
        mosaic_tile_path = record['tile_pathname']

        try:
            if os.path.exists(mosaic_tile_path):
                dataset = gdal.Open(mosaic_tile_path)
                if not dataset:
                    logger.warning('Unable to open dataset %s using gdal', mosaic_tile_path)
                    os.remove(mosaic_tile_path)
                else:
                    dataset.Close()
                    mosaic_file_ok = True
            else:
                logger.warning('Dataset %s does not exist', mosaic_tile_path)

            if not mosaic_file_ok:
                create_mosaic_file(record)
            else:
                logger.info('Dataset %s is OK', mosaic_tile_path)

            write_mosaic_records(database, record)

        except Exception as e:
            logger.warning('Exception raised while processing %s: %s', mosaic_tile_path, e.message)


if __name__ == '__main__':
    main()
