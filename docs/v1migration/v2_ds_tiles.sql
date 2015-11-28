
with ins as
(
insert into agdc.storage_unit(storage_mapping_ref, path, descriptor)
select 1,
  regexp_replace(tile_pathname, '.+LS5_TM/', ''),
  json_build_object(
  '_agdc_legacy', json_build_object(
      'acquisition_id', acquisition_id,
      'dataset_id', dataset_id
  ),
  'extents', json_build_object(
    'time_max', ctime,
    'time_min', ctime,
    'geospatial_lat_max', y_index+1,
    'geospatial_lat_min', y_index,
    'geospatial_lon_max', x_index+1,
    'geospatial_lon_min', x_index
  ),
  'coordinates', json_build_object(
    'latitude', json_build_object(
      'end', y_index+0.00025,
      'begin', y_index+1,
      'dtype', 'float64',
      'units', 'degrees_north',
      'length', 4000
    ),
    'longitude', json_build_object(
      'end', x_index+0.99975,
      'begin', x_index,
      'dtype', 'float64',
      'units', 'degrees_east',
      'length', 4000)
  ),
  'measurements', '{
    "layer1": {
      "dtype": "int16",
      "units": null,
      "nodata": -999,
      "dimensions": [
        "latitude",
        "longitude"
      ]
    },
    "layer2": {
      "dtype": "int16",
      "units": null,
      "nodata": -999,
      "dimensions": [
        "latitude",
        "longitude"
      ]
    },
    "layer3": {
      "dtype": "int16",
      "units": null,
      "nodata": -999,
      "dimensions": [
        "latitude",
        "longitude"
      ]
    },
    "layer4": {
      "dtype": "int16",
      "units": null,
      "nodata": -999,
      "dimensions": [
        "latitude",
        "longitude"
      ]
    },
    "layer5": {
      "dtype": "int16",
      "units": null,
      "nodata": -999,
      "dimensions": [
        "latitude",
        "longitude"
      ]
    },
    "layer6": {
      "dtype": "int16",
      "units": null,
      "nodata": -999,
      "dimensions": [
        "latitude",
        "longitude"
      ]
    }
  }'::json
)::jsonb
    from dblink('hostaddr=130.56.244.225 port=6432 user=cube_user password=GAcube0 dbname=hypercube_v0',
                'select tile_pathname, ctime, x_index, y_index, acquisition_id, dataset_id
    from public.tile t
        natural inner join dataset d
        natural inner join acquisition a
where d.level_id = 2 and a.satellite_id = 1;') as (
      tile_pathname text,
      ctime timestamp,
      x_index int,
      y_index int,
      acquisition_id int,
      dataset_id int
         )
RETURNING id, (select (metadata ->> 'id')::uuid
from agdc.dataset where
  (metadata -> '_agdc_legacy') = (descriptor -> '_agdc_legacy')))

insert into agdc.dataset_storage (storage_unit_ref, dataset_ref)
select * from ins;