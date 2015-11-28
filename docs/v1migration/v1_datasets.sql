
SELECT dblink_connect('agdcv1' ,'hostaddr=130.56.244.225 port=6432 user=cube_user password=GAcube0 dbname=hypercube_v0');

SELECT dblink_open('agdcv1', 'dataset', 'select
            d.dataset_id,
            a.acquisition_id,
             d.dataset_path,
             pl.level_name,
             s.satellite_name,
             sens.sensor_name,
             d.datetime_processed,
             a.start_datetime,
             a.end_datetime,
             a.ll_lat, a.ll_lon, a.lr_lat, a.lr_lon, a.ul_lat, a.ul_lon, a.ur_lat, a.ur_lon
         from dataset d
             natural inner join acquisition a
             natural inner join satellite s
             natural inner join sensor sens
             natural inner join processing_level pl
        where a.satellite_id = 1 and d.level_id = 2;');


insert into agdc.dataset(id, metadata_type, metadata_path, metadata)
select uuid, 'eo', dataset_path,
  json_build_object(
      'id', uuid,
      'ga_label', regexp_replace(dataset_path, '.+/', ''),
      'product_type', level_name,
      'creation_dt', datetime_processed,
      'platform', json_build_object('code', satellite_name),
      'instrument', json_build_object('name', sensor_name),
      '_agdc_legacy', json_build_object(
          'acquisition_id', acquisition_id,
          'dataset_id', dataset_id
      ),
      'extent', json_build_object(
          'coord', json_build_object(
              'ul', json_build_object('lat', ul_lat, 'lon', ul_lon),
              'ur', json_build_object('lat', ur_lat, 'lon', ur_lon),
              'll', json_build_object('lat', ll_lat, 'lon', ll_lon),
              'lr', json_build_object('lat', lr_lat, 'lon', lr_lon)
          ),
          'from_dt', start_datetime,
          'to_dt', end_datetime,
          'center_dt', start_datetime+(end_datetime-start_datetime)/2
      )
  )::jsonb
from (
    select uuid_generate_v4() as uuid, *
    from dblink_fetch('agdcv1', 'dataset', 1) as (
         dataset_id int,
         acquisition_id int,
         dataset_path text,
         level_name text,
         satellite_name text,
         sensor_name text,
         datetime_processed timestamp,
         start_datetime timestamp,
         end_datetime timestamp,
         ll_lat float8, ll_lon float8,
         lr_lat float8, lr_lon float8,
         ul_lat float8, ul_lon float8,
         ur_lat float8, ur_lon float8
         )
  ) foo;

select dblink_close('agdcv1', 'dataset');
