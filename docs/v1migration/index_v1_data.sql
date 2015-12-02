
-- run as admin once
create extension postgres_fdw;

create server agdcv1_database
foreign data wrapper postgres_fdw
options (host '130.56.244.225', port '5432', dbname 'hypercube_v0');

create user mapping for gxr547
server agdcv1_database
options ( user 'cube_user', password 'GAcube0');

grant usage on foreign server agdcv1_database to gxr547;

-- user code
create index on agdc.dataset ((metadata->'_agdc_legacy'));

create or replace function pg_temp.v1datasets(satellite int, level int)
  returns table(
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
language plpgsql
as $$
begin
  return query select *
               from
                 dblink('agdcv1_database',
                        'select
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
                        where a.satellite_id = ' || satellite ||  'and d.level_id = ' || level || ';') as (
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
                 );
end $$;


create or replace function pg_temp.v1tiles(satellite int, level int)
  returns table(
  tile_pathname text,
  ctime timestamp,
  x_index int,
  y_index int,
  acquisition_id int,
  dataset_id int)
language plpgsql
as $$
begin
  return query select *
   from dblink('agdcv1_database',
               'select tile_pathname, ctime, x_index, y_index, acquisition_id, dataset_id
               from public.tile t
                   natural inner join dataset d
                   natural inner join acquisition a
               where a.satellite_id = ' || satellite || 'and
                     d.level_id = ' || level || ' and
                     t.tile_class_id in (1,3);') as (
        tile_pathname text,
        ctime timestamp,
        x_index int,
        y_index int,
        acquisition_id int,
        dataset_id int
        );
end $$;


create or replace function pg_temp.index_v1datasets(satellite int, level int) returns void
language plpgsql
as $$
begin
  insert into agdc.dataset (id, collection_ref, metadata_path, metadata)
    select
      uuid,
      (select id from agdc.collection where name = 'eo'),
      dataset_path,
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
              'center_dt', start_datetime + (end_datetime - start_datetime) / 2
          )
      ) :: jsonb
    from (
           select uuid_generate_v4() as uuid, *from pg_temp.v1datasets(satellite, level)
         ) foo;
end $$;

drop function pg_temp.index_v1tiles(mapping int, satellite integer, level integer, measurements jsonb);
create or replace function pg_temp.index_v1tiles(mapping smallint, satellite integer, level integer, measurements jsonb)
  returns table(id integer, descriptor jsonb)
language plpgsql
as $$
begin
  return query
  insert into agdc.storage_unit (storage_mapping_ref, collection_ref, path, descriptor)
    select
      mapping,
      (select agdc.collection.id from agdc.collection where name = 'eo'),
      regexp_replace(tile_pathname, '.+pixel/', ''),
      json_build_object(
          '_agdc_legacy', json_build_object(
              'acquisition_id', acquisition_id,
              'dataset_id', dataset_id
          ),
          'extents', json_build_object(
              'time_max', ctime,
              'time_min', ctime,
              'geospatial_lat_max', y_index + 1,
              'geospatial_lat_min', y_index,
              'geospatial_lon_max', x_index + 1,
              'geospatial_lon_min', x_index
          ),
          'coordinates', json_build_object(
              'latitude', json_build_object(
                  'end', y_index + 0.00025,
                  'begin', y_index + 1,
                  'dtype', 'float64',
                  'units', 'degrees_north',
                  'length', 4000
              ),
              'longitude', json_build_object(
                  'end', x_index + 0.99975,
                  'begin', x_index,
                  'dtype', 'float64',
                  'units', 'degrees_east',
                  'length', 4000)
          ),
          'measurements', measurements
      ) :: jsonb
    from pg_temp.v1tiles(satellite, level)
    returning agdc.storage_unit.id, agdc.storage_unit.descriptor;
end $$;

create temp table config(name text, satellite int, level int, measurements jsonb);
insert into config values ('LS5 NBAR V1', 1, 2, '{"layer1":{"dtype":"int16","nodata":-999,"dimensions":["latitude","longitude"]},
                                   "layer2":{"dtype":"int16","nodata":-999,"dimensions":["latitude","longitude"]},
                                   "layer3":{"dtype":"int16","nodata":-999,"dimensions":["latitude","longitude"]},
                                   "layer4":{"dtype":"int16","nodata":-999,"dimensions":["latitude","longitude"]},
                                   "layer5":{"dtype":"int16","nodata":-999,"dimensions":["latitude","longitude"]},
                                   "layer6":{"dtype":"int16","nodata":-999,"dimensions":["latitude","longitude"]}}'::jsonb);
insert into config values ('LS5 PQA V1', 1, 3, '{"layer1":{"dtype":"int16","dimensions":["latitude","longitude"]}}'::jsonb);

do
$$
declare
  mapping_name text;
  satellite int;
  level int;
  measurements jsonb;
  mapping_id smallint;
begin

  -- check 'eo' collection exists
  -- check '25m_bands_geotif' exists

for mapping_name, satellite, level, measurements in select * from config loop

  insert into agdc.storage_mapping (storage_type_ref, name, location_name, file_path_template,
                                    measurements,dataset_metadata)
  select
    st.id,
    mapping_name,
    'v1tiles',
    'not_used.tif',
    '{}' :: jsonb,
    '{"dont_match_me":"bro"}' :: jsonb
  from agdc.storage_type st
  where st.name = '25m_bands_geotif'
  returning agdc.storage_mapping.id
  into mapping_id;

  RAISE NOTICE  'indexing % datasets', mapping_name;
  perform pg_temp.index_v1datasets(satellite, level);

  RAISE NOTICE  'indexing % tiles', mapping_name;
  insert into agdc.dataset_storage (dataset_ref, storage_unit_ref)
    select
      (d.metadata ->> 'id') :: uuid,
      t.id
    from
      pg_temp.index_v1tiles(mapping_id, satellite, level, measurements) t
      join agdc.dataset d on (d.metadata -> '_agdc_legacy') = (t.descriptor -> '_agdc_legacy');

end loop;
end$$;
