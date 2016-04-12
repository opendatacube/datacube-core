create extension if not exists "uuid-ossp";

begin;

\echo '-- Removing collections --'
drop view agdc.eo_dataset;
drop view agdc.eo_storage_unit;
alter table agdc.dataset
  drop column collection_ref;
alter table agdc.storage_unit
  drop column collection_ref;
drop table agdc.collection;


\echo '-- Creating dataset_type --'
create table agdc.dataset_type (
  id                      smallserial                            not null,
  name                    varchar                                not null,
  metadata                jsonb                                  not null,
  metadata_type_ref       smallint                               not null,
  added                   timestamp with time zone default now() not null,
  added_by                varchar default CURRENT_USER           not null,

  -- If this type was derived from a legacy storage type.
  source_storage_type_ref smallint                               null,

  constraint pk_dataset_type primary key (id),
  constraint ck_dataset_type_alphanumeric_name check (name ~* '^\w+$'),
  constraint uq_dataset_type_name unique (name),
  constraint fk_dataset_type_metadata_type_ref_metadata_type foreign key (metadata_type_ref) references agdc.metadata_type (id)
);


alter table agdc.dataset
  add column dataset_type_ref smallint null;

alter table agdc.dataset
  add constraint fk_dataset_dataset_type_ref_dataset_type
foreign key (dataset_type_ref) references agdc.dataset_type (id);

alter table agdc.dataset
  add column archived timestamp with time zone null default null;

-- Optional reference to storage type that created the dataset.
alter table agdc.dataset
  add column storage_type_ref smallint null;
alter table agdc.dataset
  add constraint fk_dataset_storage_type_ref_storage_type
foreign key (storage_type_ref) references agdc.storage_type (id);

-- --------------- DATASET IMPORT --------------- --

\echo '-- Creating dataset types for existing datasets --'

with unique_dataset_types as (
    select
      d.metadata -> 'platform' ->> 'code' as platform,
      d.metadata ->> 'product_type'       as type,
      d.metadata ->> 'product_level'      as product_level,
      d.metadata ->> 'ga_level'           as ga_level,
      d.metadata -> 'format' ->> 'name'   as format
    from agdc.dataset d
    group by 1, 2, 3, 4, 5
)
insert into agdc.dataset_type (name, metadata, metadata_type_ref)
  select
    -- Create a name for the dataset type.
    concat_ws('_',
              replace(lower(platform), 'landsat_', 'ls'),
              replace(lower(type), 'satellite_telemetry_data', 'telemetry'),
              lower(coalesce(product_level, ga_level)),
              replace(lower(format), 'geotiff', 'gtiff')
    ),
    -- Common fields for this dataset type.
    json_build_object(
        'platform', json_build_object('code', platform),
        'product_type', type,
        -- Assume everything either has either a 'product_level' or a 'ga_level'
        case when product_level is null
          then 'ga_level'
        else 'product_level' end,
        case when product_level is null
          then ga_level
        else product_level end,
        'format', json_build_object('name', format)
    ),
    -- They are all the default metadata_type 'eo'
    (select id
     from agdc.metadata_type
     where name = 'eo')
  from unique_dataset_types;

-- Set the metadata type for every dataset.
-- They'll all match exactly one. (it will fail loudly otherwise)
-- (SLOW)
\echo '-- Assigning each dataset a dataset type --'
update agdc.dataset d
set dataset_type_ref = (
  select dt.id
  from agdc.dataset_type dt
  where d.metadata @> dt.metadata
);

-- Metadata types are now mandatory.
\echo '-- Making dataset types mandatory --'
alter table agdc.dataset
  alter column dataset_type_ref set not null;

-- ---------------STORAGE IMPORT --------------- --



\echo '-- Adding a metadata type for legacy storage unit descriptors --'
insert into agdc.metadata_type (name, definition) values (
  'storage_unit',
  $$
    {
  "name": "storage_unit",
  "description": "Imported, legacy storage units.",
  "dataset": {
    "search_fields": {
      "platform": {
        "offset": ["platform", "code"],
        "description": "Platform code"
      },
      "instrument": {
        "offset": ["instrument", "name"],
        "description": "Instrument name"
      },
      "lat": {
        "type": "float-range",
        "max_offset": [["extents", "geospatial_lat_max"]],
        "min_offset": [["extents", "geospatial_lat_min"]],
        "description": "Latitude range"
      },
      "lon": {
        "type": "float-range",
        "max_offset": [["extents", "geospatial_lon_max"]],
        "min_offset": [["extents", "geospatial_lon_min"]],
        "description": "Longitude range"
      },
      "time": {
        "type": "datetime-range",
        "max_offset": [["extents", "time_max"]],
        "min_offset": [["extents", "time_min"]
        ],
        "description": "Acquisition time"
      }
    }
  }
}
    $$
);

\echo '-- Creating dataset types for existing storage units --'

-- TODO: Add projection/filetype/other params?
insert into agdc.dataset_type (name, metadata, metadata_type_ref, source_storage_type_ref)
  select
    st.name,
    st.dataset_metadata,
    (select id
     from agdc.metadata_type
     where name = 'storage_unit'),
    st.id
  from agdc.storage_type st;


\echo '-- Assigning dataset types to each storage unit --'

alter table agdc.storage_unit
  add column dataset_uuid uuid not null default uuid_generate_v4();

insert into agdc.dataset (id, metadata_type_ref, dataset_type_ref, metadata, added, added_by)
  select
    s.dataset_uuid,
    (select id
     from agdc.metadata_type
     where name = 'storage_unit'),
    dt.id,
    dt.metadata || s.descriptor || json_build_object('size_bytes', s.size_bytes) :: jsonb,
    s.added,
    s.added_by
  from agdc.storage_unit s
    -- Outer join so that it fails if a storage unit is missed.
    left outer join agdc.dataset_type dt on s.storage_type_ref = dt.source_storage_type_ref;

\echo '-- Adding dataset locations for the storage units'
insert into agdc.dataset_location (dataset_ref, uri_scheme, uri_body, added, added_by)
  select
    su.dataset_uuid,
    'file',
    -- TODO: get location from config (once run from python)
    '///g/data/u46/public/datacube/data' || '/' || su.path,
    su.added,
    su.added_by
  from agdc.storage_unit su;


\echo '-- Linking the "storage unit" datasets to their source datasets.'
insert into agdc.dataset_source (dataset_ref, classifier, source_dataset_ref)
  select
    su.dataset_uuid,
    date_trunc('seconds', (d.metadata -> 'extent' ->> 'center_dt') :: timestamp) :: text,
    d.id
  from agdc.storage_unit su
    inner join agdc.dataset_storage ds on su.id = ds.storage_unit_ref
    inner join agdc.dataset d on d.id = ds.dataset_ref;

commit;
