-- Index using Postgres range types, using a view for convenient access.

create index ix_dataset_metadata_lat_range on odc.dataset using gist (
    numrange(
            least(
                    CAST(metadata #>> '{extent, coord, ul, lat}' as numeric),
                    CAST(metadata #>> '{extent, coord, ll, lat}' as numeric)
            ),
            greatest(
                    CAST(metadata #>> '{extent, coord, ur, lat}' as numeric),
                    CAST(metadata #>> '{extent, coord, lr, lat}' as numeric)
            ),
            '[]'
    )
);

create index ix_dataset_metadata_lon_range on odc.dataset using gist (
    numrange(
            least(
                    CAST(metadata #>> '{extent, coord, ll, lon}' as numeric),
                    CAST(metadata #>> '{extent, coord, lr, lon}' as numeric)
            ),
            greatest(
                    CAST(metadata #>> '{extent, coord, ul, lon}' as numeric),
                    CAST(metadata #>> '{extent, coord, ur, lon}' as numeric)
            ),
            '[]'
    )
);


create index id_dataset_metadata on odc.dataset (metadata);

drop index ix_dataset_md_sat;
create index ix_dataset_md_sat on odc.dataset (upper(metadata #>> '{platform, code}'));

drop view eo_dataset;
create view eo_dataset as
    select
        id,
        upper(metadata #>> '{platform, code}') as satellite,
        type                                   as type,
        numrange(
                least(
                        CAST(metadata #>> '{extent, coord, ul, lat}' as numeric),
                        CAST(metadata #>> '{extent, coord, ll, lat}' as numeric)
                ),
                greatest(
                        CAST(metadata #>> '{extent, coord, ur, lat}' as numeric),
                        CAST(metadata #>> '{extent, coord, lr, lat}' as numeric)
                ),
                '[]'
        )                                      as lat,
        numrange(
                least(
                        CAST(metadata #>> '{extent, coord, ll, lon}' as numeric),
                        CAST(metadata #>> '{extent, coord, lr, lon}' as numeric)
                ),
                greatest(
                        CAST(metadata #>> '{extent, coord, ul, lon}' as numeric),
                        CAST(metadata #>> '{extent, coord, ur, lon}' as numeric)
                ),
                '[]'
        )                                      as lon,
        metadata,
        metadata_path
    from odc.dataset;


select *
from eo_dataset
limit 3;


select *
from eo_dataset
where lat @> -30 :: numeric and
      lon @> 136 :: numeric and
      satellite = 'LANDSAT-8' and
      type = 'nbar';
