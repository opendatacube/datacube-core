-- Index each dimension using postgres range types
-- (Also: numeric, not float, which may be slower?)

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


select *
from odc.dataset
where numrange(
              least(
                      cast(metadata #>> '{extent, coord, ul, lat}' as numeric),
                      cast(metadata #>> '{extent, coord, ll, lat}' as numeric)
              ),
              greatest(
                      cast(metadata #>> '{extent, coord, ur, lat}' as numeric),
                      cast(metadata #>> '{extent, coord, lr, lat}' as numeric)
              ),
              '[]'
      ) @> (-30 :: numeric) and
      numrange(
              least(
                      cast(metadata #>> '{extent, coord, ll, lon}' as numeric),
                      cast(metadata #>> '{extent, coord, lr, lon}' as numeric)
              ),
              greatest(
                      cast(metadata #>> '{extent, coord, ul, lon}' as numeric),
                      cast(metadata #>> '{extent, coord, ur, lon}' as numeric)
              ),
              '[]'
      ) @> (136 :: numeric)
      and dataset.metadata -> 'platform' ->> 'code' = 'Landsat-8'
                                             and dataset.metadata ->> 'product_type' = 'NBAR';


explain analyse select *
                from odc.dataset
                where numrange(
                              least(
                                      cast(metadata #>> '{extent, coord, ul, lat}' as numeric),
                                      cast(metadata #>> '{extent, coord, ll, lat}' as numeric)
                              ),
                              greatest(
                                      cast(metadata #>> '{extent, coord, ur, lat}' as numeric),
                                      cast(metadata #>> '{extent, coord, lr, lat}' as numeric)
                              ),
                              '[]'
                      ) @> (-30 :: numeric) and
                      numrange(
                              least(
                                      cast(metadata #>> '{extent, coord, ll, lon}' as numeric),
                                      cast(metadata #>> '{extent, coord, lr, lon}' as numeric)
                              ),
                              greatest(
                                      cast(metadata #>> '{extent, coord, ul, lon}' as numeric),
                                      cast(metadata #>> '{extent, coord, ur, lon}' as numeric)
                              ),
                              '[]'
                      ) @> (136 :: numeric)
                      and dataset.metadata @> '{"platform": {"code": "Landsat-8"}, "product_type": "NBAR"}' :: jsonb
                limit 3;


select
    CAST(metadata #>> '{extent, coord, ul, lat}' as numeric),
    CAST(metadata #>> '{extent, coord, ll, lat}' as numeric),
    CAST(metadata #>> '{extent, coord, ur, lat}' as numeric),
    cast(metadata #>> '{extent, coord, lr, lat}' as numeric)
from odc.dataset
limit 3;
