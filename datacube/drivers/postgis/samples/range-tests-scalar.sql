-- Index each dimension min/max value separately as traditional btree-indexed scalars.

create index ix_dataset_metadata_lat_min on agdc.dataset (
    least(
            CAST(metadata #>> '{extent, coord, ul, lat}' as float),
            CAST(metadata #>> '{extent, coord, ll, lat}' as float)
    )
);
create index ix_dataset_metadata_lat_max on agdc.dataset (
    greatest(
            CAST(metadata #>> '{extent, coord, ur, lat}' as float),
            CAST(metadata #>> '{extent, coord, lr, lat}' as float)
    )
);

create index ix_dataset_metadata_lon_min on agdc.dataset (
    least(
            CAST(metadata #>> '{extent, coord, ll, lon}' as float),
            CAST(metadata #>> '{extent, coord, lr, lon}' as float)
    )
);
create index ix_dataset_metadata_lon_max on agdc.dataset (
    greatest(
            CAST(metadata #>> '{extent, coord, ul, lon}' as float),
            CAST(metadata #>> '{extent, coord, ur, lon}' as float)
    )
);

create index id_dataset_metadata on agdc.dataset (metadata);


explain analyse select *
                from agdc.dataset
                where
                    -30 :: float between
                    least(
                            cast(metadata #>> '{extent, coord, ul, lat}' as float),
                            cast(metadata #>> '{extent, coord, ll, lat}' as float)
                    ) and
                    greatest(
                            cast(metadata #>> '{extent, coord, ur, lat}' as float),
                            cast(metadata #>> '{extent, coord, lr, lat}' as float)
                    )
                    and 136 :: float between
                    least(
                            cast(metadata #>> '{extent, coord, ll, lon}' as float),
                            cast(metadata #>> '{extent, coord, lr, lon}' as float)
                    ) and
                    greatest(
                            cast(metadata #>> '{extent, coord, ul, lon}' as float),
                            cast(metadata #>> '{extent, coord, ur, lon}' as float)
                    )
                    and dataset.metadata #>> '{platform, code}' = 'Landsat-8' and
                                             dataset.metadata #>> '{product_type}' = 'NBAR';
