from datacube.model import Dataset, DatasetType, MetadataType


def mk_sample_product(name,
                      description='Sample',
                      measurements=['red', 'green', 'blue']):

    eo_type = MetadataType({
        'name': 'eo',
        'description': 'Sample',
        'dataset': dict(
            id=['id'],
            label=['ga_label'],
            creation_time=['creation_dt'],
            measurements=['image', 'bands'],
            sources=['lineage', 'source_datasets'],
            format=['format', 'name'],
        )
    }, dataset_search_fields={})

    common = dict(dtype='int16',
                  nodata=-999,
                  units='1',
                  aliases=[])

    def mk_measurement(m):
        if isinstance(m, str):
            return dict(name=m, **common)
        if isinstance(m, tuple):
            name, dtype, nodata = m
            m = common.copy()
            m.update(name=name, dtype=dtype, nodata=nodata)
            return m
        if isinstance(m, dict):
            m_merged = common.copy()
            m_merged.update(m)
            return m_merged

        assert False and 'Only support str|dict|(name, dtype, nodata)'

    measurements = [mk_measurement(m) for m in measurements]

    return DatasetType(eo_type, dict(
        name=name,
        description=description,
        metadata_type='eo',
        metadata={},
        measurements=measurements
    ))


def mk_sample_dataset(bands,
                      uri='file:///tmp',
                      product_name='sample',
                      format='GeoTiff',
                      id='12345678123456781234567812345678'):
    image_bands_keys = 'path layer band'.split(' ')
    measurement_keys = 'dtype units nodata aliases name'.split(' ')

    def with_keys(d, keys):
        return dict((k, d[k]) for k in keys if k in d)

    measurements = [with_keys(m, measurement_keys) for m in bands]
    image_bands = dict((m['name'], with_keys(m, image_bands_keys)) for m in bands)

    ds_type = mk_sample_product(product_name,
                                measurements=measurements)

    return Dataset(ds_type, {
        'id': id,
        'format': {'name': format},
        'image': {'bands': image_bands}
    }, uris=[uri])
