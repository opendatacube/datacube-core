import datacube
from datacube.utils.masking import mask_invalid_data

query = {
    'time': ('1990-01-01', '1991-01-01'),
    'lat': (-35.2, -35.4),
    'lon': (149.0, 149.2),
}

dc = datacube.Datacube(app='plot-rgb-recipe')
data = dc.load(product='ls5_nbar_albers', measurements=['red', 'green', 'blue'], **query)
data = mask_invalid_data(data)

fake_saturation = 4000
rgb = data.to_array(dim='color')
rgb = rgb.transpose(*(rgb.dims[1:]+rgb.dims[:1]))  # make 'color' the last dimension
rgb = rgb.where((rgb <= fake_saturation).all(dim='color'))  # mask out pixels where any band is 'saturated'
rgb /= fake_saturation  # scale to [0, 1] range for imshow

rgb.plot.imshow(x=data.crs.dimensions[1], y=data.crs.dimensions[0],
                col='time', col_wrap=5, add_colorbar=False)
