
Loading 3D Datasets
-------------------

Uses
:class:`datacube.Datacube`
:class:`xarray.DataSet`
:meth:`datacube.Datacube.load`

Below are some examples of loading a 3D dataset, using small test dataset supplied in
``tests/data/lbg/gedi/``. Support for 3D datasets requires a :ref:`3D read driver <extending-datacube-3d-reads>`
and :ref:`product-doc-extra-dim`.


**Loading all time slices of a 3D dataset**

.. code-block:: python

    import datacube

    dc = datacube.Datacube()

    query = {
        "latitude": (-35.45, -35.25),
        "longitude": (149.0, 149.2),
        "output_crs": "EPSG:4326",
        "resolution": (0.00027778, -0.00027778),
    }

    dc.load(product='gedi_l2b_cover_z', **query)


Returns a 3D (+ time) Dataset with ``z`` coordinates in addition to ``latitude``/``longitude``:

.. code-block::

    <xarray.Dataset>
    Dimensions:      (latitude: 720, longitude: 721, time: 2, z: 30)
    Coordinates:
    * time         (time) datetime64[ns] 2019-08-16T09:28:51 2019-10-21T15:54:01
    * latitude     (latitude) float64 -35.45 -35.45 -35.45 ... -35.25 -35.25
    * longitude    (longitude) float64 149.2 149.2 149.2 ... 149.0 149.0 149.0
        spatial_ref  int32 4326
    * z            (z) float64 5.0 10.0 15.0 20.0 25.0 ... 135.0 140.0 145.0 150.0
    Data variables:
        cover_z      (time, z, latitude, longitude) float32 -9.999e+03 ... -9.999...
    Attributes:
        crs:           EPSG:4326
        grid_mapping:  spatial_ref


**Slice the dataset along the `z` dimension**

.. code-block:: python

    dc.load(product='gedi_l2b_cover_z', z=(30, 50), **query)

.. code-block::

    <xarray.Dataset>
    Dimensions:      (latitude: 720, longitude: 721, time: 2, z: 5)
    Coordinates:
    * time         (time) datetime64[ns] 2019-08-16T09:28:51 2019-10-21T15:54:01
    * latitude     (latitude) float64 -35.45 -35.45 -35.45 ... -35.25 -35.25
    * longitude    (longitude) float64 149.2 149.2 149.2 ... 149.0 149.0 149.0
        spatial_ref  int32 4326
    * z            (z) float64 30.0 35.0 40.0 45.0 50.0
    Data variables:
        cover_z      (time, z, latitude, longitude) float32 -9.999e+03 ... -9.999...
    Attributes:
        crs:           EPSG:4326
        grid_mapping:  spatial_ref


**Query the dataset at a single `z` coordinate**

.. code-block:: python

    dc.load(product='gedi_l2b_cover_z', z=30, **query)

.. code-block::

    <xarray.Dataset>
    Dimensions:      (latitude: 720, longitude: 721, time: 2, z: 1)
    Coordinates:
    * time         (time) datetime64[ns] 2019-08-16T09:28:51 2019-10-21T15:54:01
    * latitude     (latitude) float64 -35.45 -35.45 -35.45 ... -35.25 -35.25
    * longitude    (longitude) float64 149.2 149.2 149.2 ... 149.0 149.0 149.0
        spatial_ref  int32 4326
    * z            (z) float64 30.0
    Data variables:
        cover_z      (time, z, latitude, longitude) float32 -9.999e+03 ... -9.999...
    Attributes:
        crs:           EPSG:4326
        grid_mapping:  spatial_ref


**Use dask to chunk the dataset along the `z` dimension**

.. code-block:: python

    dc.load(product='gedi_l2b_cover_z', dask_chunks={'z': 15}, **query)

.. code-block::

    <xarray.Dataset>
    Dimensions:      (latitude: 720, longitude: 721, time: 2, z: 30)
    Coordinates:
    * time         (time) datetime64[ns] 2019-08-16T09:28:51 2019-10-21T15:54:01
    * latitude     (latitude) float64 -35.45 -35.45 -35.45 ... -35.25 -35.25
    * longitude    (longitude) float64 149.2 149.2 149.2 ... 149.0 149.0 149.0
        spatial_ref  int32 4326
    * z            (z) float64 5.0 10.0 15.0 20.0 25.0 ... 135.0 140.0 145.0 150.0
    Data variables:
        cover_z      (time, z, latitude, longitude) float32 dask.array<chunksize=(1, 15, 720, 721), meta=np.ndarray>
    Attributes:
        crs:           EPSG:4326
        grid_mapping:  spatial_ref
