.. _dev_arch_storage:

Data Loading
************

Types of Data Loading
=====================

There are two major use-cases for loading data from the Datacube
*Ad hoc access*, and *Large scale processing*. These are described below.

1. Ad hoc access

-  A small spatial region and time segment are chosen by the user
-  Data is expected to fit into RAM

2. Large scale processing (:class:`~datacube.api.GridWorkflow`)

-  Continental scale processing
-  Used to compute new products or to perform statistics on existing data
-  Often unconstrained spatially
-  Often unconstrained along time dimension
-  Data is accessed using regular grid in *small enough* chunks
-  The specific access pattern is algorithm/compute environment dependent
   and is supplied by the user and requires manual tuning

Ad hoc data access
------------------

Typically a small spatial region and time range are chosen by the user,
and all of the returned Data is expected to fit into RAM.

One database query maps to one :class:`xarray.Dataset` which is
processed/displayed/analyzed by custom user code. This happens as a two
step process:

1. Build a Virtual Storage Resource (VSR) – a description of what data
   needs to be loaded, possibly from many disparate sources
2. Load data from the VSR into a contiguous memory representation
   :class:`xarray.Dataset`

Building the VSR involves querying the database to find all possible *storage
units* that might contribute to the :abbr:`ROI (region of interest, x,y,t)` and
performing various post processing on the result:

-  Possibly pruning results somewhat based on *valid data* geo-polygons,
   or any other metric
-  Grouping result by time (i.e. assembling several ``VSR2D`` into ``VSR3D``)

**Code refs:** :meth:`~datacube.Datacube.find_datasets`, :meth:`~datacube.Datacube.group_datasets`,
:class:`datacube.model.Dataset`

Once a VSR is built it can be loaded into memory either as a whole or small
portions at a time.

**Code refs:** :meth:`~datacube.Datacube.load_data`, :class:`~datacube.model.GeoBox`

Large scale processing data access
----------------------------------

Just like in the ad hoc scenario described above there are two steps.
One involves querying the database in order to build a VSR, and the
second step is loading data. The difference is in the way the VSR is built.
Rather than constructing one giant VSR covering the entire collection a
large number of VSRs are constructed each covering a non-overlapping
region (one of the cells on a grid). Rather than querying the database once
for each grid cell, a single query is performed and the result is
then binned according to the :term:`grid spec`.

Data loading happens in exactly the same way as in the ad hoc approach, except
it usually happens in parallel across multiple processing nodes.

Data Structures
===============

Virtual Storage Resource
------------------------

-  *Virtual* as opposite of Real/Physical, meaning constructed on the fly
   as opposed to read from database or file. Logical is another name
   often used for this kind of thing
-  *Storage* as in just container of data, no possibility for compute
   beyond maybe projection changes, not specific to raster data
-  *Resource* as in U\ **R**\ I, U\ **R**\ L, possibly file on some
   local/network file system, but could be S3, HTTP, FTP, OPeNDAP, etc.

Provides a unified view of a collection of disparate storage resources.

At the moment there is no actual *Virtual Storage Resource* class
instead we use

-  VSR3D is an :class:`xarray.Dataset` that has a time dimension and contains
   a VSR2D for every timestamp
-  VSR2D is a list of :class:`datacube.model.Dataset`
-  :class:`datacube.model.Dataset` aggregates multiple bands into one storage
   resource. It is stored in the database and is used for provenance tracking.

All the information about individual *storage units* is captured in the
:class:`datacube.model.Dataset`, it includes:

-  Mapping from band names to underlying files/URIs
-  Geo-spatial info: CRS, extent
-  Time range covered by the observation
-  Complete metadata document (excluding lineage data)

It’s important to note that :class:`datacube.model.Dataset` describes
observations for one timeslice only.

    **TODO**: describe issues with timestamps, each pixel has it’s own
    actual capture time, which we do not store or track, but it does
    mean that single time slice is not just a point in time, but rather
    an interval)

The relationship between :class:`datacube.model.Dataset` and *storage units* is
complex, it’s not one to one, nor is one to many. Common scenarios are
listed below

1. :class:`datacube.model.Dataset` refers to several GeoTiff files, one for
   each band. Each GeoTiff file is referenced by exactly one dataset.
2. :class:`datacube.model.Dataset` refers to one netCDF4 file containing
   single timeslice, all bands are stored in that one file. NetCDF4 file
   is referenced by one dataset.
3. :class:`datacube.model.Dataset` refers to one time slice within a
   *stacked* netCDF4 file. This same netCDF4 file is referenced by a
   large number of datasets, each referring to a single time slice
   within the file.

It is assumed that individual storage units within a
:class:`datacube.model.Dataset` are of the same format. In fact storage
format is usually shared by all datasets belonging to the same :ref:`Product`,
although it is possible to index different formats under one product.

Data load in detail
===================

.. math::

  \text{VSR}, \text{GeoBox}, [\text{bands of interest}, \text{ opts}] \rightarrow \text{pixel data}

  
Once you have VSR constructed you can load all or part of it into memory
using :meth:`~datacube.Datacube.load_data`. At this point users can customise which bands they
want, how to deal with overlapping data, and other options like a per band
re-sampling strategy can also be supplied.

Internal interfaces
-------------------

The primary internal interface for loading data from storage is
:class:`datacube.storage.storage.BandDataSource`, unfortunately this rather generic name is taken by the
specific implementation based on the `rasterio`_ library.
:class:`datacube.storage.storage.BandDataSource` is responsible for describing data stored for a given
band, one can query:

-  The Shape (in pixels) and data type
-  Geospatial information: CRS + Affine transform

and also provides access to pixel data via 2 methods

-  :meth:`~datacube.storage.storage.BandDataSource.read`: access a section of source data in native projection but
   possibly in different resolution
-  :meth:`~datacube.storage.storage.BandDataSource.reproject`: access a section of source data, re-projecting to
   an arbitrary projection/resolution

This interface follows very closely the interface provided by the `rasterio`_
library. Conflating the reading and transformation of pixel data into one
function is motivated by the need for efficient data access. Some file
formats support multi-resolution storage for example, so it is more
efficient to read data at the appropriate scale rather than reading
highest resolution version followed by down sampling. Similarly
re-projection can be more memory efficient if source data is loaded in
smaller chunks interleaved with raster warping execution compared to a
conceptually simpler but less efficient *load all then warp all*
approach.

**Code refs:** :meth:`~datacube.Datacube.load_data`, :class:`~datacube.model.GeoBox`, :class:`~datacube.storage.storage.BandDataSource`,
:class:`~datacube.storage.storage.RasterDatasetDataSource`

Fuse function customisation
===========================

A VSR2D might consist of multiple overlapping pixel planes. This is
either due to duplicated data (e.g. consecutive Landsat scenes include a north/south
overlap, and all derived products keep those duplicates) or due to
grouping using a larger time period (e.g. one month). Whatever the reason,
the overlap needs to be resolved when loading data since the user expects a
single plane of pixels.

The strategy for dealing with overlapping data can be supplied by the
user at the load time. The default strategy is to simply pick the first
observed valid pixel value, where any pixel that is different from the
``nodata`` value is considered valid. In situations where pixel validity
is defined by a more complex metric, one can supply a custom ``fuse``
function. Fuse function takes two pixel planes (:class:`numpy.ndarray`) of
the same shape and data type, the first contains *fused result so far*,
and the second one is the *new data*. The ``fuse`` function is expected to
update *fused result so far* with the *new data* in place.

Below is a pseudo-code of the load code that uses a ``fuse`` function
(:func:`~datacube.storage.storage.reproject_and_fuse` is the actual implementation).

.. code:: python

    dst = ndarray_filled_with_nodata_values()

    for ds in datasets_for_this_timeslot:
       new_data = get_the_data_in_the_right_projection(ds)
       # tmp and dst have the same shape and dtype
       fuse(dst, new_data) ## << update dst in place

**Code refs:** :func:`~datacube.storage.storage.reproject_and_fuse`, :func:`~datacube.api.core._fuse_measurement`,
:meth:`~datacube.Datacube.load_data`

Problems with the current approach to fusing
--------------------------------------------

One major limitation is that the ``fuse`` function is customised per
product, but should really be customised per band. It is completely
reasonable for different bands of the same product to be sufficiently
different as to require a different fusing strategy. And since a ``fuse``
function doesn’t know which band it is processing it can not dispatch to
different implementations internally.

The types of computation a ``fuse`` function can perform is limited by the
interface, for example one can not implement *average* nor *median*. With
some modification it should be possible to support arbitrary incremental
computations, like *average*, without loading all the data at once.

Lazy load with dask
===================

In computer science context *lazy* means roughly *not computed until
needed*. Rather then loading all the data immediately :meth:`~datacube.Datacube.load_data`
function can instead construct an :class:`xarray.Dataset` that the user can use
in the same way as a fully *loaded* data set, except that pixel data will be
fetched from disk/network on demand as needed. The on-demand loading
functionality is provided by third party libraries `xarray`_ and
`dask`_\ (used internally by `xarray`_). Datacube code constructs
a *recipe* for loading data on demand, this recipe is executed as needed
by ``xarray``/``dask`` library when real data is required to be loaded for the first
time.

.. note::
   **TODO**

   - Discuss chunks and how they relate to on-disk storage chunks
   - Discuss memory management, how data is unloaded from RAM,
     avoiding out of memory errors when processing large arrays.
   - We need to provide a clear guidance as to when this mode should be used
     and how

Limitations and problems
========================

One of the original goals of Datacube is to support a wide variety of
different input data sources, as such flexibility has been preferred to
efficiency. When designing an API one would strive for simplicity,
generality and efficiency. An "Ideal API" would have all three turned up to
the max, but often it is necessary to balance one at the expense of the
other. Efficiency in particular often has significant complexity costs,
it is also harder to achieve when striving to be as generic as possible.

Internal interfaces for reading data is per time slice per band.
Description of a storage unit for a given band for a given time slice
(:class:`datacube.model.Dataset`) is passed from the database to storage
specific loading code one by one, and the results are assembled into a
3D structure by generic loading code.

On a plus side this maps nicely to the way things work in
``gdal/rasterio`` land and is the most generic representation that
allows for greatest variety of storage regimes

-  bands/time slices split across multiple files
-  bands stored in one files, one file per time slice
-  stacked files that store multiple time slices and all the bands

On the other hand this way of partitioning code leads to less than
optimal I/O access patterns. This is particularly noticeable when using
“stacked files” (a common use case on the NCI installation of the
datacube) while doing “pixel drill” type of access.

Problems are:

-  Same netCDF file is opened/closed multiple times – no netCDF chunk
   cache sharing between reads
-  Larger more complex (many bands) files might have slightly larger
   “open overhead” to begin with, not a problem if you share the same
   file handle to load all the data of interest, but adds to a
   significant cost when you re-open the same file many times
   needlessly.
-  File open overhead increases as we move towards cloud storage
   solutions like Amazon S3.
-  Chunking along time dimension makes depth reads even more costly when
   using this access pattern since data is read and decompressed just to
   be thrown away (in the case of NCI install, chunking along time
   dimension is 5 time slices per chunk, so 80% of decoded data is
   thrown away due to access pattern, since we only read one time slice
   at a time).

Possible Solutions
------------------

One possible solution is to keep internal interfaces as they are and
introduce global IO cache to allow sharing of opened files/partially
loaded data. This adds quite a bit of complexity, particularly around
memory management: can’t just keep adding data to the cache, need to
purge some data eventually, meaning that depending on the use pattern
efficiency improvements aren’t guaranteed. Global state that such a
solution will need to rely on is problematic in the multi-threaded
environment and often leads to hard to debug errors even in a single
threaded application. Global state makes testing harder too.

As such we believe that a more practical approach is to modify internal
IO interfaces to support efficient reads from stacked multi-band
storage. To do that we need to move internal interface boundary up to
VSR3D level, VSR in :class:`xarray.Dataset` out.

We propose roughly the following interface

1. ``open :: VSR, [output CRS, output scale, opts] -> VSRDataSource``
2. ``read :: VSRDataSource, [GeoBox, bands of interest, time of interest, opts] -> xarray.Dataset``

A two step process, first construct pixel data source supplying ahead of
time output projection and scale (optional, defaulting to native
projection and resolution when possible), then read sections of data as
needed, user can choose what spatio-temporal region they want to access
and select a subset of bands they need to read into memory. Note that
read might perform re-projection under the hood, based on whether output
projection/resolution was supplied and whether it differs from native.


Storage Drivers
===============

GDAL
----
The GDAL-based driver uses `rasterio`_ to read a single time slice of a single
variable/measurement at a time, in a synchronous manner.


S3IO
----
This driver provides access to chunked array storage on Amazon S3.


Supporting Diagrams
===================

Data Read Process
-----------------

.. uml:: /diagrams/current_data_read_process.plantuml
   :caption: Current Data Read Process



Storage Classes
---------------

.. uml:: /diagrams/storage_drivers_old.plantuml
   :caption: Classes currently implementing the DataCube Data Read Functionality



.. _rasterio: https://rasterio.readthedocs.io/en/latest/
.. _xarray: https://xarray.pydata.org/
.. _dask: https://dask.pydata.org/
