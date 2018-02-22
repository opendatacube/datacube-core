# Virtual Products

There are two major use-cases for loading data from the datacube

1. Ad hoc access
  - A small spatial region and time segment are chosen by the user
  - Data is expected to fit into RAM

2. Large scale processing (`GridWorkflow`)
  - Continental scale processing
  - Used to compute new products or to perform statistics on existing data
  - Often unconstrained spatially
  - Often unconstrained along time dimension also
  - Data is accessed using regular grid in "small enough" chunks
  - Specific access pattern is algorithm/compute environment dependent and is
    supplied by the user and requires manual tuning


## Ad hoc data access

One DB query maps to one `xarray.Dataset` that gets processed/displayed/analyzed by custom user code. 
Essentially this a two step process

1. Build Virtual Storage Resource (VSR) -- a description of what data needs to be loaded, 
possibly from many disparate sources
2. Load data from VSR into a contiguous memory representation `xarray.Dataset`

Building VSR involves querying database to find all possible storage units that
might contribute to the ROI (region of interest, x,y,t) and performing various
post processing on the result:

- Possibly pruning results somewhat based on "valid data" geo-polygons, or any other metric
- Grouping result by time (i.e. assembling several VSR<sup>2D</sup> into VSR<sup>3D</sup>  )

**Code refs:** `find_datasets`, `group_datasets`, `datacube.model.Dataset`

Once VSR is built it can be loaded into memory either as a whole or one portion
at a time.

**Code refs:** `load_data`, `GeoBox`


## Large scale processing data access

Just like in the ad hoc scenario described above there are two steps. One
involves querying the database in order to build a VSR, and the second step is
loading data. The difference is in the way VSR is built. Rather than
constructing one giant VSR covering the entire collection a large number of VSRs
are constructed each covering a non-overlapping region (one of the cells on a
grid). Rather than querying database many times for each grid cell, a single
query is performed and the result is then binned according to the grid spec.

Data loading happens in exactly the same way as ad hoc approach, except it
usually happens in parallel across multiple processing nodes.

## VSR Data Structures

Virtual Storage Resource

- Virtual as opposite of Real/Physical, meaning constructed on the fly as
  opposed to read from database or file. Logical is another name often used for
  that kind of thing
- Storage as in just container of data, no possibility for compute beyond maybe
  projection changes, not specific to raster data
- Resource as in U**R**I, U**R**L, possibly file on some local/network file system,
  but could be s3, http, ftp, opendap, etc.

Provides unified view of a collection of disparate storage resources.

At the moment there is no actual "Virtual Storage Resource" class instead we use

- VSR<sup>3D</sup> is an `xarray.Dataset` that has time dimension and contains VSR<sup>2D</sup> for every timestamp
- VSR<sup>2D</sup> is a list of `datacube.model.Dataset`
- `datacube.model.Dataset` aggregates multiple bands into one storage resource, is stored in the DB

All the information about individual storage units is captured in the `datacube.model.Dataset`, it includes:

- Mapping from band names to underlying files/URIs
- Geo-spatial info: CRS, extent
- Time range covered by the observation
- Complete metadata document (excluding lineage data)

It's important to note that `datacube.model.Dataset` describes observation for
one timeslice only.

> **TODO**: describe issues with timestamps, each pixel has it's own actual capture
> time, which we do not store or track, but it does mean that single time slice
> is not just a point in time, but rather an interval)

The relationship between `datacube.model.Dataset` and storage units is complex,
it's not one to one, nor is one to many. Common scenarios are listed below

1. `datacube.model.Dataset` refers to several GeoTiff files, one for each band.
   Each GeoTiff file is references by exactly one dataset.
2. `datacube.model.Dataset` refers to one netCDF4 file containing single
   timeslice, all bands are stored in that one file. NetCDF4 file is referenced
   by one dataset.
3. `datacube.model.Dataset` refers to one time slice within a "stacked" netCDF4
   file. This same netCDF4 file is referenced by a large number of datasets,
   each referring to a single time slice within the file.

It is assumed that individual storage units within `datacube.model.Dataset` are
of the same format. In fact storage format is usually shared by all datasets
belonging to the same product, although one can probably index different formats
under one product.

## Data load in detail

`VSR, GeoBox, [bands of interest, opts] -> pixel data`

Once you have VSR constructed you can load all or part of it into memory using
`load_data`. At this point user can customize what bands they want, how to deal
with overlapping data, and other options like per band re-sampling strategy can
also be supplied.

### Internal interfaces

The primary internal interface for loading data from storage is
`BandDataSource`, unfortunately this rather generic name is taken by specific
implementation based on `rasterio` library. `BandDataSource` is responsible for
describing data stored for a given band, one can query

- Shape (in pixels) and data type
- Geospatial information: CRS + Affine transform

and also provides access to pixel data via 2 methods

- `read` access section of source data in native projection but possibly in different resolution
- `reproject` access section of source data re-projecting to arbitrary projection/resolution

This interface follows very closely interface provided by `rasterio` library.
Conflating of reading and transformation of pixel data into one function is
motivated by the need for efficient data access. Some file formats support
multi-resolution storage for example, so it is more efficient to read data at
the appropriate scale rather than reading highest resolution version followed by
down sampling. Similarly re-projection can be more memory efficient if source
data is loaded in smaller chunks interleaved with raster warping execution
compared to a conceptually simpler but less efficient "load all then warp all"
approach.


**Code refs:** `load_data`, `GeoBox`, `BandDataSource`, `RasterDatasetDataSource`

### Fuse function customization

VSR<sup>2D</sup> might consists of multiple overlapping pixel planes. This is
either due to duplicated data (e.g. Landsat scenes include north/south overlap,
and all derived products keep those duplicates) or due to grouping using larger
time period (e.g. one month). Whatever the reason, overlap needs to be resolved
when loading data since user expects a single plane of pixels.

The strategy for dealing with overlapping data can be supplied by the user at
the load time. Default strategy is to simply pick the first observed valid pixel
value, where any pixel that is different from `nodata` value is considered
valid. In situations where pixel validity is defined by a more complex metric,
one can supply a custom `fuse` function. Fuse function takes two pixel planes
(`numpy.ndarray`) of the same shape and data type, first one contains "fused
result so far", and the second one is the "new data", `fuse` function is
expected to update "fused result so far" with the "new data" in place.

Below is a pseudo-code of the load code that uses `fuse` function
(`reproject_and_fuse` is where the real code is)

```python
dst = ndarray_filled_with_nodata_values()

for ds in datasets_for_this_timeslot:
   new_data = get_the_data_in_the_right_projection(ds)
   # tmp and dst have the same shape and dtype
   fuse(dst, new_data) ## << update dst in place
```

**Code refs:** `reproject_and_fuse`, `_fuse_measurement`, `_load_data`

#### Problems with the current approach to fusing

One major limitation is that `fuse` function is customized per product, but
should really be customized per band. It is completely reasonable for different
bands of the same product to be sufficiently different as to require different
fuse strategy. And since `fuse` function doesn't know which band it is
processing it can not dispatch to different implementations internally.

Types of computation `fuse` function can perform is limited by the interface,
for example one can not implement average nor median. With some modification it
should be possible to support arbitrary incremental computations, like average,
without loading all the data at once.


### Lazy load with dask

In computer science context "lazy" means roughly "not computed until needed".
Rather then loading all the data immediately `load_data` function can instead
construct an `xarray.Dataset` that user can use same way as fully loaded data
set, except that pixel data will be fetched from disk/network on demand as
needed. The on-demand loading functionality is provided by third party libraries
`xarray` and `dask`(used internally by `xarray`). Datacube code just constructs
a "recipe" for loading data on demand, this recipe is executed as needed by
`xarray`/`dask` library when user accesses data for the first time.

> **TODO:**
> - Discuss chunks and how they relate to on-disk storage chunks
> - Discuss memory management, how data is unloaded from RAM, avoiding out of
>   memory errors when processing large arrays.
> - We need to provide a clear guidance as to when this mode should be used and how


### Limitations and problems

One of the original goals of Datacube is to support a great variety of different
input data sources, as such flexibility is often preferred to efficiency. When
designing an API one would strive for simplicity, generality and efficiency.
Ideal API would have all three turned up to the max, but often it is necessary
to balance one at the expense of the other. Efficiency in particular often has
significant complexity costs, it is also harder to achieve when striving to be
as generic as possible.

Internal interfaces for reading data is per time slice per band. Description of
a storage unit for a given band for a given time slice
(`datacube.model.Dataset`) is passed from the database to storage specific
loading code one by one, and the results are assembled into a 3D structure by
generic loading code.

On a plus side this maps nicely to the way things work in `gdal/rasterio` land
and is the most generic representation that allows for greatest variety of
storage regimes

- bands/time slices split across multiple files
- bands stored in one files, one file per time slice
- stacked files that store multiple time slices and all the bands

On the other hand this way of partitioning code leads to less than optimal I/O
access patterns. This is particularly noticeable when using "stacked files" (a
common use case on the NCI installation of the datacube) while doing "pixel drill"
type of access.

Problems are:

- Same netCDF file is opened/closed multiple times -- no netCDF chunk cache
  sharing between reads
- Larger more complex (many bands) files might have slightly larger "open
  overhead" to begin with, not a problem if you share the same file handle to
  load all the data of interest, but adds to a significant cost when you re-open
  the same file many times needlessly.
- File open overhead increases as we move towards cloud storage solutions like
  Amazon S3.
- Chunking along time dimension makes depth reads even more costly when using
  this access pattern since data is read and decompressed just to be thrown away
  (in the case of NCI install, chunking along time dimension is 5 time slices
  per chunk, so 80% of decoded data is thrown away due to access pattern, since
  we only read one time slice at a time).


#### Possible Solutions

One possible solution is to keep internal interfaces as they are and introduce
global IO cache to allow sharing of opened files/partially loaded data. This
adds quite a bit of complexity, particularly around memory management: can't
just keep adding data to the cache, need to purge some data eventually, meaning
that depending on the use pattern efficiency improvements aren't guaranteed.
Global state that such a solution will need to rely on is problematic in the
multi-threaded environment and often leads to hard to debug errors even in a
single threaded application. Global state makes testing harder too.

As such we believe that a more practical approach is to modify internal IO
interfaces to support efficient reads from stacked multi-band storage. To do
that we need to move internal interface boundary up to VSR<sup>3D</sup> level,
VSR in `xarray.Dataset` out.

We propose roughly the following interface

1. `open :: VSR, [output CRS, output scale, opts] -> VSRDataSource`
2. `read :: VSRDataSource, [GeoBox, bands of interest, time of interest, opts] -> xarray.Dataset`

A two step process, first construct pixel data source supplying ahead of time
output projection and scale (optional, defaulting to native projection and
resolution when possible), then read sections of data as needed, user can choose
what spatio-temporal region they want to access and select a subset of bands
they need to read into memory. Note that read might perform re-projection under
the hood, based on whether output projection/resolution was supplied and whether
it differs from native.