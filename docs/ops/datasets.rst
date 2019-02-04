
Types of Datasets
=================

When using the Data Cube, it will contain records about 2 different types of
products and datasets.

================= ========== ================================
 Type of dataset   In Index          Typical data
================= ========== ================================
 Indexed              Yes           Created externally
----------------- ---------- --------------------------------
 Ingested             Yes     Created within the Data Cube
================= ========== ================================


Indexed Datasets
~~~~~~~~~~~~~~~~

Data is available (has a file location or uri), with associated metadata
available in a format understood by the Data Cube.

Example:

- USGS Landsat Scenes with prepared ``agdc-metadata.yaml``
- GA Landsat Scenes

Ingested Datasets
~~~~~~~~~~~~~~~~~

Data has been created by/and is managed by the Data Cube. The data has typically been
been copied, compressed, tiled and possibly re-projected into a shape suitable
for analysis, and stored in NetCDF4 files.

Example:

- Tiled GA Landsat Data, ingested into Australian Albers Equal Area
  Projection (EPSG:3577) and stored in 100km tiles in NetCDF4


Provenance
~~~~~~~~~~

.. digraph:: provenance

    node[fontname="Helvetica"] //, colorscheme=greens3, color=1];

    raw[label="RAW Telemetry", style=dashed]
    ortho[label="Ortho Rectified", style=dashed]
    nbar[style=solid, label="NBAR Scene", styled=solid]
    nbartile[label="NBAR Albers Tile", shape=box, style=filled]

    raw -> ortho -> nbar -> nbartile;
    //rankdir=LR;
