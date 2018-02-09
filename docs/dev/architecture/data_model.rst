
Data Model
==========

Dataset
-------

.. pull-quote::
   “The smallest aggregation of data independently described, inventoried, and managed.”​

(Definition of “Granule” from NASA EarthData Unified Metadata Model)​

Examples of ODC Datasets:​
* a Landsat Scene​
* an Albers Equal Area tile portion of a Landsat Scene​

.. _product:

Product
-------
Products are collections of `datasets` that share the same set of measurements and some subset of metadata.

.. digraph:: product

    graph [rankdir=TB];
    node [shape=record,style=filled,fillcolor=gray95];
    edge [dir=back, arrowhead=normal];

    Product -> Measurements [arrowhead=diamond,style=dashed,label="conceptual "];
    GridSpec -> CRS;
    Dataset -> Measurements;
    Product -> Dataset [arrowhead=diamond];
    Product -> GridSpec [label="optional\nshould exist for managed products",
    style=dashed];

    Dataset -> CRS;

    Dataset[label = "{Dataset|+ dataset_type\l+ local_path\l+ bounds\l+ crs\l+ measurements\l+ time\l...|...}"];


    Product [label="{Product/DatasetType|+ name\l+ managed\l+ grid_spec
     (optional)\l+ dimensions\l...|...}"];


Metadata Types
--------------
Metadata Types define custom index search fields across products.
The default `eo` metadata type defines fields such as 'platform', 'instrument' and the spatial bounds.




How the Index Works
-------------------

.. uml:: /diagrams/index_sequence.plantuml
   :caption: Sequence of steps when creating an index
