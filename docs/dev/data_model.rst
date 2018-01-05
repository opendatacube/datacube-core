
Data Model
==========


.. _product:

Product
-------

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


Reading Data
------------

.. figure:: /diagrams/current_data_read_process.svg

   Current Data Read Process


Data Load Classes
-----------------

.. figure:: /diagrams/storage_drivers_old.svg

   Classes currently implementing the DataCube Data Read Functionality

How the Index Works
-------------------

.. figure:: /diagrams/index_sequence.svg

   Sequence of steps when creating an index
