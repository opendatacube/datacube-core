
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

.. uml:: /diagrams/current_data_read_process.plantuml
   :caption: Current Data Read Process


Data Load Classes
-----------------

.. uml:: /diagrams/storage_drivers_old.plantuml
   :caption: Classes currently implementing the DataCube Data Read Functionality

How the Index Works
-------------------

.. uml:: /diagrams/index_sequence.plantuml
   :caption: Sequence of steps when creating an index
