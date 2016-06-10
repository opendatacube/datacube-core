
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

