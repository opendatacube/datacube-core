Operations Guide
================

This section contains information on setting up and running a Data Cube.
Generally, users will not be required to know most of the information contained
here.

.. toctree::
   :maxdepth: 2
   :caption: Operations Guide

   install
   db_setup
   indexing
   ingest
   config
   prepare_scripts
   tools
   replication

.. toctree::
   :hidden:

   conda_base


Overview
--------

Follow the steps below to install and configure a new Data Cube instance.

.. digraph:: setup

    node [shape=box,style=filled,fillcolor=gray95]
    subgraph cluster0 {
        edge [style=dashed];
        color = grey;
        style = dashed;
        fontsize = 12;
        label = "Optional data ingestion\n(Performance optimisation)";

        WriteIngest -> RunIngest;

    }
    subgraph cluster1 {
        edge [style=dashed];
        color = grey;
        style = dashed;
        fontsize = 12;
        label = "Optional dataset preparation\nrequired for 3rd party datasets";
        WritePrepScript -> RunPrepScript;
    }

    InstallPackage -> CreateDB -> InitialiseDatabase -> LoadProductTypes;

    LoadProductTypes -> IndexData [weight=10];

    IndexData -> Finished [weight=5];
    IndexData -> WriteIngest [style=dashed];
    RunIngest -> Finished  [style=dashed];


    RunPrepScript -> IndexData [style=dashed];

    InstallPackage [href="../ops/install.html", target="_top", label="Install Data Cube Package and
     Dependencies"];
    CreateDB [href="../ops/db_setup.html", target="_top", label="Create a Database to hold the
    Index"];
    InitialiseDatabase [href="../ops/db_setup.html#initialise-the-database-schema", target="_top", label="Initialise Database"];
    IndexData [href="../ops/indexing.html", target="_top", label="Index Datasets"];
    WriteIngest [href="../ops/ingest.html#ingestion-configuration", target="_top", label="Write
     ingest config"];
    RunIngest [href="../ops/ingest.html#ingest-some-data", target="_top", label="Run ingestion"];
    Finished [label="Finished\nReady to analyse data"];
    LoadProductTypes [href="../ops/indexing.html#product-definitions", target="_top", label="Define Product Types"];
    WritePrepScript [href="../ops/prepare_scripts.html", target="_top", label="Write Prepare Script"];
    RunPrepScript [href="../ops/prepare_scripts.html", target="_top", label="Run Prepare Script"];
