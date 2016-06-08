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

    InstallPackage [href="install.html", label="Install Data Cube Package and
     Dependencies"];
    CreateDB [href="db_setup.html", label="Create a Database to hold the
    Index"];
    InitialiseDatabase [href="db_setup.html#initialise-the-database-schema", label="Initialise Database"];
    IndexData [href="indexing.html", label="Index Datasets"];
    WriteIngest [href="ingest.html#ingestion-configuration", label="Write
     ingest config"];
    RunIngest [href="ingest.html#ingest-some-data", label="Run ingestion"];
    Finished [label="Finished\nReady to analyse data"];
    LoadProductTypes [href="indexing.html#product-definitions", label="Define Product Types"];
    WritePrepScript [href="prepare_scripts.html", label="Write Prepare Script"];
    RunPrepScript [href="prepare_scripts.html", label="Run Prepare Script"];
