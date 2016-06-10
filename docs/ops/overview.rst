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

    InstallPackage [href="install.html", target="_top", label="Install Data Cube Package and
     Dependencies"];
    CreateDB [href="db_setup.html", target="_top", label="Create a Database to hold the
    Index"];
    InitialiseDatabase [href="db_setup.html#initialise-the-database-schema", target="_top", label="Initialise Database"];
    IndexData [href="indexing.html", target="_top", label="Index Datasets"];
    WriteIngest [href="ingest.html#ingestion-configuration", target="_top", label="Write
     ingest config"];
    RunIngest [href="ingest.html#ingest-some-data", target="_top", label="Run ingestion"];
    Finished [label="Finished\nReady to analyse data"];
    LoadProductTypes [href="indexing.html#product-definitions", target="_top", label="Define Product Types"];
    WritePrepScript [href="prepare_scripts.html", target="_top", label="Write Prepare Script"];
    RunPrepScript [href="prepare_scripts.html", target="_top", label="Run Prepare Script"];
