Tools
=====

.. _datacube-config-tool:

datacube-config
---------------

.. code-block:: text

    Usage: datacube-config [OPTIONS] COMMAND [ARGS]...

    Configure the Data Cube

    Options:
      --version
      -v, --verbose      Use multiple times for more verbosity
      -C, --config_file TEXT
      --log-queries      Print database queries.
      -h, --help         Show this message and exit.

    Commands:
      check        Verify & view current configuration.
      collections  Dataset collections
      database     Initialise the database
      storage      Storage types

.. _datacube-ingest-tool:

datacube-ingest
---------------

.. code-block:: text

    Usage: datacube-ingest [OPTIONS] COMMAND [DATASETS]...

        Data Management Tool

    Options:
      --version
      -v, --verbose           Use multiple times for more verbosity
      -C, --config_file TEXT
      --log-queries           Print database queries.
      -h, --help              Show this message and exit.

    Commands:
      ingest Ingest datasets into the Data Cube.
      stack  Stack storage units
.. _datacube-search-tool:

datacube-search
---------------

.. code-block:: text

    Usage: datacube-search [OPTIONS] COMMAND [ARGS]...

      Search the Data Cube

    Options:
      --version
      -v, --verbose           Use multiple times for more verbosity
      -C, --config_file TEXT
      --log-queries           Print database queries.
      -f [csv|pretty]         Output format  [default: pretty]
      -h, --help              Show this message and exit.

    Commands:
      datasets  Datasets
      units     Storage units
