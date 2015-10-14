# datacube-experiments
File format, ingestion and performance experiments relating to Australian Geoscience Datacube development.

## Test Ingestion Script

    $ datacube_ingest --help
    Usage: datacube_ingest [OPTIONS] INPUT_PATH FILENAME_FORMAT
    
      Example output filename format: combined_{x}_{y}.nc
    
    Options:
      -o, --output-dir TEXT
      --multi-variable
      --single-variable
      --tile / --no-tile     Allow partial processing
      --merge / --no-merge   Allow partial processing
      -v, --verbose          Use multiple times for more verbosity
      --help                 Show this message and exit.



