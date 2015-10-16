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


## Python Usage

Tile and stack files with:

    from ingestor import datacube_ingestor
    
    input_path = "/g/data/rs0/scenes/ARG25_V0.0/2015-08/LS8_OLI_TIRS_NBAR_P54_GANBAR01-032_089_081_20150807/"
    output_dir = "/tmp/ingest_test/"
    filename_format = "combined_{x}_{y}.nc"
    
    datacube_ingestor.ingest(input_path, output_dir, filename_format)