from datacube.api.grid_workflow import GridWorkflow


def test_create_gridworkflow_with_logging(index):
    from logging import getLogger, StreamHandler

    logger = getLogger(__name__)
    logger.addHandler(StreamHandler())

    gw = GridWorkflow(index)
