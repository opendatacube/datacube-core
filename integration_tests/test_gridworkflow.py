from datacube.api.grid_workflow import GridWorkflow


# TODO: do we still need this now that driver manager is gone?
def test_create_gridworkflow_with_logging(index):
    from logging import getLogger, StreamHandler

    logger = getLogger(__name__)
    handler = StreamHandler()
    logger.addHandler(handler)

    try:
        gw = GridWorkflow(index)
    finally:
        logger.removeHandler(handler)
