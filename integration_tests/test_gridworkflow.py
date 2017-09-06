from datacube.api.grid_workflow import GridWorkflow


# The preliminary implementation of `datacube.drivers.manager.DriverManager`
# had a member logger instance that prevented it from being pickled.
# Creating a `GridWorkflow` instance also failed in that case, breaking
# the `datacube-stats` application, among other things.
def test_create_gridworkflow_with_logging(index):
    from logging import getLogger, StreamHandler

    logger = getLogger(__name__)
    handler = StreamHandler()
    logger.addHandler(handler)

    try:
        gw = GridWorkflow(index)
    finally:
        logger.removeHandler(handler)
