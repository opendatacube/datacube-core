import logging
from contextlib import contextmanager

from click.testing import CliRunner


@contextmanager
def alter_log_level(logger, level=logging.WARN):
    previous_level = logger.getEffectiveLevel()
    logger.setLevel(level)
    yield
    logger.setLevel(previous_level)


def assert_click_command(command, args):
    result = CliRunner().invoke(
        command,
        args=args,
        catch_exceptions=False
    )
    print(result.output)
    assert not result.exception
    assert result.exit_code == 0
