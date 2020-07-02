import pytest

from datacube import Datacube
from datacube.config import LocalConfig


def test_multiple_environment_config(tmpdir):
    config_path = tmpdir.join('second.conf')

    config_path.write("""
[DEFAULT]
db_username: test_user
index_driver: default

[default]
db_hostname: db.opendatacube.test

[test_alt]
db_hostname: alt-db.opendatacube.test
    """)

    config_path = str(config_path)

    config = LocalConfig.find([config_path])
    assert config['db_hostname'] == 'db.opendatacube.test'
    alt_config = LocalConfig.find([config_path], env='test_alt')
    assert alt_config['db_hostname'] == 'alt-db.opendatacube.test'

    # Make sure the correct config is passed through the API
    # Parsed config:
    db_url = 'postgresql://{user}@db.opendatacube.test:5432/datacube'.format(user=config['db_username'])
    alt_db_url = 'postgresql://{user}@alt-db.opendatacube.test:5432/datacube'.format(user=config['db_username'])

    with Datacube(config=config, validate_connection=False) as dc:
        assert str(dc.index.url) == db_url

    # When none specified, default environment is loaded
    with Datacube(config=str(config_path), validate_connection=False) as dc:
        assert str(dc.index.url) == db_url
    # When specific environment is loaded
    with Datacube(config=config_path, env='test_alt', validate_connection=False) as dc:
        assert str(dc.index.url) == alt_db_url

    # An environment that isn't in any config files
    with pytest.raises(ValueError):
        with Datacube(config=config_path, env='undefined-env', validate_connection=False) as dc:
            pass


def test_wrong_env_error_message(clirunner_raw, monkeypatch):
    from datacube import config
    monkeypatch.setattr(config, 'DEFAULT_CONF_PATHS', ('/no/such/path-264619',))

    result = clirunner_raw(['-E', 'nosuch-env', 'system', 'check'],
                           expect_success=False)
    assert "No datacube config found for 'nosuch-env'" in result.output
    assert result.exit_code != 0

    result = clirunner_raw(['system', 'check'],
                           expect_success=False)
    assert "No datacube config found" in result.output
    assert result.exit_code != 0
