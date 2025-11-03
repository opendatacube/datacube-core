This is the Alembic environment for the ODC postgis environment.

Example of how to run alembic to generate an upgrade from inside the Docker container (after updating the schema)
`uv run alembic -c datacube/drivers/postgis/alembic.ini -x env=postgis revision --autogenerate -m "Commit message"`

(skip `--autogenerate` if you want a manual migration file)

Upgrade to latest, or downgrade to previous
`alembic upgrade head`
`alembic downgrade -1`

Current state of database:
`alembic current`

List all migrations
`alembic history`
