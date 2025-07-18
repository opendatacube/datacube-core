This is the Alembic environment for the ODC postgis environment.

Example of how to run alembic to generate an upgrade from inside the Docker container:
`uv run alembic -c datacube/drivers/postgis/alembic.ini -x env=postgis revision --autogenerate -m "Commit message"`
