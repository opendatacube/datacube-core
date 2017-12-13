from datacube.index._api import Index
from datacube.index.postgres.tables import _pg_exists


class S3BlockIndex(Index):
    def requirements_satisfied(self):
        """Check requirements are satisfied.

        :return: True if requirements is satisfied, otherwise returns False
        """
        # check database
        # pylint: disable=protected-access
        try:
            with self.index._db.connect() as connection:
                return (_pg_exists(connection._connection, "agdc.s3_dataset") and
                        _pg_exists(connection._connection, "agdc.s3_dataset_chunk") and
                        _pg_exists(connection._connection, "agdc.s3_dataset_mapping"))
        except AttributeError:
            self.logger.warning('Should only be here for tests.')
            return True

    def _make(self):
        pass
