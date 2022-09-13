# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0


from contextlib import contextmanager
from typing import Any

from datacube.drivers.postgres import PostgresDb
from datacube.drivers.postgres._api import PostgresDbAPI
from datacube.index.abstract import AbstractTransaction


class PostgresTransaction(AbstractTransaction):
    def __init__(self, db: PostgresDb, idx_id: str) -> None:
        super().__init__(idx_id)
        self._db = db

    def _new_connection(self) -> Any:
        return self._db.begin()

    def _commit(self) -> None:
        self._connection.commit()

    def _rollback(self) -> None:
        self._connection.rollback()

    def _release_connection(self) -> None:
        self._connection.close()


class IndexResourceAddIn:
    @contextmanager
    def db_connection(self, transaction=False) -> PostgresDbAPI:
        """
        Context manager representing a database connection.

        If there is an active transaction for this index in the current thread, the connection object from that
        transaction is returned, with the active transaction remaining in control of commit and rollback.

        If there is no active transaction and the transaction argument is True, a new transactionised connection
        is returned, with this context manager handling commit and rollback.

        If there is no active transaction and the transaction argument is False (the default), a new connection
        is returned with autocommit semantics.

        Note that autocommit behaviour is NOT available if there is an active transaction for the index
        and the active thread.

        In Resource Manager code replace self._db.connect() with self.db_connection(), and replace
        self._db.begin() with self.db_connection(transaction=True).

        :param transaction: Use a transaction if one is not already active for the thread.
        :return: A PostgresDbAPI object, with the specified transaction semantics.
        """
        trans = self._index.thread_transaction()
        if trans is not None:
            # Use active transaction
            yield trans._connection
        elif transaction:
            with self._db.begin() as conn:
                yield conn
        else:
            # Autocommit behaviour:
            with self._db.connect() as conn:
                yield conn

