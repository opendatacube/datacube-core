# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from threading import Lock
from typing import Any

from typing_extensions import override

from datacube.index.exceptions import TransactionException
from datacube.utils.generic import thread_local_cache


class AbstractTransaction(ABC):
    """
    Abstract base class for a Transaction Manager.  All index implementations should extend this base class.

    Thread-local storage and locks ensures one active transaction per index per thread.
    """

    def __init__(self, index_id: str) -> None:
        self._connection: Any = None
        self._tls_id = f"txn-{index_id}"
        self._obj_lock = Lock()
        self._controlling_trans = None

    # Main Transaction API
    def begin(self) -> None:
        """
        Start a new transaction.

        Raises an error if a transaction is already active for this thread.

        Calls implementation-specific _new_connection() method and manages thread local storage and locks.
        """
        with self._obj_lock:
            if self._connection is not None:
                raise ValueError(
                    "Cannot start a new transaction as one is already active"
                )
            self._tls_stash()

    def commit(self) -> None:
        """
        Commit the transaction.

        Raises an error if transaction is not active.

        Calls implementation-specific _commit() method, and manages thread local storage and locks.
        """
        with self._obj_lock:
            if self._connection is None:
                raise ValueError("Cannot commit inactive transaction")
            self._commit()
            self._release_connection()
            self._connection = None
            self._tls_purge()

    def rollback(self) -> None:
        """
        Rollback the transaction.

        Raises an error if transaction is not active.

        Calls implementation-specific _rollback() method, and manages thread local storage and locks.
        """
        with self._obj_lock:
            if self._connection is None:
                raise ValueError("Cannot rollback inactive transaction")
            self._rollback()
            self._release_connection()
            self._connection = None
            self._tls_purge()

    @property
    def active(self) -> bool:
        """
        :return:  True if the transaction is active.
        """
        return self._connection is not None

    # Manage thread-local storage
    def _tls_stash(self) -> None:
        """
        Check TLS is empty, create a new connection and stash it.
        :return:
        """
        stored_val = thread_local_cache(self._tls_id)
        if stored_val is not None:
            # stored_val is outermost transaction in a stack of nested transaction.
            self._controlling_trans = stored_val
            self._connection = stored_val._connection
        else:
            self._connection = self._new_connection()
            thread_local_cache(self._tls_id, purge=True)
            thread_local_cache(self._tls_id, self)

    def _tls_purge(self) -> None:
        thread_local_cache(self._tls_id, purge=True)

    # Commit/Rollback exceptions for Context Manager usage patterns
    def commit_exception(self, errmsg: str) -> TransactionException:
        return TransactionException(errmsg, commit=True)

    def rollback_exception(self, errmsg: str) -> TransactionException:
        return TransactionException(errmsg, commit=False)

    # Context Manager Interface
    def __enter__(self) -> "AbstractTransaction":
        self.begin()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        if not self.active:
            # User has already manually committed or rolled back.
            return True
        if exc_type is not None and issubclass(exc_type, TransactionException):
            # User raised a TransactionException,
            if self._controlling_trans:
                # Nested transaction - reraise TransactionException
                return False
            # Commit or rollback as per exception
            if exc_value.commit:
                self.commit()
            else:
                self.rollback()
            # Tell runtime exception is caught and handled.
            return True
        elif exc_value is not None:
            # Any other exception - reraise.  Rollback if outermost transaction
            if not self._controlling_trans:
                self.rollback()
            # Instruct runtime to rethrow exception
            return False
        else:
            # Exited without exception.  Commit if outermost transaction
            if not self._controlling_trans:
                self.commit()
            return True

    # Internal abstract methods for implementation-specific functionality
    @abstractmethod
    def _new_connection(self) -> Any:
        """
        :return: a new index driver object representing a database connection or equivalent against which transactions
        will be executed.
        """

    @abstractmethod
    def _commit(self) -> None:
        """
        Commit the transaction.
        """

    @abstractmethod
    def _rollback(self) -> None:
        """
        Rollback the transaction.
        """

    @abstractmethod
    def _release_connection(self) -> None:
        """
        Release the connection object stored in self._connection
        """


class UnhandledTransaction(AbstractTransaction):
    # Minimal implementation for index drivers with no transaction handling.
    @override
    def _new_connection(self) -> Any:
        return True

    @override
    def _commit(self) -> None:
        pass

    @override
    def _rollback(self) -> None:
        pass

    @override
    def _release_connection(self) -> None:
        pass
