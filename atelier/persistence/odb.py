#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /atelier/persistence/odb.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday March 2nd 2023 05:49:40 am                                                 #
# Modified   : Thursday March 2nd 2023 03:55:14 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
import os
import shelve
from typing import Any

from atelier.persistence.database import Database
from atelier.persistence.exceptions import (
    ObjectExistsError,
    ObjectNotFoundError,
    ObjectDatabaseConnectionError,
)


# ------------------------------------------------------------------------------------------------ #
#                                       OBJECT DB                                                  #
# ------------------------------------------------------------------------------------------------ #
class ObjectDB(Database):
    """Object Database"""

    def __init__(self, name: str, filepath: str) -> None:
        super().__init__()
        self._name = name
        self._filepath = filepath
        self._is_connected = False
        self._connection = None
        os.makedirs(os.path.dirname(self._filepath), exist_ok=True)

    @property
    def name(self) -> str:
        return self._name

    @property
    def filepath(self) -> str:
        return self._filepath

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._is_connected:
            self.close()
        if exc_type is not None:
            self._logger.error(f"\nExecution Type: {exc_type}")
            self._logger.error(f"\nExecution Value: {exc_value}")
            self._logger.error(f"\nTraceback: {traceback}")

    def connect(self) -> None:
        """Connects to the database."""
        self._connection = shelve.open(self._filepath)
        self._is_connected = True

    def close(self) -> None:
        """Closes the underlying database connection."""
        self._connection.close()
        self._is_connected = False

    def insert(self, key: str, value: Any) -> None:
        """Inserts a key/value pair into the database."""
        if self.exists(key):
            msg = f"Object with key {key} already exists in the database {self._name}."
            self._logger.error(msg)
            raise ObjectExistsError(msg)

        self._connection[key] = value

    def select(self, key: str) -> Any:
        """Retrieves data from the database"""
        try:
            return self._connection[key]
        except KeyError:
            msg = f"Object with key {key} not found in database {self._name}."
            self._logger.error(msg)
            raise ObjectNotFoundError(msg)
        except ValueError:
            msg = f"Database connection {self._name} is closed."
            self._logger.error(msg)
            raise ObjectDatabaseConnectionError(msg)
        except TypeError:
            msg = f"Database connection {self._name} is closed."
            self._logger.error(msg)
            raise ObjectDatabaseConnectionError(msg)

    def selectall(self) -> Any:
        """Retrieves all data from the database"""
        objects = {}
        try:
            keys = self._connection.keys()
            for key in keys:
                objects[key] = self._connection[key]
            return objects
        except ValueError:
            msg = f"Database connection {self._name} is closed."
            self._logger.error(msg)
            raise ObjectDatabaseConnectionError(msg)
        except AttributeError:
            msg = f"Database connection {self._name} is closed."
            self._logger.error(msg)
            raise ObjectDatabaseConnectionError(msg)

    def update(self, key: str, value: Any) -> None:
        """Updates an existing object in the database."""
        if self.exists(key):
            self._connection[key] = value

        else:
            msg = f"Object with key {key} not found in database {self._name}."
            self._logger.error(msg)
            raise ObjectNotFoundError(msg)

    def delete(self, key: str) -> None:
        """Deletes existing data."""
        try:
            del self._connection[key]
        except KeyError:
            msg = f"Object with key {key} doesn't exist in the database {self._name}."
            self._logger.error(msg)
            raise ObjectNotFoundError(msg)
        except ValueError:
            msg = f"Database connection {self._name} is closed."
            self._logger.error(msg)
            raise ObjectDatabaseConnectionError(msg)
        except TypeError:
            msg = f"Database connection {self._name} is closed."
            self._logger.error(msg)
            raise ObjectDatabaseConnectionError(msg)

    def exists(self, key: str) -> bool:
        """Checks existence of an item in the database."""
        try:
            return key in self._connection.keys()
        except ValueError:
            msg = f"Database connection {self._name} is closed."
            self._logger.error(msg)
            raise ObjectDatabaseConnectionError(msg)
        except AttributeError:
            msg = f"Database connection {self._name} is closed."
            self._logger.error(msg)
            raise ObjectDatabaseConnectionError(msg)

    def clear(self) -> None:
        """Clears cache of all objects."""
        try:
            self._connection.clear()
        except ValueError:
            msg = f"Database connection {self._name} is closed."
            self._logger.error(msg)
            raise ObjectDatabaseConnectionError(msg)
        except AttributeError:
            msg = f"Database connection {self._name} is closed."
            self._logger.error(msg)
            raise ObjectDatabaseConnectionError(msg)
