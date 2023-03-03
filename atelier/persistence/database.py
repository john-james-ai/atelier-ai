#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /atelier/persistence/database.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday March 2nd 2023 05:48:49 am                                                 #
# Modified   : Thursday March 2nd 2023 03:35:21 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from typing import Any
import logging


# ------------------------------------------------------------------------------------------------ #
#                                       DATABASE                                                  #
# ------------------------------------------------------------------------------------------------ #
class Database(ABC):  # pragma: no cover
    """Abstract Base Class for Database"""

    def __init__(self) -> None:
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @abstractmethod
    def connect(self) -> None:
        """Connects to the database."""

    @abstractmethod
    def close(self) -> None:
        """Closes the underlying database connection."""

    @abstractmethod
    def insert(self, key: str, value: Any) -> None:
        """Inserts a key/value pair into the database."""

    @abstractmethod
    def select(self, key: str) -> Any:
        """Retrieves data from the database"""

    @abstractmethod
    def update(self, key: str, value: Any) -> None:
        """Updates an existing object in the database."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Deletes existing data."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Checks existence of an item in the database."""
