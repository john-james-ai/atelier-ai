#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /atelier/persistence/base.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday March 2nd 2023 10:26:31 am                                                 #
# Modified   : Thursday March 2nd 2023 02:16:02 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Interface Definitions for the Persistence Package"""
from abc import ABC, abstractmethod
from datetime import datetime
import logging
from typing import Any


# ------------------------------------------------------------------------------------------------ #
class Asset(ABC):  # pragma: no cover
    def __init__(self) -> None:
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the name of the asset"""

    @property
    @abstractmethod
    def description(self) -> str:
        """Returns the description of the asset"""

    @property
    @abstractmethod
    def created(self) -> str:
        """Returns the datetime the asset was created."""

    @property
    @abstractmethod
    def added(self) -> str:
        """Returns the datetime the asset was added to the repository."""

    @added.setter
    @abstractmethod
    def added(self, added: datetime) -> None:
        """Updates the date added"""

    @property
    @abstractmethod
    def modified(self) -> str:
        """Returns the datetime the asset was modified."""

    @modified.setter
    @abstractmethod
    def modified(self, modified: datetime) -> None:
        """Updates the date modified."""

    @property
    @abstractmethod
    def memory(self) -> str:
        """Returns the amount of memory the object consumes in bytes."""

    # @memory.setter
    # @abstractmethod
    # def memory(self, memory: int) -> None:
    #     """Sets the size of memory the object consumes."""


# ------------------------------------------------------------------------------------------------ #
class RepoABC(ABC):  # pragma: no cover
    def __init__(self) -> None:
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the name of the repository."""

    @property
    @abstractmethod
    def location(self) -> str:
        """Returns the location (directory) in which the repository resides."""

    @property
    @abstractmethod
    def size(self) -> str:
        """Returns the size of the repository."""

    @abstractmethod
    def add(self, *args, **kwargs) -> Any:
        """Adds an asset to the repository and returns it."""

    @abstractmethod
    def get(self, *args, **kwargs) -> Any:
        """Obtains an item from the repository."""

    @abstractmethod
    def getall(self, *args, **kwargs) -> Any:
        """Obtains all items from the repository."""

    @abstractmethod
    def update(self, *args, **kwargs) -> Any:
        """Updates an existing item in the repository and returns it."""

    @abstractmethod
    def remove(self, *args, **kwargs) -> None:
        """Removes an existing item from the repository."""

    @abstractmethod
    def exists(self, *args, **kwargs) -> bool:
        """Returns True if hte named asset exists, False otherwise."""

    @abstractmethod
    def print(self) -> None:
        """Prints the inventory of items."""
