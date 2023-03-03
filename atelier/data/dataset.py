#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /atelier/data/dataset.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday March 2nd 2023 12:54:24 pm                                                 #
# Modified   : Thursday March 2nd 2023 01:48:37 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
from datetime import datetime
from typing import Any

from pympler.asizeof import asizeof

from atelier import Asset


# ------------------------------------------------------------------------------------------------ #
class Dataset(Asset):
    def __init__(self, name: str, description: str, data: Any) -> None:
        super().__init__()
        self._name = name
        self._description = description
        self._data = data
        self._created = datetime.now()
        self._added = None
        self._modified = None
        self._memory = None

    @property
    def name(self) -> str:
        """Returns the name of the asset"""
        return self._name

    @property
    def description(self) -> str:
        """Returns the description of the asset"""
        return self._description

    @property
    def created(self) -> str:
        """Returns the datetime the asset was created."""
        return self._created

    @property
    def added(self) -> str:
        """Returns the datetime the asset was added to the repository."""
        return self._added

    @added.setter
    def added(self, added: datetime) -> None:
        """Updates the date added"""
        self._added = added

    @property
    def modified(self) -> str:
        """Returns the datetime the asset was modified."""
        return self._modified

    @modified.setter
    def modified(self, modified: datetime) -> None:
        """Updates the date modified."""
        self._modified = modified

    @property
    def memory(self) -> int:
        """Returns the size of memory consumed by the object."""
        return asizeof(self)

    # @memory.setter
    # def memory(self, memory: int) -> None:
    #     """Sets the memory consumed by object."""
    #     self._memory = memory
