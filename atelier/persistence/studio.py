#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /atelier/persistence/studio.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday March 2nd 2023 09:39:53 am                                                 #
# Modified   : Thursday March 2nd 2023 04:21:47 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import os
import shutil

import pandas as pd

from atelier.persistence.base import RepoABC
from atelier.persistence.odb import ObjectDB
from atelier.persistence.workspace import Workspace


# ------------------------------------------------------------------------------------------------ #
class Studio(RepoABC):
    """Studio object

    Args:
        name (str): The name of the studio.
        location (str): The directory containing the studio. This defaults to the root directory.
    """

    def __init__(self, name: str, location: str = "", safe_mode: bool = True) -> None:
        super().__init__()
        self._name = name
        self._location = location
        self._safe_mode = safe_mode
        self._studio_location = os.path.join(self._location, name + "/")
        self._db_filepath = os.path.join(self._studio_location, "workspaces.db")
        self._db = ObjectDB(name=name, filepath=self._db_filepath)

    @property
    def name(self) -> str:
        """Returns the name of the studio."""
        return self._name

    @property
    def location(self) -> str:
        """Returns the name of the studio."""
        return self._studio_location

    @property
    def size(self) -> int:
        return self._size()

    def create_workspace(self, name: str) -> Workspace:
        """Creates a Workspace object"""
        return Workspace(name=name, location=self._name)

    def add(self, workspace: Workspace) -> Workspace:
        """Adds a Workspace object to the studio."""
        with self._db as db:
            db.insert(key=workspace.name, value=workspace)
        return workspace

    def get(self, name: str) -> Workspace:
        """Obtains an Workspace object from the studio."""
        with self._db as db:
            return db.select(key=name)

    def getall(self) -> dict:
        """Obtains an workspace from the studio."""
        with self._db as db:
            return db.selectall()

    def update(self, workspace: Workspace) -> Workspace:
        """Updates a workspace object in the studio."""
        with self._db as db:
            db.update(key=workspace.name, value=workspace)
        return workspace

    def exists(self, name: str) -> bool:
        """Removes an existing item from the Workspace."""
        with self._db as db:
            return db.exists(key=name)

    def remove(self, name: str) -> None:
        """Removes an existing item from the Workspace."""
        with self._db as db:
            db.delete(key=name)

    def print(self) -> None:
        """Prints the inventory of items."""
        data = []
        with self._db as db:
            workspaces = db.selectall()
        for workspace in workspaces.values():
            d = {}
            d["name"] = workspace.name
            d["location"] = workspace.location
            d["size"] = workspace.size
            data.append(d)
        df = pd.DataFrame(data=data)
        print(df)

    def drop(self) -> None:
        """Deletes the workspace"""
        if self._safe_mode:
            x = input("This will permanently delete the workspace. Are you sure? [y/n] ")
            if "y" in x:
                shutil.rmtree(self._studio_location, ignore_errors=True)
        else:
            shutil.rmtree(self._studio_location, ignore_errors=True)

    def _size(self) -> int:
        """Returns the size of the Workspace."""
        size = 0
        workspaces = self.getall()
        for workspace in workspaces.values():
            size += workspace.size
        return size
