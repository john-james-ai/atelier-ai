#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /atelier/persistence/repo.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday March 2nd 2023 09:39:53 am                                                 #
# Modified   : Thursday March 2nd 2023 04:22:33 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations
import os
from datetime import datetime
import pandas as pd
import shutil

from atelier.persistence.base import RepoABC, Asset
from atelier.persistence.odb import ObjectDB


# ------------------------------------------------------------------------------------------------ #
class Repo(RepoABC):
    """Repository object

    Args:
        name (str): The name of the repository.
        location (str): The directory in which the repository will be created.
    """

    def __init__(self, name: str, location: str, safe_mode: bool = True) -> None:
        super().__init__()
        self._name = name
        self._location = location
        self._safe_mode = safe_mode
        self._db_filepath = os.path.join(self._location, self._name, "repository.db")
        self._repo_location = os.path.dirname(self._db_filepath)
        self._db = ObjectDB(name=name, filepath=self._db_filepath)

    @property
    def name(self) -> str:
        """Returns the name of the repository."""
        return self._name

    @property
    def location(self) -> str:
        """Returns the location (directory) in which the repository resides."""
        return self._repo_location

    @property
    def size(self) -> int:
        return self._size()

    def add(self, asset: Asset) -> Asset:
        """Adds an asset to the repository and returns it."""
        asset.added = datetime.now()
        with self._db as db:
            db.insert(key=asset.name, value=asset)
        return asset

    def get(self, name: str) -> Asset:
        """Obtains an item from the repository."""
        with self._db as db:
            return db.select(key=name)

    def getall(self) -> dict:
        """Obtains an item from the repository."""
        with self._db as db:
            return db.selectall()

    def update(self, asset: Asset) -> Asset:
        """Updates an existing item in the repository and returns it."""
        asset.modified = datetime.now()
        with self._db as db:
            db.update(key=asset.name, value=asset)
        return asset

    def exists(self, name: str) -> bool:
        """Returns a boolean indicating the existence of the named asset."""
        with self._db as db:
            return db.exists(key=name)

    def remove(self, name: str) -> None:
        """Removes an existing item from the repository."""
        with self._db as db:
            db.delete(key=name)

    def print(self) -> None:
        """Prints the inventory of items."""
        data = []
        with self._db as db:
            assets = db.selectall()
        for asset in assets.values():
            d = {}
            d["name"] = asset.name
            d["description"] = asset.description
            d["memory"] = asset.memory
            d["created"] = asset.created
            d["added"] = asset.added
            d["modified"] = asset.modified
            data.append(d)
        df = pd.DataFrame(data=data)
        print(df)

    def drop(self) -> None:
        """Deletes the workspace"""
        if self._safe_mode:
            x = input("This will permanently delete the workspace. Are you sure? [y/n] ")
            if "y" in x:
                shutil.rmtree(self._repo_location, ignore_errors=True)
        else:
            shutil.rmtree(self._repo_location, ignore_errors=True)

    def _size(self) -> int:
        """Returns the size of the repository."""
        size = 0
        objects = self.getall()
        for object in objects.values():
            size += object.memory
        return size
