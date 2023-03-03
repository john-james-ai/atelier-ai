#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /atelier/persistence/workspace.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday March 2nd 2023 09:39:53 am                                                 #
# Modified   : Thursday March 2nd 2023 04:26:07 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations
import os
import pandas as pd
import shutil

from atelier.persistence.base import RepoABC
from atelier.persistence.repo import Repo
from atelier.persistence.odb import ObjectDB


# ------------------------------------------------------------------------------------------------ #
class Workspace(RepoABC):
    """Workspace object

    Args:
        name (str): The name of the workspace.
        location (str): The directory in which the repository will be created.
    """

    def __init__(self, name: str, location: str, safe_mode: bool = True) -> None:
        super().__init__()
        self._name = name
        self._location = location
        self._safe_mode = safe_mode
        self._db_filepath = os.path.join(self._location, self._name, "repository.db")
        self._workspace_location = os.path.dirname(self._db_filepath)
        self._db = ObjectDB(name=name, filepath=self._db_filepath)

    @property
    def name(self) -> str:
        """Returns the name of the workspace."""
        return self._name

    @property
    def location(self) -> str:
        """Returns the location (directory) in which the workspace resides."""
        return self._workspace_location

    @property
    def size(self) -> int:
        return self._size()

    def create_repo(self, name: str) -> Repo:
        """Creates a repository object"""
        return Repo(name=name, location=self._workspace_location)

    def add(self, repo: Repo) -> Repo:
        """Adds a Repo object to the workspace."""
        with self._db as db:
            db.insert(key=repo.name, value=repo)
        return repo

    def get(self, name: str) -> Repo:
        """Obtains an Repo object from the workspace."""
        with self._db as db:
            return db.select(key=name)

    def getall(self) -> dict:
        """Obtains an repo from the workspace."""
        with self._db as db:
            return db.selectall()

    def update(self, repo: Repo) -> Repo:
        """Updates a repo object in the workspace."""
        with self._db as db:
            db.update(key=repo.name, value=repo)
        return repo

    def exists(self, name: str) -> bool:
        """Removes an existing item from the repository."""
        with self._db as db:
            return db.exists(key=name)

    def remove(self, name: str) -> None:
        """Removes an existing item from the repository."""
        with self._db as db:
            db.delete(key=name)

    def drop(self) -> None:
        """Deletes the workspace"""
        if self._safe_mode:
            x = input("This will permanently delete the workspace. Are you sure? [y/n] ")
            if "y" in x:
                shutil.rmtree(self._workspace_location, ignore_errors=True)
        else:
            shutil.rmtree(self._workspace_location, ignore_errors=True)

    def print(self) -> None:
        """Prints the inventory of items."""
        data = []
        with self._db as db:
            repos = db.selectall()
        for repo in repos.values():
            d = {}
            d["name"] = repo.name
            d["location"] = repo.location
            d["memory"] = repo.size
            data.append(d)
        df = pd.DataFrame(data=data)
        print(df)

    def _size(self) -> int:
        """Returns the size of the repository."""
        size = 0
        repos = self.getall()
        for repo in repos.values():
            size += repo.size
        return size
