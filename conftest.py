#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /conftest.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday August 16th 2022 12:58:20 am                                                #
# Modified   : Thursday March 2nd 2023 04:19:54 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Includes fixtures, classes and functions supporting testing."""
import sys
import pytest
import pandas as pd
import shutil

from atelier.workflow.pipeline import DataPipeBuilder  # noqa E402
from atelier.data.dataset import Dataset
from atelier.persistence.repo import Repo
from atelier.persistence.workspace import Workspace

# This is needed so Python can find test_tools on the path.
sys.path.append("../..")
from test_tools.fixtures.common import *  # noqa E402


# ------------------------------------------------------------------------------------------------ #
TEST_DATAFRAME_FILEPATH = "tests/testdata/testfile.csv"
TEST_YAML_FILEPATH = "tests/testdata/testfile.yml"
TEST_REPO_FILEPATH = "tests/testdata/persistence/repo/"
TEST_WORKSPACE_FILEPATH = "tests/testdata/persistence/workspace/"
TEST_STUDIO_FILEPATH = "tests/testdata/persistence/studio/"
# ------------------------------------------------------------------------------------------------ #
#                                        IGNORE                                                    #
# ------------------------------------------------------------------------------------------------ #
collect_ignore_glob = ["tests/old_tests/**/*.py"]
# ------------------------------------------------------------------------------------------------ #
#                                          SOURCE                                                  #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="session")
def dataframe():
    return pd.read_csv(TEST_DATAFRAME_FILEPATH)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def dictionary():
    d = {"when": "now", "where": "here", "why": "why not", "how": "rtfm"}
    return d


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def test_data_folder():
    return "tests/testdata"


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def dataset(dataframe):
    return Dataset(name="test_dataset", description="Dataset for Testing", data=dataframe)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def datasets(dataframe):
    datasets = []
    for i in range(1, 6):
        name = "test_dataset_" + str(i)
        desc = "Dataset for Testing #" + str(i)
        dataset = Dataset(name=name, description=desc, data=dataframe)
        datasets.append(dataset)
    return datasets


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def repo(datasets):
    shutil.rmtree(TEST_REPO_FILEPATH, ignore_errors=True)
    repo = Repo(name="test_repo", location=TEST_REPO_FILEPATH)
    for dataset in datasets:
        repo.add(dataset)
    return repo


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def repos(datasets):
    shutil.rmtree(TEST_REPO_FILEPATH, ignore_errors=True)
    repos = []
    for i in range(1, 6):
        name = "test_repo_" + str(i)
        repo = Repo(name=name, location=TEST_REPO_FILEPATH)
        for dataset in datasets:
            repo.add(dataset)
        repos.append(repo)
    return repos


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def workspace(repos):
    shutil.rmtree(TEST_WORKSPACE_FILEPATH, ignore_errors=True)
    ws = Workspace(name="test_workspace", location=TEST_WORKSPACE_FILEPATH)
    for repo in repos:
        ws.add(repo)
    return ws


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def workspaces(repos):
    workspaces = []
    for i in range(1, 6):
        name = "test_workspace_" + str(i)
        ws = Workspace(name=name, location=TEST_WORKSPACE_FILEPATH)
        for repo in repos:
            ws.add(repo)
        workspaces.append(ws)
    return workspaces


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def datapipe():
    config_filepath = "tests/testdata/config/pipe.yml"
    builder = DataPipeBuilder()
    builder.build(config_filepath=config_filepath)
    datapipe = builder.pipeline
    return datapipe


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def datapipe_builder():
    config_filepath = "tests/testdata/config/pipe.yml"
    builder = DataPipeBuilder()
    builder.build(config_filepath=config_filepath)
    return builder
