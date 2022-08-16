#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : \conftest.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday August 16th 2022 12:58:20 am                                                #
# Modified   : Tuesday August 16th 2022 12:58:30 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Includes fixtures, classes and functions supporting testing."""
import pytest
import pandas as pd

# ------------------------------------------------------------------------------------------------ #
TEST_DATAFRAME_FILEPATH = "tests/testdata/testfile.csv"
TEST_YAML_FILEPATH = "tests/testdata/testfile.yml"
# ------------------------------------------------------------------------------------------------ #
#                                        IGNORE                                                    #
# ------------------------------------------------------------------------------------------------ #
collect_ignore_glob = ["tests/old_tests/**/*.py"]
# ------------------------------------------------------------------------------------------------ #
#                                          SOURCE                                                  #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def dataframe():
    return pd.read_csv(TEST_DATAFRAME_FILEPATH)


@pytest.fixture(scope="module")
def dictionary():
    d = {"when": "now", "where": "here", "why": "why not", "how": "rtfm"}
    return d
