#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.3                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : \test_operators.py                                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday August 16th 2022 01:36:17 pm                                                #
# Modified   : Thursday September 8th 2022 12:40:55 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
import os
import pytest
import pandas as pd
from datetime import datetime
import logging
import logging.config
import shutil

# Enter imports for modules and classes being tested her
from atelier.workflow.operators import (
    KaggleDownloader,
    ExtractZip,
    LoadCSV,
    SaveCSV,
    LoadParquet,
    SaveParquet,
    DataFrameSample,
)

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.operators
class TestOperators:
    def test_setup(self):

        output_directory = "tests/testdata/test_operators/output"
        shutil.rmtree(output_directory, ignore_errors=True)

    def test_kaggle_downloader(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        name = inspect.stack()[0][3]
        params = {
            "competition": "feedback-prize-2021",
            "destination": "tests/testdata/test_operators/output/external",
            "filename": "feedback-prize-2021.zip",
            "force": False,
        }
        destination_filepath = os.path.join(params["destination"], params["filename"])
        operator = KaggleDownloader(name=name, params=params)
        assert isinstance(operator.__str__(), str)
        assert isinstance(operator.__repr__(), str)
        assert operator.name == inspect.stack()[0][3]
        assert isinstance(operator.params, dict)
        assert isinstance(operator.created, datetime)
        assert operator.started is None
        assert operator.stopped is None
        assert operator.duration is None

        operator.run()
        assert os.path.exists(destination_filepath)
        assert isinstance(operator.started, datetime)
        assert isinstance(operator.stopped, datetime)
        assert isinstance(operator.duration, float)

        operator.run()
        assert operator.skipped is True

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_extract_zip(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        name = inspect.stack()[0][3]
        params = {
            "source": "tests/testdata/test_operators/output/external/feedback-prize-2021.zip",
            "destination": "tests/testdata/test_operators/output/raw",
            "force": False,
        }

        operator = ExtractZip(name=name, params=params)
        assert isinstance(operator.__str__(), str)
        assert isinstance(operator.__repr__(), str)
        assert operator.name == inspect.stack()[0][3]
        assert isinstance(operator.params, dict)
        assert isinstance(operator.created, datetime)
        assert not os.path.exists(params["destination"])
        assert operator.started is None
        assert operator.stopped is None
        assert operator.duration is None

        operator.run()
        assert len(os.listdir(params["destination"])) > 0
        assert isinstance(operator.started, datetime)
        assert isinstance(operator.stopped, datetime)
        assert isinstance(operator.duration, float)

        operator.run()
        assert operator.skipped is True

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_load_csv(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        name = inspect.stack()[0][3]
        params = {
            "filepath": "tests/testdata/test_operators/output/raw/train.csv",
        }

        operator = LoadCSV(name=name, params=params)
        assert isinstance(operator.__str__(), str)
        assert isinstance(operator.__repr__(), str)
        assert operator.name == inspect.stack()[0][3]
        assert isinstance(operator.params, dict)
        assert isinstance(operator.created, datetime)
        assert operator.started is None
        assert operator.stopped is None
        assert operator.duration is None

        data = operator.run()
        data.head()
        assert data.shape[0] > 100
        assert data.shape[1] == 8
        assert isinstance(data, pd.DataFrame)
        assert isinstance(operator.started, datetime)
        assert isinstance(operator.stopped, datetime)
        assert isinstance(operator.duration, float)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_save_csv(self, caplog, dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        name = inspect.stack()[0][3]
        params = {
            "filepath": "tests/testdata/test_operators/output/staged/train.csv",
        }

        operator = SaveCSV(name=name, params=params)
        assert isinstance(operator.__str__(), str)
        assert isinstance(operator.__repr__(), str)
        assert operator.name == inspect.stack()[0][3]
        assert isinstance(operator.params, dict)
        assert isinstance(operator.created, datetime)
        assert operator.started is None
        assert operator.stopped is None
        assert operator.duration is None

        operator.run(data=dataframe)
        assert os.path.exists(params["filepath"])
        assert isinstance(operator.started, datetime)
        assert isinstance(operator.stopped, datetime)
        assert isinstance(operator.duration, float)

        operator.run(data=dataframe)
        assert operator.skipped is True

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_save_parquet(self, caplog, dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        name = inspect.stack()[0][3]
        params = {
            "filepath": "tests/testdata/test_operators/output/staged/train.parquet",
        }

        operator = SaveParquet(name=name, params=params)
        assert isinstance(operator.__str__(), str)
        assert isinstance(operator.__repr__(), str)
        assert operator.name == inspect.stack()[0][3]
        assert isinstance(operator.params, dict)
        assert isinstance(operator.created, datetime)
        assert operator.started is None
        assert operator.stopped is None
        assert operator.duration is None

        operator.run(data=dataframe)
        assert os.path.exists(params["filepath"])
        assert isinstance(operator.started, datetime)
        assert isinstance(operator.stopped, datetime)
        assert isinstance(operator.duration, float)

        operator.run(data=dataframe)
        assert operator.skipped is True

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_load_parquet(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        name = inspect.stack()[0][3]
        params = {
            "filepath": "tests/testdata/test_operators/output/staged/train.parquet",
        }

        operator = LoadParquet(name=name, params=params)
        assert isinstance(operator.__str__(), str)
        assert isinstance(operator.__repr__(), str)
        assert operator.name == inspect.stack()[0][3]
        assert isinstance(operator.params, dict)
        assert isinstance(operator.created, datetime)
        assert operator.started is None
        assert operator.stopped is None
        assert operator.duration is None

        data = operator.run()
        data.head()
        assert data.shape[0] > 100
        assert data.shape[1] == 5
        assert isinstance(data, pd.DataFrame)
        assert isinstance(operator.started, datetime)
        assert isinstance(operator.stopped, datetime)
        assert isinstance(operator.duration, float)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_sampler(self, caplog, dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        name = inspect.stack()[0][3]
        params = {
            "n": 100,
            "frac": None,
            "replace": False,
            "random_state": 202,
            "ignore_index": True,
        }

        operator = DataFrameSample(name=name, params=params)
        assert isinstance(operator.__str__(), str)
        assert isinstance(operator.__repr__(), str)
        assert operator.name == inspect.stack()[0][3]
        assert isinstance(operator.params, dict)
        assert isinstance(operator.created, datetime)
        assert operator.started is None
        assert operator.stopped is None
        assert operator.duration is None

        data = operator.run(data=dataframe)
        assert isinstance(data, pd.DataFrame)
        assert isinstance(operator.started, datetime)
        assert isinstance(operator.stopped, datetime)
        assert isinstance(operator.duration, float)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_sampler_n(self, caplog, dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        name = inspect.stack()[0][3]
        params = {
            "n": 100,
            "frac": None,
            "replace": False,
            "random_state": 202,
            "ignore_index": True,
        }

        operator = DataFrameSample(name=name, params=params)
        data = operator.run(data=dataframe)
        assert data.shape[0] == 100

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_sampler_frac(self, caplog, dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        name = inspect.stack()[0][3]
        params = {
            "n": None,
            "frac": 0.1,
            "replace": False,
            "random_state": 202,
            "ignore_index": True,
        }

        operator = DataFrameSample(name=name, params=params)
        data = operator.run(data=dataframe)
        assert data.shape[0] <= dataframe.shape[0] / 10

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_sampler_frac_gt_1(self, caplog, dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        name = inspect.stack()[0][3]
        params = {
            "n": None,
            "frac": 1.5,
            "replace": False,
            "random_state": 202,
            "ignore_index": True,
        }

        operator = DataFrameSample(name=name, params=params)
        data = operator.run(data=dataframe)
        assert data.shape[0] <= dataframe.shape[0] * 1.5

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
