#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /test_datasource.py                                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday August 15th 2022 12:27:50 pm                                                 #
# Modified   : Thursday September 8th 2022 01:04:55 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
import pytest
import logging

# Enter imports for modules and classes being tested here
from atelier.data.datasource import DatasourceKaggle

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ================================================================================================ #
#                                    TEST DATASOURCE                                               #
# ================================================================================================ #


@pytest.mark.datasource
class TestDatasource:
    def test_datasource(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        name = "test_kaggle_datasource"
        competition = "feedback-prize-effectiveness"
        filename = "feedback-prize-effectiveness.zip"
        destination = "tests/test_data/output"
        filepath = os.path.join(destination, filename)

        ds = DatasourceKaggle(name=name, competition=competition, filename=filename)
        ds.extract(destination=destination)
        assert os.path.exists(filepath)
        assert ds.name == name
        assert ds.filename == filename
        assert ds.competition == competition

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_exceptions(self, caplog):

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        name = "test_kaggle_datasource"  # noqa F841
        competition = "feedback-prize-effectiveness"  # noqa F841
        filename = "feedback-prize-effectiveness.zip"  # noqa F841

        with pytest.raises(TypeError):
            ds = DatasourceKaggle(competition=competition, filename=filename)  # noqa F841

        with pytest.raises(TypeError):
            ds = DatasourceKaggle(name=name, filename=filename)  # noqa F841

        with pytest.raises(TypeError):
            ds = DatasourceKaggle(name=name, competition=competition)  # noqa F841

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
