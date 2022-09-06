#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.2                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : \test_pipeline.py                                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday August 16th 2022 04:24:03 am                                                #
# Modified   : Tuesday September 6th 2022 02:34:12 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from datetime import datetime
import inspect
import pytest
import logging
import logging.config

# Enter imports for modules and classes being tested here
from atelier.workflow.pipeline import DataPipeBuilder, DataPipe

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.pipe
class TestPipelineBuilder:
    def test_builder(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        config_filepath = "tests/testdata/config/pipe.yml"
        builder = DataPipeBuilder()
        builder.build(config_filepath=config_filepath)
        datapipe = builder.pipeline

        assert isinstance(builder.__str__(), str)
        assert isinstance(builder.__repr__(), str)
        assert isinstance(datapipe, DataPipe)

        builder.reset()
        assert builder.pipeline is None

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_pipeline(self, caplog, datapipe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        assert isinstance(datapipe.__str__(), str)
        assert isinstance(datapipe.__repr__(), str)
        assert isinstance(datapipe.name, str)
        assert isinstance(datapipe.created, datetime)
        assert len(datapipe.steps) == 4
        assert isinstance(datapipe.created, datetime)

        datapipe.run()

        datapipe.print_steps()

        assert isinstance(datapipe.started, datetime)
        assert isinstance(datapipe.stopped, datetime)
        assert isinstance(datapipe.duration, float)
        assert isinstance(datapipe.run_id, str)

        config_filepath = "tests/testdata/config/malpipe.yml"
        builder = DataPipeBuilder()
        with pytest.raises(KeyError):
            builder.build(config_filepath=config_filepath)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
