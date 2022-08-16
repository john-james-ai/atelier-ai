#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : \test_etl.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday August 16th 2022 04:24:03 am                                                #
# Modified   : Tuesday August 16th 2022 05:36:33 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
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


@pytest.mark.etl
class TestETL:
    def test_etl(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        config_filepath = "config/etl.yml"
        download_path = "dwh/fp2022/external/feedback-prize-effectiveness.zip"
        raw_path = "dwh/fp2022/raw/train.csv"
        staged_path = "dwh/fp2022/staged/train.parquet"

        builder = DataPipeBuilder()
        builder.build(config_filepath=config_filepath)
        datapipe = builder.pipeline
        datapipe.print_steps()
        assert isinstance(datapipe, DataPipe)

        datapipe.run()
        assert os.path.exists(download_path)
        assert os.path.exists(raw_path)
        assert os.path.exists(staged_path)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
