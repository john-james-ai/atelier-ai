#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /tests/test_operators/test_download.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 29th 2022 09:31:49 pm                                             #
# Modified   : Friday December 30th 2022 08:04:39 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
from datetime import datetime
import pytest
import logging

from atelier.operator.download import Downloader, DownloadExtractorZip, DownloadExtractorTarGZ

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
TEXT_FILE = "https://vision.cs.utexas.edu/projects/finegrained/utzap50k/readme.txt"
ZIP_FILE = "https://vision.cs.utexas.edu/projects/finegrained/utzap50k/ut-zap50k-data.zip"
GZ_FILE = "http://www.sbeams.org/sample_data/Microarray/External_test_data.tar.gz"

DESTINATION_1 = "tests/test_data/output/test_operators/test_download/"
DESTINATION_2 = "tests/test_data/output/test_operators/test_download/zipfile/"
DESTINATION_3 = "tests/test_data/output/test_operators/test_download/targz/"


@pytest.mark.download
class TestDownloader:  # pragma: no cover
    # ============================================================================================ #
    def test_downloader(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        os.makedirs(os.path.dirname(DESTINATION_1), exist_ok=True)

        d = Downloader(TEXT_FILE, DESTINATION_1)
        d.execute()
        assert len(os.listdir(DESTINATION_1)) > 0
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )


@pytest.mark.download_zip
class TestDownloadZip:  # pragma: no cover
    # ============================================================================================ #
    def test_download_zip(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        d = DownloadExtractorZip(url=ZIP_FILE, destination=DESTINATION_2)
        d.execute()
        assert len(os.listdir(DESTINATION_2)) > 0
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )


@pytest.mark.download_targz
class TestDownloadTarGZ:  # pragma: no cover
    # ============================================================================================ #
    def test_download_targz(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        d = DownloadExtractorTarGZ(url=GZ_FILE, destination=DESTINATION_3)
        d.execute()
        assert len(os.listdir(DESTINATION_3)) > 0
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
