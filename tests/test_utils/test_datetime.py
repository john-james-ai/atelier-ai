#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /test_datetime.py                                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 13th 2022 12:48:12 pm                                               #
# Modified   : Sunday November 13th 2022 02:56:56 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging
import math
import time

from atelier.utils.datetimes import Timer, Duration

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.duration
class TestDateTimes:
    def test_duration(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ------------------------------------------------------------------------------------------------ #
        secs = 925489.257
        d = Duration(secs)
        assert math.isclose(d.seconds, 49.257)
        assert d.minutes == 4
        assert d.hours == 17
        assert d.days == 10

        logger.info(d.as_string())

        # ------------------------------------------------------------------------------------------------ #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    def test_timer(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ------------------------------------------------------------------------------------------------ #
        t = Timer()
        t.start()
        time.sleep(3)
        t.stop()

        assert isinstance(t.started, datetime)
        assert isinstance(t.stopped, datetime)
        assert isinstance(t.duration, Duration)
        duration = t.duration
        assert duration.seconds < 4
        assert duration.seconds > 3
        assert duration.days == 0
        assert duration.hours == 0
        assert duration.minutes == 0

        logger.info(duration.as_string())

        # ------------------------------------------------------------------------------------------------ #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )
