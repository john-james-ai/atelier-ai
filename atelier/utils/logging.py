#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /atelier/utils/logging.py                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 13th 2022 12:05:36 pm                                               #
# Modified   : Monday January 30th 2023 06:04:38 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import functools
import logging

from datetimes import Timer

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


def log_start(module: str, classname: str, timer: Timer):

    date = timer.started.strftime("%m/%d/%Y")
    time = timer.stopped.strftime("%H:%M:%S")

    msg = "Started {} at {} on {}".format(classname, time, date)
    logger = logging.getLogger(module)

    logger.info(msg)


def log_end(module: str, classname: str, timer: Timer):

    date = timer.stopped.strftime("%m/%d/%Y")
    time = timer.stopped.strftime("%H:%M:%S")
    duration = timer.duration.as_string()

    msg = "Completed {} at {} on {}. Duration: {}.".format(classname, time, date, duration)
    logger = logging.getLogger(module)

    logger.info(msg)


def operator(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):

        module = func.__module__
        classname = func.__qualname__

        try:
            timer = Timer()
            timer.start()
            log_start(module, classname, timer)
            result = func(self, *args, **kwargs)
            timer.stop()
            log_end(module, classname, timer)
            return result

        except Exception as e:
            logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e

    return wrapper
